"""Verify real running crawls stop admitting captures after user Pause."""

import json
import time
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
from django.contrib.auth import get_user_model
from django.urls import reverse

from archivebox.core.models import Snapshot
from archivebox.crawls.models import Crawl
from archivebox.machine.models import Process
from archivebox.tests.conftest import (
    ADMIN_TEST_HOST,
    cleanup_process_group,
    cli_env,
    parse_jsonl_output,
    run_archivebox_cmd,
)
from archivebox.tests.test_orm_helpers import use_archivebox_db
from archivebox.workers.models import RETRY_AT_MAX

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.mark.timeout(180)
@pytest.mark.parametrize("pause_via", ["admin", "api"])
def test_live_pause_stops_new_captures_and_survives_runner_restart(initialized_archive, client, pause_via):
    site = initialized_archive / "pause-origin"
    site.mkdir()
    (site / "first.html").write_text("<html><title>Completed before pause</title></html>")
    (site / "second.html").write_text("<html><title>Running at pause</title>" + "x" * 32000 + "</html>")
    (site / "third.html").write_text("<html><title>Only after resume</title></html>")
    origin = ThreadingHTTPServer(("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(site)))
    origin_thread = Thread(target=origin.serve_forever, daemon=True)
    origin_thread.start()
    base_url = f"http://127.0.0.1:{origin.server_port}"
    urls = [base_url + f"/{name}.html" for name in ("first", "second", "third")]
    env = cli_env(
        wget=True,
        WGET_ARGS_EXTRA='["--limit-rate=2048"]',
        CRAWL_MAX_CONCURRENT_SNAPSHOTS="1",
        TIMEOUT="90",
    )
    runner = None
    hook_pid = None
    try:
        created = run_archivebox_cmd(["crawl", "create", *urls], cwd=initialized_archive, env=env)
        assert created.returncode == 0, created.stdout + created.stderr
        crawl_id = next(record["id"] for record in parse_jsonl_output(created.stdout) if record.get("type") == "Crawl")
        runner = run_archivebox_cmd(
            ["run", f"--crawl-id={crawl_id}"],
            cwd=initialized_archive,
            env=env,
            wait=False,
            start_new_session=True,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            assert runner.poll() is None, runner.communicate()
            with use_archivebox_db(initialized_archive):
                finished = Snapshot.objects.filter(crawl_id=crawl_id, url=urls[0], status=Snapshot.StatusChoices.SEALED).first()
                active = Snapshot.objects.filter(crawl_id=crawl_id, url=urls[1]).first()
                if finished and active:
                    running_hook = active.archiveresult_set.filter(plugin="wget", process__status=Process.StatusChoices.RUNNING).first()
                    if running_hook and running_hook.process.pid:
                        hook_pid = running_hook.process.pid
                        break
            time.sleep(0.1)
        assert hook_pid, "second real wget capture never started after first capture completed"
        with use_archivebox_db(initialized_archive):
            third = Snapshot.objects.get(crawl_id=crawl_id, url=urls[2])
            assert not third.archiveresult_set.exists()
            old_finished = Snapshot.objects.filter(pk=finished.pk).values().get()
            old_results = list(finished.archiveresult_set.order_by("pk").values())
            old_files = {path: path.read_bytes() for path in finished.output_dir.rglob("*") if path.is_file()}
            assert old_files
            user = get_user_model().objects.create_superuser(username="live_pause_admin", password="testpassword")
            client.force_login(user)
            if pause_via == "admin":
                response = client.post(
                    reverse("admin:crawls_crawl_changelist"),
                    {"action": "pause_selected_crawls", "_selected_action": crawl_id, "index": "0"},
                    HTTP_HOST=ADMIN_TEST_HOST,
                )
                assert response.status_code == 302
            else:
                response = client.patch(
                    f"/api/v1/crawls/crawl/{crawl_id}",
                    data={"action": "pause"},
                    content_type="application/json",
                    HTTP_HOST=ADMIN_TEST_HOST,
                )
                assert response.status_code == 200, response.content
            crawl = Crawl.objects.get(pk=crawl_id)
            assert crawl.status == Crawl.StatusChoices.PAUSED
            assert crawl.retry_at == RETRY_AT_MAX

        stdout, stderr = runner.communicate(timeout=60)
        assert runner.returncode == 0, stdout + stderr
        # Two fresh runner processes must observe persisted Pause rather than
        # accidentally restarting the remaining work during recovery.
        for _ in range(2):
            paused = run_archivebox_cmd(["run", f"--crawl-id={crawl_id}"], cwd=initialized_archive, env=env)
            assert paused.returncode == 0, paused.stdout + paused.stderr
            with use_archivebox_db(initialized_archive):
                crawl.refresh_from_db()
                assert crawl.status == Crawl.StatusChoices.PAUSED
                assert crawl.retry_at == RETRY_AT_MAX
                assert not third.archiveresult_set.exists()
                assert Snapshot.objects.filter(pk=finished.pk).values().get() == old_finished
                assert list(finished.archiveresult_set.order_by("pk").values()) == old_results
                assert all(path.read_bytes() == content for path, content in old_files.items())

        with use_archivebox_db(initialized_archive):
            if pause_via == "admin":
                response = client.post(
                    reverse("admin:crawls_crawl_changelist"),
                    {"action": "resume_selected_crawls", "_selected_action": crawl_id, "index": "0"},
                    HTTP_HOST=ADMIN_TEST_HOST,
                )
                assert response.status_code == 302
            else:
                response = client.patch(
                    f"/api/v1/crawls/crawl/{crawl_id}",
                    data={"action": "resume"},
                    content_type="application/json",
                    HTTP_HOST=ADMIN_TEST_HOST,
                )
                assert response.status_code == 200, response.content
        resumed = run_archivebox_cmd(["run", f"--crawl-id={crawl_id}"], cwd=initialized_archive, env=env, timeout=90)
        assert resumed.returncode == 0, resumed.stdout + resumed.stderr
        with use_archivebox_db(initialized_archive):
            crawl.refresh_from_db()
            assert crawl.status == Crawl.StatusChoices.SEALED
            assert crawl.snapshot_set.count() == 3
            assert crawl.snapshot_set.filter(status=Snapshot.StatusChoices.SEALED).count() == 3
            capture = third.archiveresult_set.get(plugin="wget", status="succeeded")
            assert capture.output_files
            assert any(b"Only after resume" in path.read_bytes() for path in (third.output_dir / "wget").rglob("*.html"))
            assert Snapshot.objects.filter(pk=finished.pk).values().get() == old_finished
            assert list(finished.archiveresult_set.order_by("pk").values()) == old_results
            assert all(path.read_bytes() == content for path, content in old_files.items())
            print(json.dumps({"pause_via": pause_via, "crawl": crawl_id, "paused_runner_restarts": 2, "completed_captures": 3}))
    finally:
        cleanup_process_group(runner.pid if runner else None, hook_pid)
        origin.shutdown()
        origin.server_close()
        origin_thread.join(timeout=5)
