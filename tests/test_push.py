import pytest
from unittest.mock import MagicMock, call
import logging

from multiworkerqueue import MultiWorkerQueue, ClientNotFoundError


@pytest.fixture
def logger():
    return logging.getLogger("test")


task_handler = MagicMock()

@pytest.fixture
def queue(logger):
    queue = MultiWorkerQueue(logger, max_threads=1, max_q_size=10, task_handler=task_handler)
    yield queue
    queue.kill()


def test_push_to_q(queue):
    task_handler.reset_mock()
    queue.add_client(client_id="client1")
    queue.push("task1", "client1")
    queue.push("task2", "client1")
    queue.push("task3", "client1")
    queue.push("task4", "client1")
    queue.remove_client(client_id="client1", wait_to_pending_tasks=True, handle_finish=None)
    task_handler.assert_has_calls([call("task1"), call("task2"), call("task3"), call("task4")])


def test_invalid_client(queue):
    with pytest.raises(ClientNotFoundError):
        queue.push("task", "client1")


def test_finish_handler(queue):
    task_handler.reset_mock()
    queue.add_client(client_id="client1")
    queue.push("task1", "client1")
    queue.push("task2", "client1")
    queue.push("task3", "client1")
    queue.remove_client("client1", True, task_handler, "finish_handler_args")
    task_handler.assert_has_calls([call("task1"), call("task2"), call("task3"), call.__bool__(), call("finish_handler_args")])
