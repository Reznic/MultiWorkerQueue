from queue import Queue, Full
from threading import Thread, Lock


class RequestsQueue(object):
    def __init__(self, logger, max_threads, max_q_size, request_handler):
        self.logger = logger
        self.max_threads = max_threads
        self.max_q_size = max_q_size
        self.request_handler = request_handler
        self.raised_exception = None

        self._client_queues = {}
        self._dict_lock = Lock()
        self._schedule_queue = Queue()
        self._finish_handlers = {}
        self._alive = True
        self._threads = []
        for _ in range(max_threads):
            thread = Thread(target=self._worker_thread_loop)
            self._threads.append(thread)
            thread.start()

    def push(self, request, client_id):
        if request is None or client_id is None:
            raise ValueError()

        with self._dict_lock:
            client_queue = self._client_queues[client_id]

        try:
            client_queue.put(request)
        except Full:
            self.logger.error(f"{len(client_queue.queue)} pending video "
                              f"encoding requests for session {client_id}. "
                              f"Ignoring request!")
            return

        with client_queue.mutex:
            if not client_queue.scheduled and len(client_queue.queue) > 0:
                client_queue.scheduled = True
                self._schedule_queue.put(client_queue)

    def add_client(self, client_id):
        with self._dict_lock:
            self._client_queues[client_id] = Queue(maxsize=self.max_q_size)
            self._client_queues[client_id].scheduled = False

    def remove_client(self, client_id, handle_finish, wait_to_pending_requests):
        with self._dict_lock:
            client_queue = self._client_queues.pop(client_id)

        if not client_queue.scheduled:
            client_queue.join()
            handle_finish()
            return

        if not wait_to_pending_requests:
            with client_queue.mutex:
                client_queue.queue.clear()

        self._finish_handlers[client_queue] = handle_finish
        client_queue.put(None)  # Poison pill
        client_queue.join()

    def join(self):
        self._schedule_queue.join()
        self.kill()
        for thread in self._threads:
            thread.join(timeout=3)

    def kill(self):
        self._alive = False
        self._schedule_queue.put(None)  # Poison pill

    def size(self):
        return None

    def _worker_thread_loop(self):
        while self._alive:
            client_queue = self._schedule_queue.get()
            if client_queue is None:
                self._schedule_queue.task_done()
                return

            request = client_queue.get()

            if request is None:
                # Queue finished
                handle_finish = self._finish_handlers.pop(client_queue)
                client_queue.task_done()
                client_queue.join()
                handle_finish()

            else:
                # Push client queue to the back (round robin scheduling) if not empty
                with client_queue.mutex:
                    if len(client_queue.queue) > 0:
                        client_queue.scheduled = True
                        self._schedule_queue.put(client_queue)
                    else:
                        client_queue.scheduled = False

                try:
                    self.request_handler(request)
                except Exception as e:
                    self.logger.error("Requests executor failed to handle request")
                    self.raised_exception = e

                client_queue.task_done()

            self._schedule_queue.task_done()
