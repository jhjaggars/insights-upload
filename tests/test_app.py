import asyncio
import io
import json
import os

import pytest
import requests
from botocore.exceptions import ClientError
from tornado.httpclient import AsyncHTTPClient, HTTPClientError
from tornado.testing import AsyncHTTPTestCase, gen_test
from unittest import TestCase
from re import search


import app
from tests.fixtures.fake_mq import FakeMQ
from tests.fixtures import StopLoopException
from utils.storage import s3 as s3_storage
from mock import patch

client = AsyncHTTPClient()
with open('VERSION', 'rb') as f:
    VERSION = f.read()


class TestContentRegex(TestCase):
    """
    Test the content MIME type regex described in IPP 1.
    """

    def test_valid_mime_type(self):
        """
        A valid MIME type is correctly recognized.
        """
        mime_types = [
            'application/vnd.redhat.insights.advisor+zip',
            'application/vnd.redhat.insights.compliance+tgz',
            'application/vnd.redhat.my-app.service1+zip',
            'application/vnd.redhat.s0m3-s3rv1c3.s0m3-4pp+tgz'
        ]
        for mime_type in mime_types:
            with self.subTest(mime_type=mime_type):
                self.assertIsNotNone(search(app.content_regex, mime_type))

    def test_invalid_mime_type(self):
        """
        An invalid MIME type is correctly recognized.
        """
        mime_types = [
            'application/vnd.redhat.insights.advisor+tbz2',
            'application/vnd.redhat.compliance+tgz',
            'application/vnd.redhat.my_app.service+zip',
            'text/vnd.redhat.insights.advisor+tgz',
            'application/bbq.redhat.insights.advisor+tgz',
            'application/vnd.ibm.insights.advisor+tgz'
        ]
        for mime_type in mime_types:
            with self.subTest(mime_type=mime_type):
                self.assertIsNone(search(app.content_regex, mime_type))


class TestUploadHandler(AsyncHTTPTestCase):

    @staticmethod
    def prepare_request_context(file_size=100, file_name=None, mime_type='application/vnd.redhat.advisor.payload+tgz',
                                file_field_name='upload'):

        # Build HTTP Request so that Tornado can recognize and use the payload test
        request = requests.Request(
            url="http://localhost:8888/r/insights/platform/upload/api/v1/upload", data={},
            files={file_field_name: (file_name, io.BytesIO(os.urandom(file_size)), mime_type)} if file_name else None
        )
        request.headers["x-rh-insights-request-id"] = "test"

        return request.prepare()

    def get_app(self):
        return app.app

    @gen_test
    def test_root_get(self):
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload'), method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'boop')
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload'), method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, HEAD, OPTIONS')

    @gen_test
    def test_upload_get(self):
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload/api/v1/upload'), method='GET')
        self.assertEqual(response.body, b"Accepted Content-Types: gzipped tarfile, zip file")

    @gen_test
    def test_upload_allowed_methods(self):
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload/api/v1/upload'), method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, POST, HEAD, OPTIONS')

    @gen_test
    def test_upload_post(self):
        request_context = self.prepare_request_context(100, 'payload.tar.gz')
        response = yield self.http_client.fetch(
            self.get_url('/r/insights/platform/upload/api/v1/upload'),
            method='POST',
            body=request_context.body,
            headers=request_context.headers
        )

        self.assertEqual(response.code, 202)

    @gen_test
    def test_version(self):
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload/api/v1/version'), method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'{"version": "%s"}' % VERSION)

    @gen_test
    def test_upload_post_file_too_large(self):
        request_context = self.prepare_request_context(app.MAX_LENGTH + 1, 'payload.tar.gz')

        with self.assertRaises(HTTPClientError) as response:
            yield self.http_client.fetch(
                self.get_url('/r/insights/platform/upload/api/v1/upload'),
                method='POST',
                body=request_context.body,
                headers=request_context.headers
            )

        self.assertEqual(response.exception.code, 413)
        self.assertEqual(
            'Payload too large: {content_length}. Should not exceed {max_length} bytes'.format(
                content_length=str(request_context.headers.get('Content-Length')),
                max_length=str(app.MAX_LENGTH)
            ), response.exception.message
        )

    @gen_test
    def test_upload_post_file_wrong_mime_type(self):
        request_context = self.prepare_request_context(100, 'payload.tar.gz', mime_type='application/json')

        with self.assertRaises(HTTPClientError) as response:
            yield self.http_client.fetch(
                self.get_url('/r/insights/platform/upload/api/v1/upload'),
                method='POST',
                body=request_context.body,
                headers=request_context.headers
            )

        self.assertEqual(response.exception.code, 415)
        self.assertEqual(response.exception.message, 'Unsupported Media Type')

    @gen_test
    def test_upload_post_no_file(self):
        request_context = self.prepare_request_context(file_name='payload.tar.gz', file_field_name='not_upload')

        with self.assertRaises(HTTPClientError) as response:
            yield self.http_client.fetch(
                self.get_url('/r/insights/platform/upload/api/v1/upload'),
                method='POST',
                body=request_context.body,
                headers=request_context.headers
            )

        self.assertEqual(response.exception.code, 415)
        self.assertEqual(response.exception.message, 'Upload field not found')


class TestProducerAndConsumer:

    @staticmethod
    def _create_message_s3(_file, _stage_message, avoid_produce_queue=False, validation='success',
                           topic='platform.upload.validation'):
        return _stage_message(_file, topic, avoid_produce_queue, validation)

    @asyncio.coroutine
    async def coroutine_test(self, method, exc_message='Stopping the iteration'):
        with pytest.raises(StopLoopException, message=exc_message) as e:
            await method()

        assert str(e.value) == exc_message

    def test_producer_with_s3_bucket(self, local_file, s3_mocked, broker_stage_messages, event_loop):
        total_messages = 4
        [self._create_message_s3(local_file, broker_stage_messages) for _ in range(total_messages)]

        with FakeMQ() as mq:
            producer = app.MQClient(mq, "producer").run(app.make_preprocessor())
            assert app.mqp.produce_calls_count == 0
            assert len(app.produce_queue) == total_messages

            event_loop.run_until_complete(self.coroutine_test(producer))

            assert app.mqp.produce_calls_count == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqp.disconnect_in_operation_called is False
            assert app.mqp.trying_to_connect_failures_calls == 0

    def test_consumer_with_s3_bucket(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        produced_messages = []
        with FakeMQ() as mq:
            consumer = app.MQClient(mq, "consumer").run(app.handle_validation)

            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic
                )
                app.mqc.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            for m in produced_messages:
                assert s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert app.mqc.produce_calls_count == total_messages
            assert app.mqc.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqc.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(consumer))

            for m in produced_messages:
                with pytest.raises(ClientError) as e:
                    s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])
                assert str(e.value) == 'An error occurred (404) when calling the HeadObject operation: Not Found'

                assert s3_storage.ls(s3_storage.PERM, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert app.mqc.consume_calls_count > 0
            assert app.mqc.consume_return_messages_count == 1

            assert app.mqc.count_topic_messages(topic) == 0
            assert app.mqc.disconnect_in_operation_called is False
            assert app.mqc.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 4

    def test_consumer_with_validation_failure(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        s3_storage.s3.create_bucket(Bucket=s3_storage.REJECT)
        produced_messages = []

        with FakeMQ() as mq:
            consumer = app.MQClient(mq, "consumer").run(app.handle_validation)
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic, validation='failure'
                )
                app.mqc.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            assert app.mqc.produce_calls_count == total_messages
            assert app.mqc.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqc.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(consumer))

            for m in produced_messages:
                with pytest.raises(ClientError) as e:
                    s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])
                assert str(e.value) == 'An error occurred (404) when calling the HeadObject operation: Not Found'

                assert s3_storage.ls(s3_storage.REJECT, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert app.mqc.consume_calls_count > 0
            assert app.mqc.consume_return_messages_count == 1

            assert app.mqc.count_topic_messages(topic) == 0
            assert app.mqc.disconnect_in_operation_called is False
            assert app.mqc.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 0

    def test_consumer_with_validation_unknown(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        s3_storage.s3.create_bucket(Bucket=s3_storage.REJECT)
        produced_messages = []

        with FakeMQ() as mq:
            consumer = app.MQClient(mq, "consumer").run(app.handle_validation)
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic, validation='unknown'
                )
                app.mqc.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            assert app.mqc.produce_calls_count == total_messages
            assert app.mqc.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqc.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(consumer))

            assert app.mqc.consume_calls_count > 0
            assert app.mqc.consume_return_messages_count == 1

            assert app.mqc.count_topic_messages(topic) == 0
            assert app.mqc.disconnect_in_operation_called is False
            assert app.mqc.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 0

    @patch("app.RETRY_INTERVAL", 0.01)
    def test_producer_with_connection_issues(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        [self._create_message_s3(local_file, broker_stage_messages) for _ in range(total_messages)]

        with FakeMQ(connection_failing_attempt_countdown=1, disconnect_in_operation=2) as mq:
            producer = app.MQClient(mq, "producer").run(app.make_preprocessor())
            assert app.mqp.produce_calls_count == 0
            assert len(app.produce_queue) == total_messages

            event_loop.run_until_complete(self.coroutine_test(producer))

            assert app.mqp.produce_calls_count == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqp.disconnect_in_operation_called is True
            assert app.mqp.trying_to_connect_failures_calls == 1

    @patch("app.RETRY_INTERVAL", 0.01)
    def test_consumer_with_connection_issues(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'

        with FakeMQ(connection_failing_attempt_countdown=1, disconnect_in_operation=2) as mq:
            consumer = app.MQClient(mq, "consumer").run(app.handle_validation)
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic
                )
                app.mqc.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)

            assert app.mqc.produce_calls_count == total_messages
            assert app.mqc.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqc.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(consumer))

            assert app.mqc.consume_calls_count > 0
            assert app.mqc.consume_return_messages_count == 1

            assert app.mqc.count_topic_messages(topic) == 0
            assert app.mqc.disconnect_in_operation_called is True
            assert app.mqc.trying_to_connect_failures_calls == 1
            assert len(app.produce_queue) == 4
