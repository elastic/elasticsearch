/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.HttpEntity;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.reindex.remote.RemoteReindexingUtils.wrapExceptionToPreserveStatus;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteReindexingUtilsTests extends ESTestCase {
    private ThreadPool threadPool;
    private RestClient client;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ExecutorService executor(String name) {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }
        };
        client = mock(RestClient.class);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    /**
     * Verifies that lookupRemoteVersion correctly parses historical and
     * forward-compatible main action responses.
     */
    public void testLookupRemoteVersion() throws Exception {
        assertLookupRemoteVersion(Version.fromString("0.20.5"), "main/0_20_5.json");
        assertLookupRemoteVersion(Version.fromString("0.90.13"), "main/0_90_13.json");
        assertLookupRemoteVersion(Version.fromString("1.7.5"), "main/1_7_5.json");
        assertLookupRemoteVersion(Version.fromId(2030399), "main/2_3_3.json");
        assertLookupRemoteVersion(Version.fromId(5000099), "main/5_0_0_alpha_3.json");
        assertLookupRemoteVersion(Version.fromId(5000099), "main/with_unknown_fields.json");
    }

    private void assertLookupRemoteVersion(Version expected, String resource) throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        URL url = Thread.currentThread().getContextClassLoader().getResource("responses/" + resource);
        assertNotNull("missing test resource [" + resource + "]", url);

        HttpEntity entity = new InputStreamEntity(FileSystemUtils.openFileURLStream(url), ContentType.APPLICATION_JSON);
        org.elasticsearch.client.Response response = mock(org.elasticsearch.client.Response.class);
        when(response.getEntity()).thenReturn(entity);

        mockSuccess(response);
        RemoteReindexingUtils.lookupRemoteVersion(RejectAwareActionListener.wrap(v -> {
            assertEquals(expected, v);
            called.set(true);
        }, e -> fail(), e -> fail()), threadPool, client);
        assertTrue("listener was not called", called.get());
    }

    /**
     * Verifies that lookupRemoteVersion fails when the response does not include
     * a Content-Type header, and that the error message includes the response body.
     */
    public void testLookupRemoteVersionFailsWithoutContentType() throws Exception {
        URL url = Thread.currentThread().getContextClassLoader().getResource("responses/main/0_20_5.json");
        assertNotNull(url);

        HttpEntity entity = new InputStreamEntity(
            FileSystemUtils.openFileURLStream(url),
            // intentionally no Content-Type
            null
        );

        org.elasticsearch.client.Response response = mock(org.elasticsearch.client.Response.class);
        when(response.getEntity()).thenReturn(entity);
        mockSuccess(response);

        try {
            RemoteReindexingUtils.lookupRemoteVersion(
                RejectAwareActionListener.wrap(
                    v -> fail("Expected an exception yet one was not thrown"),
                    // We're expecting an exception, so no need to fail
                    e -> {},
                    e -> {}
                ),
                threadPool,
                client
            );
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString("Response didn't include Content-Type: body={"));
        } catch (Exception e) {
            fail("Expected RuntimeException");
        }
    }

    /**
     * Verifies that HTTP 429 responses are routed to onRejection rather than onFailure.
     */
    public void testLookupRemoteVersionTooManyRequestsTriggersRejection() throws Exception {
        AtomicBoolean rejected = new AtomicBoolean();
        Response response = mock(Response.class);
        when(response.getEntity()).thenReturn(null);

        org.apache.http.StatusLine statusLine = mock(org.apache.http.StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(RestStatus.TOO_MANY_REQUESTS.getStatus());
        when(response.getStatusLine()).thenReturn(statusLine);

        // Mocks used in the ResponseException constructor
        RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn("mock");
        when(response.getRequestLine()).thenReturn(requestLine);
        mockFailure(new org.elasticsearch.client.ResponseException(response));

        RemoteReindexingUtils.lookupRemoteVersion(
            RejectAwareActionListener.wrap(v -> fail("unexpected success"), e -> fail("unexpected failure"), e -> rejected.set(true)),
            threadPool,
            client
        );
        assertTrue("onRejection was not called", rejected.get());
    }

    /**
     * Verifies that non-429 HTTP errors are routed to onFailure.
     */
    public void testLookupRemoteVersionHttpErrorTriggersFailure() throws Exception {
        org.apache.http.StatusLine statusLine = mock(org.apache.http.StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(RestStatus.BAD_REQUEST.getStatus());
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(response.getEntity()).thenReturn(new StringEntity("bad request", ContentType.TEXT_PLAIN));

        // Mocks used in the ResponseException constructor
        RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn("mock");
        when(response.getRequestLine()).thenReturn(requestLine);
        mockFailure(new org.elasticsearch.client.ResponseException(response));

        RemoteReindexingUtils.lookupRemoteVersion(RejectAwareActionListener.wrap(v -> fail(), ex -> {
            assertTrue(ex instanceof ElasticsearchException);
            assertEquals(RestStatus.BAD_REQUEST, ((ElasticsearchStatusException) ex).status());
        }, ex -> fail()), threadPool, client);
    }

    /**
     * Verifies that ContentTooLongException is translated into a user-facing IllegalArgumentException.
     */
    public void testContentTooLongExceptionIsWrapped() {
        mockFailure(new org.apache.http.ContentTooLongException("too large"));

        RemoteReindexingUtils.lookupRemoteVersion(RejectAwareActionListener.wrap(v -> fail(), ex -> {
            assertTrue(ex instanceof IllegalArgumentException);
            assertThat(ex.getMessage(), containsString("Remote responded with a chunk that was too large"));
        }, ex -> fail()), threadPool, client);
    }

    public void testInvalidJsonThrowsElasticsearchException() {
        HttpEntity entity = new StringEntity("this is not json", ContentType.APPLICATION_JSON);
        Response response = mock(Response.class);
        when(response.getEntity()).thenReturn(entity);
        mockSuccess(response);

        RemoteReindexingUtils.lookupRemoteVersion(RejectAwareActionListener.wrap(v -> fail(), ex -> {
            assertTrue(ex instanceof ElasticsearchException);
            assertThat(ex.getMessage(), containsString("remote is likely not an Elasticsearch instance"));
        }, ex -> fail()), threadPool, client);
    }

    /**
     * Verifies that IOExceptions during response deserialization are surfaced correctly.
     */
    public void testIOExceptionDuringDeserialization() throws Exception {
        HttpEntity entity = mock(HttpEntity.class);
        when(entity.getContent()).thenThrow(new IOException("boom"));
        Response response = mock(Response.class);
        when(response.getEntity()).thenReturn(entity);
        mockSuccess(response);

        RemoteReindexingUtils.lookupRemoteVersion(RejectAwareActionListener.wrap(v -> fail(), ex -> {
            assertTrue(ex instanceof ElasticsearchException);
            assertThat(ex.getMessage(), containsString("Error deserializing response"));
        }, ex -> fail()), threadPool, client);
    }

    public void testWrapExceptionToPreserveStatus() throws IOException {
        Exception cause = new Exception();

        // Successfully get the status without a body
        RestStatus status = randomFrom(RestStatus.values());
        ElasticsearchStatusException wrapped = wrapExceptionToPreserveStatus(status.getStatus(), null, cause);
        assertEquals(status, wrapped.status());
        assertEquals(cause, wrapped.getCause());
        assertEquals("No error body.", wrapped.getMessage());

        // Successfully get the status without a body
        HttpEntity okEntity = new StringEntity("test body", ContentType.TEXT_PLAIN);
        wrapped = wrapExceptionToPreserveStatus(status.getStatus(), okEntity, cause);
        assertEquals(status, wrapped.status());
        assertEquals(cause, wrapped.getCause());
        assertEquals("body=test body", wrapped.getMessage());

        // Successfully get the status with a broken body
        IOException badEntityException = new IOException();
        HttpEntity badEntity = mock(HttpEntity.class);
        when(badEntity.getContent()).thenThrow(badEntityException);
        wrapped = wrapExceptionToPreserveStatus(status.getStatus(), badEntity, cause);
        assertEquals(status, wrapped.status());
        assertEquals(cause, wrapped.getCause());
        assertEquals("Failed to extract body.", wrapped.getMessage());
        assertEquals(badEntityException, wrapped.getSuppressed()[0]);

        // Fail to get the status without a body
        int notAnHttpStatus = -1;
        assertNull(RestStatus.fromCode(notAnHttpStatus));
        wrapped = wrapExceptionToPreserveStatus(notAnHttpStatus, null, cause);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, wrapped.status());
        assertEquals(cause, wrapped.getCause());
        assertEquals("Couldn't extract status [" + notAnHttpStatus + "]. No error body.", wrapped.getMessage());

        // Fail to get the status without a body
        wrapped = wrapExceptionToPreserveStatus(notAnHttpStatus, okEntity, cause);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, wrapped.status());
        assertEquals(cause, wrapped.getCause());
        assertEquals("Couldn't extract status [" + notAnHttpStatus + "]. body=test body", wrapped.getMessage());

        // Fail to get the status with a broken body
        wrapped = wrapExceptionToPreserveStatus(notAnHttpStatus, badEntity, cause);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, wrapped.status());
        assertEquals(cause, wrapped.getCause());
        assertEquals("Couldn't extract status [" + notAnHttpStatus + "]. Failed to extract body.", wrapped.getMessage());
        assertEquals(badEntityException, wrapped.getSuppressed()[0]);
    }

    public void testBodyMessageWithNullEntity() throws Exception {
        String message = RemoteReindexingUtils.bodyMessage(null);
        assertEquals("No error body.", message);
    }

    public void testBodyMessageWithReadableEntity() throws Exception {
        String testBody = randomAlphanumericOfLength(10);
        HttpEntity entity = new StringEntity(testBody, ContentType.TEXT_PLAIN);

        String message = RemoteReindexingUtils.bodyMessage(entity);

        assertEquals("body=" + testBody, message);
    }

    public void testBodyMessageWithIOException() throws Exception {
        IOException expected = new IOException("Exception");

        HttpEntity entity = mock(HttpEntity.class);
        when(entity.getContent()).thenThrow(expected);

        IOException actual = expectThrows(IOException.class, () -> RemoteReindexingUtils.bodyMessage(entity));

        assertSame(expected, actual);
    }

    private void mockSuccess(Response response) {
        doAnswer(inv -> {
            ((org.elasticsearch.client.ResponseListener) inv.getArgument(1)).onSuccess(response);
            return null;
        }).when(client).performRequestAsync(any(), any());
    }

    private void mockFailure(Exception e) {
        doAnswer(inv -> {
            ((org.elasticsearch.client.ResponseListener) inv.getArgument(1)).onFailure(e);
            return null;
        }).when(client).performRequestAsync(any(), any());
    }
}
