/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import javax.net.ssl.SSLHandshakeException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class SyncResponseListenerTests extends RestClientTestCase {
    /**
     * Asserts that the provided {@linkplain Exception} contains the method
     * that called this <strong>somewhere</strong> on its stack. This is
     * normally the case for synchronous calls but {@link RestClient} performs
     * synchronous calls by performing asynchronous calls and blocking the
     * current thread until the call returns so it has to take special care
     * to make sure that the caller shows up in the exception. We use this
     * assertion to make sure that we don't break that "special care".
     */
    static void assertExceptionStackContainsCallingMethod(Exception e) {
        // 0 is getStackTrace
        // 1 is this method
        // 2 is the caller, what we want
        StackTraceElement myMethod = Thread.currentThread().getStackTrace()[2];
        for (StackTraceElement se : e.getStackTrace()) {
            if (se.getClassName().equals(myMethod.getClassName())
                    && se.getMethodName().equals(myMethod.getMethodName())) {
                return;
            }
        }
        StringWriter stack = new StringWriter();
        e.printStackTrace(new PrintWriter(stack));
        fail("didn't find the calling method (looks like " + myMethod + ") in:\n" + stack);
    }

    public void testOnSuccessNullResponse() {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        try {
            syncResponseListener.onSuccess(null);
            fail("onSuccess should have failed");
        } catch (NullPointerException e) {
            assertEquals("response must not be null", e.getMessage());
        }
    }

    public void testOnFailureNullException() {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        try {
            syncResponseListener.onFailure(null);
            fail("onFailure should have failed");
        } catch (NullPointerException e) {
            assertEquals("exception must not be null", e.getMessage());
        }
    }

    public void testOnSuccess() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        Response mockResponse = mockResponse();
        syncResponseListener.onSuccess(mockResponse);
        Response response = syncResponseListener.get();
        assertSame(response, mockResponse);

        try {
            syncResponseListener.onSuccess(mockResponse);
            fail("get should have failed");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "response is already set");
        }
        response = syncResponseListener.get();
        assertSame(response, mockResponse);
    }

    public void testOnFailure() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        RuntimeException firstException = new RuntimeException("first-test");
        syncResponseListener.onFailure(firstException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (RuntimeException e) {
            assertEquals(firstException.getMessage(), e.getMessage());
            assertSame(firstException, e.getCause());
        }

        RuntimeException secondException = new RuntimeException("second-test");
        try {
            syncResponseListener.onFailure(secondException);
        } catch(IllegalStateException e) {
            assertEquals(e.getMessage(), "exception is already set");
        }
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (RuntimeException e) {
            assertEquals(firstException.getMessage(), e.getMessage());
            assertSame(firstException, e.getCause());
        }

        Response response = mockResponse();
        syncResponseListener.onSuccess(response);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (IllegalStateException e) {
            assertEquals("response and exception are unexpectedly set at the same time", e.getMessage());
            assertNotNull(e.getSuppressed());
            assertEquals(1, e.getSuppressed().length);
            assertSame(firstException, e.getSuppressed()[0]);
        }
    }

    public void testRuntimeIsBuiltCorrectly() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        RuntimeException runtimeException = new RuntimeException();
        syncResponseListener.onFailure(runtimeException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (RuntimeException e) {
            // We preserve the original exception in the cause
            assertSame(runtimeException, e.getCause());
            // We copy the message
            assertEquals(runtimeException.getMessage(), e.getMessage());
            // And we do all that so the thrown exception has our method in the stacktrace
            assertExceptionStackContainsCallingMethod(e);
        }
    }

    public void testConnectTimeoutExceptionIsBuiltCorrectly() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        ConnectTimeoutException timeoutException = new ConnectTimeoutException();
        syncResponseListener.onFailure(timeoutException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (IOException e) {
            // We preserve the original exception in the cause
            assertSame(timeoutException, e.getCause());
            // We copy the message
            assertEquals(timeoutException.getMessage(), e.getMessage());
            // And we do all that so the thrown exception has our method in the stacktrace
            assertExceptionStackContainsCallingMethod(e);
        }
    }

    public void testSocketTimeoutExceptionIsBuiltCorrectly() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        SocketTimeoutException timeoutException = new SocketTimeoutException();
        syncResponseListener.onFailure(timeoutException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (IOException e) {
            // We preserve the original exception in the cause
            assertSame(timeoutException, e.getCause());
            // We copy the message
            assertEquals(timeoutException.getMessage(), e.getMessage());
            // And we do all that so the thrown exception has our method in the stacktrace
            assertExceptionStackContainsCallingMethod(e);
        }
    }

    public void testConnectionClosedExceptionIsWrapped() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        ConnectionClosedException closedException = new ConnectionClosedException(randomAsciiAlphanumOfLength(5));
        syncResponseListener.onFailure(closedException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (ConnectionClosedException e) {
            // We preserve the original exception in the cause
            assertSame(closedException, e.getCause());
            // We copy the message
            assertEquals(closedException.getMessage(), e.getMessage());
            // And we do all that so the thrown exception has our method in the stacktrace
            assertExceptionStackContainsCallingMethod(e);
        }
    }

    public void testSSLHandshakeExceptionIsWrapped() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        SSLHandshakeException exception = new SSLHandshakeException(randomAsciiAlphanumOfLength(5));
        syncResponseListener.onFailure(exception);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (SSLHandshakeException e) {
            // We preserve the original exception in the cause
            assertSame(exception, e.getCause());
            // We copy the message
            assertEquals(exception.getMessage(), e.getMessage());
            // And we do all that so the thrown exception has our method in the stacktrace
            assertExceptionStackContainsCallingMethod(e);
        }
    }

    public void testIOExceptionIsBuiltCorrectly() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        IOException ioException = new IOException();
        syncResponseListener.onFailure(ioException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (IOException e) {
            // We preserve the original exception in the cause
            assertSame(ioException, e.getCause());
            // We copy the message
            assertEquals(ioException.getMessage(), e.getMessage());
            // And we do all that so the thrown exception has our method in the stacktrace
            assertExceptionStackContainsCallingMethod(e);
        }
    }

    public void testExceptionIsWrapped() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        //we just need any checked exception
        URISyntaxException exception = new URISyntaxException("test", "test");
        syncResponseListener.onFailure(exception);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch (RuntimeException e) {
            assertEquals("error while performing request", e.getMessage());
            // We preserve the original exception in the cause
            assertSame(exception, e.getCause());
            // And we do all that so the thrown exception has our method in the stacktrace
            assertExceptionStackContainsCallingMethod(e);
        }
    }

    private static Response mockResponse() {
        ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        RequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        StatusLine statusLine = new BasicStatusLine(protocolVersion, 200, "OK");
        HttpResponse httpResponse = new BasicHttpResponse(statusLine);
        return new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
    }
}
