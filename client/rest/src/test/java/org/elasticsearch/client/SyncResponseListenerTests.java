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

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class SyncResponseListenerTests extends RestClientTestCase {

    public void testOnSuccessNullResponse() {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        try {
            syncResponseListener.onSuccess(null);
            fail("onSuccess should have failed");
        } catch(NullPointerException e) {
            assertEquals("response must not be null", e.getMessage());
        }
    }

    public void testOnFailureNullException() {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        try {
            syncResponseListener.onFailure(null);
            fail("onFailure should have failed");
        } catch(NullPointerException e) {
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
        } catch(IllegalStateException e) {
            assertEquals(e.getMessage(), "response is already set");
        }
        response = syncResponseListener.get();
        assertSame(response, mockResponse);

        RuntimeException runtimeException = new RuntimeException("test");
        syncResponseListener.onFailure(runtimeException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch(IllegalStateException e) {
            assertEquals("response and exception are unexpectedly set at the same time", e.getMessage());
            assertNotNull(e.getSuppressed());
            assertEquals(1, e.getSuppressed().length);
            assertSame(runtimeException, e.getSuppressed()[0]);
        }
    }

    public void testOnFailure() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        RuntimeException firstException = new RuntimeException("first-test");
        syncResponseListener.onFailure(firstException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch(RuntimeException e) {
            assertSame(firstException, e);
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
        } catch(RuntimeException e) {
            assertSame(firstException, e);
        }

        Response response = mockResponse();
        syncResponseListener.onSuccess(response);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch(IllegalStateException e) {
            assertEquals("response and exception are unexpectedly set at the same time", e.getMessage());
            assertNotNull(e.getSuppressed());
            assertEquals(1, e.getSuppressed().length);
            assertSame(firstException, e.getSuppressed()[0]);
        }
    }

    public void testRuntimeExceptionIsNotWrapped() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        RuntimeException runtimeException = new RuntimeException();
        syncResponseListener.onFailure(runtimeException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch(RuntimeException e) {
            assertSame(runtimeException, e);
        }
    }

    public void testIOExceptionIsNotWrapped() throws Exception {
        RestClient.SyncResponseListener syncResponseListener = new RestClient.SyncResponseListener(10000);
        IOException ioException = new IOException();
        syncResponseListener.onFailure(ioException);
        try {
            syncResponseListener.get();
            fail("get should have failed");
        } catch(IOException e) {
            assertSame(ioException, e);
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
        } catch(RuntimeException e) {
            assertEquals("error while performing request", e.getMessage());
            assertSame(exception, e.getCause());
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
