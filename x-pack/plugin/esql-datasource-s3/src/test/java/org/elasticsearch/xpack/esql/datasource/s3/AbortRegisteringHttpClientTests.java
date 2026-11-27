/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbortRegisteringHttpClientTests extends ESTestCase {

    public void testPrepareRequestRegistersAbortOnSinkDuringCall() throws IOException {
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean registeredDuringCall = new AtomicBoolean();
        Set<Abortable> sink = ConcurrentHashMap.newKeySet();
        ExecutableHttpRequest inner = new ExecutableHttpRequest() {
            @Override
            public HttpExecuteResponse call() {
                registeredDuringCall.set(sink.size() == 1);
                sink.forEach(Abortable::abort);
                return mock(HttpExecuteResponse.class);
            }

            @Override
            public void abort() {
                aborted.set(true);
            }
        };
        SdkHttpClient delegate = mock(SdkHttpClient.class);
        when(delegate.prepareRequest(any(HttpExecuteRequest.class))).thenReturn(inner);

        Set<Abortable> executes = ConcurrentHashMap.newKeySet();
        S3StorageObject.HTTP_ABORT_SINK.set(sink);
        S3StorageObject.HTTP_ABORT_EXECUTES.set(executes);
        try {
            AbortRegisteringHttpClient client = new AbortRegisteringHttpClient(delegate);
            ExecutableHttpRequest wrapped = client.prepareRequest(executeRequest());
            assertTrue(sink.isEmpty());
            wrapped.call();
            assertTrue("execute must be on the sink while Apache is blocked in call()", registeredDuringCall.get());
            assertTrue(aborted.get());
            assertEquals("execute stays abortable until getObject returns, including error-body reads", 1, sink.size());
            assertEquals(1, executes.size());
            sink.removeAll(executes);
            assertTrue("trackedSyncGet finally drops the execute handle after getObject returns", sink.isEmpty());
        } finally {
            S3StorageObject.HTTP_ABORT_SINK.remove();
            S3StorageObject.HTTP_ABORT_EXECUTES.remove();
        }
    }

    public void testClosedFlagAbortsWithoutCallingInner() throws IOException {
        AtomicBoolean innerCalled = new AtomicBoolean();
        AtomicBoolean aborted = new AtomicBoolean();
        ExecutableHttpRequest inner = new ExecutableHttpRequest() {
            @Override
            public HttpExecuteResponse call() {
                innerCalled.set(true);
                return mock(HttpExecuteResponse.class);
            }

            @Override
            public void abort() {
                aborted.set(true);
            }
        };
        SdkHttpClient delegate = mock(SdkHttpClient.class);
        when(delegate.prepareRequest(any(HttpExecuteRequest.class))).thenReturn(inner);

        S3StorageObject.HTTP_ABORT_CLOSED.set(new AtomicBoolean(true));
        try {
            AbortRegisteringHttpClient client = new AbortRegisteringHttpClient(delegate);
            ExecutableHttpRequest wrapped = client.prepareRequest(executeRequest());
            expectThrows(AbortedException.class, wrapped::call);
            assertTrue(aborted.get());
            assertFalse(innerCalled.get());
        } finally {
            S3StorageObject.HTTP_ABORT_CLOSED.remove();
        }
    }

    public void testFailedCallKeepsAbortHandle() throws IOException {
        ExecutableHttpRequest inner = new ExecutableHttpRequest() {
            @Override
            public HttpExecuteResponse call() throws IOException {
                throw new IOException("boom");
            }

            @Override
            public void abort() {}
        };
        SdkHttpClient delegate = mock(SdkHttpClient.class);
        when(delegate.prepareRequest(any(HttpExecuteRequest.class))).thenReturn(inner);

        Set<Abortable> sink = ConcurrentHashMap.newKeySet();
        S3StorageObject.HTTP_ABORT_SINK.set(sink);
        try {
            AbortRegisteringHttpClient client = new AbortRegisteringHttpClient(delegate);
            ExecutableHttpRequest wrapped = client.prepareRequest(executeRequest());
            expectThrows(IOException.class, wrapped::call);
            assertEquals("failed call keeps the handle so close() can abort an error-body stall", 1, sink.size());
        } finally {
            S3StorageObject.HTTP_ABORT_SINK.remove();
        }
    }

    private static HttpExecuteRequest executeRequest() {
        return HttpExecuteRequest.builder()
            .request(SdkHttpRequest.builder().uri(URI.create("http://localhost")).method(SdkHttpMethod.GET).build())
            .build();
    }
}
