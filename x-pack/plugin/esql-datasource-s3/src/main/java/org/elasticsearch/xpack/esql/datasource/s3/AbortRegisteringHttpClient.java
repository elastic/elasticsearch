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
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;

/**
 * Apache {@link SdkHttpClient} that registers each in-flight execute with
 * {@link S3StorageObject}'s abort sink so {@link S3StorageObject#close()} can drop the socket
 * while {@code S3Client.getObject} is still blocked on headers or reading an error body. Apache
 * {@code HttpRequestBase.abort()} is safe from another thread; the AWS Netty adapter is not.
 */
final class AbortRegisteringHttpClient implements SdkHttpClient {

    private final SdkHttpClient delegate;

    AbortRegisteringHttpClient(SdkHttpClient delegate) {
        this.delegate = delegate;
    }

    static final class Builder implements SdkHttpClient.Builder<Builder> {
        private final int maxConnections;
        private final Duration connectionAcquisitionTimeout;

        Builder(int maxConnections, Duration connectionAcquisitionTimeout) {
            this.maxConnections = maxConnections;
            this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
        }

        @Override
        public SdkHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
            return new AbortRegisteringHttpClient(
                ApacheHttpClient.builder()
                    .maxConnections(maxConnections)
                    .connectionAcquisitionTimeout(connectionAcquisitionTimeout)
                    .buildWithDefaults(serviceDefaults)
            );
        }
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
        ExecutableHttpRequest inner = delegate.prepareRequest(request);
        return new ExecutableHttpRequest() {
            @Override
            public HttpExecuteResponse call() throws IOException {
                if (S3StorageObject.httpAbortClosed()) {
                    inner.abort();
                    throw AbortedException.create("read cancelled");
                }
                Set<Abortable> sink = S3StorageObject.httpAbortSink();
                Set<Abortable> executes = S3StorageObject.httpAbortExecutes();
                if (sink != null) {
                    sink.add(this);
                }
                if (executes != null) {
                    executes.add(this);
                }
                if (S3StorageObject.httpAbortClosed()) {
                    inner.abort();
                    throw AbortedException.create("read cancelled");
                }
                // Stay on the sink until trackedSyncGet finishes. Apache execute() returns after
                // headers; S3Client.getObject still reads an error XML body after that, and close()
                // must be able to abort that stall.
                return inner.call();
            }

            @Override
            public void abort() {
                inner.abort();
            }
        };
    }

    @Override
    public String clientName() {
        return delegate.clientName();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
