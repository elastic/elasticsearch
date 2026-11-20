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
import java.util.Set;

/**
 * Apache {@link SdkHttpClient} that registers each in-flight execute with
 * {@link S3StorageObject}'s abort sink so {@link S3StorageObject#close()} can drop the socket
 * while {@code S3Client.getObject} is still blocked on headers. Apache {@code HttpRequestBase.abort()}
 * is safe from another thread; the AWS Netty adapter is not.
 */
final class AbortRegisteringHttpClient implements SdkHttpClient {

    private final SdkHttpClient delegate;

    AbortRegisteringHttpClient(SdkHttpClient delegate) {
        this.delegate = delegate;
    }

    static final class Builder implements SdkHttpClient.Builder<Builder> {
        @Override
        public SdkHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
            return new AbortRegisteringHttpClient(ApacheHttpClient.builder().buildWithDefaults(serviceDefaults));
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
                if (sink != null) {
                    sink.add(this);
                }
                try {
                    if (S3StorageObject.httpAbortClosed()) {
                        inner.abort();
                        throw AbortedException.create("read cancelled");
                    }
                    return inner.call();
                } finally {
                    // Headers are in; wrapLiveGet owns abort of the body. Leaving this handle
                    // in the sink would leak every successful GET until StorageObject.close().
                    if (sink != null) {
                        sink.remove(this);
                    }
                }
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
