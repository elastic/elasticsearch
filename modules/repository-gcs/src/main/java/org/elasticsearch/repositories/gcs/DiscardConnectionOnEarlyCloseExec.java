/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.apache.hc.client5.http.classic.ExecChain;
import org.apache.hc.client5.http.classic.ExecChainHandler;
import org.apache.hc.client5.http.classic.ExecRuntime;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.entity.HttpEntityWrapper;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Makes closing a response whose body was not read to the end discard the connection instead of draining the rest of the body.
 * <p>
 * Apache HttpClient keeps a connection reusable by reading and discarding the remainder of a response body when the body stream
 * or the response is closed before EOF. It does so synchronously on the closing thread, without a size limit, and it reports
 * failures of that drain from {@code close()}. The JDK {@code HttpURLConnection} transport that this module used before did not
 * behave like that: it closed the socket unless only a few KB were left. Draining is a problem here because
 * {@link GoogleCloudStorageRetryingInputStream} closes a stream whose read failed before reopening the blob at the current
 * offset: on a stuck connection the drain blocks for another read timeout, on a slow one it downloads bytes that are requested
 * again right after, and on a broken one {@code close()} throws. This handler does what the S3 repository does with
 * {@code abort()}: if the body was not read to the end when it is closed, the connection is closed and returned to the pool as
 * unusable, which is immediate and never fails. Fully read bodies release their connection for reuse as usual.
 */
final class DiscardConnectionOnEarlyCloseExec implements ExecChainHandler {

    static final String NAME = "discard-connection-on-early-close";

    @Override
    public ClassicHttpResponse execute(ClassicHttpRequest request, ExecChain.Scope scope, ExecChain chain) throws IOException,
        HttpException {
        final ClassicHttpResponse response = chain.proceed(request, scope);
        final HttpEntity entity = response.getEntity();
        if (entity != null) {
            response.setEntity(new DiscardOnEarlyCloseEntity(entity, scope.execRuntime));
        }
        return response;
    }

    private static final class DiscardOnEarlyCloseEntity extends HttpEntityWrapper {

        private final ExecRuntime execRuntime;
        private final long contentLength;
        private long bytesRead;
        private boolean eof;

        DiscardOnEarlyCloseEntity(HttpEntity entity, ExecRuntime execRuntime) {
            super(entity);
            this.execRuntime = execRuntime;
            this.contentLength = entity.getContentLength();
        }

        private boolean isFullyConsumed() {
            return eof || (contentLength >= 0 && bytesRead >= contentLength);
        }

        @Override
        public InputStream getContent() throws IOException {
            return new FilterInputStream(super.getContent()) {
                @Override
                public int read() throws IOException {
                    final int b = in.read();
                    if (b == -1) {
                        eof = true;
                    } else {
                        bytesRead++;
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    final int n = in.read(b, off, len);
                    if (n == -1) {
                        eof = true;
                    } else {
                        bytesRead += n;
                    }
                    return n;
                }

                @Override
                public long skip(long n) throws IOException {
                    final long skipped = in.skip(n);
                    bytesRead += skipped;
                    return skipped;
                }

                @Override
                public void close() throws IOException {
                    if (isFullyConsumed()) {
                        in.close();
                    } else {
                        // the wrapped stream is left alone on purpose: closing it would drain the connection we are discarding
                        execRuntime.discardEndpoint();
                    }
                }
            };
        }

        @Override
        public void close() throws IOException {
            if (isFullyConsumed()) {
                super.close();
            } else {
                // see the stream close() above; a no-op if the connection was already discarded after a failed read
                execRuntime.discardEndpoint();
            }
        }
    }
}
