/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url.http;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class HttpResponseInputStream extends FilterInputStream {
    private final HttpRequestBase request;
    private final CloseableHttpResponse httpResponse;

    public HttpResponseInputStream(HttpRequestBase request, CloseableHttpResponse httpResponse) throws IOException {
        super(httpResponse.getEntity() == null ? EmptyInputStream.INSTANCE : httpResponse.getEntity().getContent());
        this.request = request;
        this.httpResponse = httpResponse;
    }

    public void abort() {
        request.abort();
    }

    @Override
    public void close() throws IOException {
        super.close();
        httpResponse.close();
    }

    private static class EmptyInputStream extends InputStream {
        public static final EmptyInputStream INSTANCE = new EmptyInputStream();

        private EmptyInputStream() {
        }

        @Override
        public int available() {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public void mark(final int readLimit) {
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public int read(final byte[] buf) {
            return -1;
        }

        @Override
        public int read(final byte[] buf, final int off, final int len) {
            return -1;
        }

        @Override
        public void reset() {
        }

        @Override
        public long skip(final long n) {
            return 0L;
        }
    }
}
