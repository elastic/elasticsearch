/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;

import java.io.FilterInputStream;
import java.io.IOException;

public class HttpResponseInputStream extends FilterInputStream {
    private final HttpRequestBase request;
    private final CloseableHttpResponse httpResponse;

    public HttpResponseInputStream(HttpRequestBase request, CloseableHttpResponse httpResponse) throws IOException {
        super(httpResponse.getEntity().getContent());
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
}
