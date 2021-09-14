/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

public class HttpPipelinedResponse implements HttpPipelinedMessage, HttpResponse {

    private final int sequence;
    private final HttpResponse delegate;

    public HttpPipelinedResponse(int sequence, HttpResponse delegate) {
        this.sequence = sequence;
        this.delegate = delegate;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    @Override
    public void addHeader(String name, String value) {
        delegate.addHeader(name, value);
    }

    @Override
    public boolean containsHeader(String name) {
        return delegate.containsHeader(name);
    }

    public HttpResponse getDelegateRequest() {
        return delegate;
    }
}
