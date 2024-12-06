/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;

class DelegatingRestChannel implements RestChannel {

    private final RestChannel delegate;

    DelegatingRestChannel(RestChannel delegate) {
        this.delegate = delegate;
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
        return delegate.newBuilder();
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
        return delegate.newErrorBuilder();
    }

    @Override
    public XContentBuilder newBuilder(@Nullable XContentType xContentType, boolean useFiltering) throws IOException {
        return delegate.newBuilder(xContentType, useFiltering);
    }

    @Override
    public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering)
        throws IOException {
        return delegate.newBuilder(xContentType, responseContentType, useFiltering);
    }

    @Override
    public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering, OutputStream out)
        throws IOException {
        return delegate.newBuilder(xContentType, responseContentType, useFiltering, out);
    }

    @Override
    public BytesStream bytesOutput() {
        return delegate.bytesOutput();
    }

    @Override
    public void releaseOutputBuffer() {
        delegate.releaseOutputBuffer();
    }

    @Override
    public RestRequest request() {
        return delegate.request();
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return delegate.detailedErrorsEnabled();
    }

    @Override
    public void sendResponse(RestResponse response) {
        delegate.sendResponse(response);
    }
}
