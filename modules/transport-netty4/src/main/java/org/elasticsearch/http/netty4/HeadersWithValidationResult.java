/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;

import org.apache.lucene.util.SetOnce;

public class HeadersWithValidationResult extends DefaultHttpHeaders {

    private final SetOnce<HeadersValidationResult> validationResult = new SetOnce<>();

    public static DefaultHttpRequest wrapRequestWithValidatableHeaders(DefaultHttpRequest originalRequest, boolean validateHeaders) {
        HeadersWithValidationResult httpHeaders = new HeadersWithValidationResult(originalRequest.headers(), validateHeaders);
        return new DefaultHttpRequest(originalRequest.protocolVersion(), originalRequest.method(), originalRequest.uri(), httpHeaders);
    }

    public HeadersWithValidationResult(HttpHeaders httpHeaders, boolean validateHeaders) {
        super(validateHeaders);
        add(httpHeaders);
    }

    public HeadersValidationResult getValidationResult() {
        return validationResult.get();
    }

    public void setValidationResult(HeadersValidationResult validationResult) {
        this.validationResult.set(validationResult);
    }
}
