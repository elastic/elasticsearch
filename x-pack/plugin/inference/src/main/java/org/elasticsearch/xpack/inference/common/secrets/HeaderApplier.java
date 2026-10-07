/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.secrets;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.core5.http.Header;
import org.elasticsearch.action.ActionListener;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Applies a header (e.g. API key bearer token, Entra ID token) to an outbound inference request.
 * @param headerSupplier a supplier to retrieve the header to apply to the request
 */
public record HeaderApplier(Supplier<Header> headerSupplier) implements SecretsApplier {

    public HeaderApplier(Supplier<Header> headerSupplier) {
        this.headerSupplier = Objects.requireNonNull(headerSupplier);
    }

    @Override
    public void applyTo(SimpleHttpRequest request, ActionListener<SimpleHttpRequest> listener) {
        request.setHeader(headerSupplier.get());
        listener.onResponse(request);
    }
}
