/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.action.ActionListener;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Applies a header to a request. Mainly used to apply secrets (like the entra ID or API key) to requests.
 * @param headerSupplier a supplier to retrieve the header to apply to the request
 */
public record AzureOpenAiHeaderApplier(Supplier<Header> headerSupplier) implements AzureOpenAiSecretsApplier {

    public AzureOpenAiHeaderApplier(Supplier<Header> headerSupplier) {
        this.headerSupplier = Objects.requireNonNull(headerSupplier);
    }

    @Override
    public void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener) {
        request.setHeader(headerSupplier.get());
        listener.onResponse(request);
    }
}
