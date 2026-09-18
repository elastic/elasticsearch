/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.secrets;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.action.ActionListener;

/**
 * {@link SecretsApplier} that leaves the request unchanged. Used when a model is
 * retrieved without secrets (e.g. a GET request) and a non-null applier is still required.
 */
public final class NoopSecretsApplier implements SecretsApplier {

    public static final NoopSecretsApplier INSTANCE = new NoopSecretsApplier();

    private NoopSecretsApplier() {}

    @Override
    public void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener) {
        listener.onResponse(request);
    }
}
