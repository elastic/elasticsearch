/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.SecureString;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public record CohereAccount(URI uri, SecureString apiKey) {

    public static CohereAccount of(CohereModel model, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        var uri = buildUri(model.uri(), "Cohere", uriBuilder);

        return new CohereAccount(uri, model.apiKey());
    }

    public CohereAccount {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(apiKey);
    }
}
