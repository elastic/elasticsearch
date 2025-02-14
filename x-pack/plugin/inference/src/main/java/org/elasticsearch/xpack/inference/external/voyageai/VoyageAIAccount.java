/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.voyageai;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public record VoyageAIAccount(URI uri, SecureString apiKey) {

    public static VoyageAIAccount of(VoyageAIModel model, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        var uri = buildUri(model.uri(), "VoyageAI", uriBuilder);

        return new VoyageAIAccount(uri, model.apiKey());
    }

    public VoyageAIAccount {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(apiKey);
    }
}
