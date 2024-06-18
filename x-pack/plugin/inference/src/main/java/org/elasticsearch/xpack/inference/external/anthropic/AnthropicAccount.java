/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.anthropic;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public record AnthropicAccount(URI uri, SecureString apiKey) {

    public static AnthropicAccount of(AnthropicModel model, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        var uri = buildUri(model.rateLimitServiceSettings().uri(), "Anthropic", uriBuilder);

        return new AnthropicAccount(uri, model.apiKey());
    }

    public AnthropicAccount {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(apiKey);
    }
}
