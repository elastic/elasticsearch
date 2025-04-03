/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.SecureString;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public record JinaAIAccount(URI uri, SecureString apiKey) {

    public static JinaAIAccount of(JinaAIModel model, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        var uri = buildUri(model.uri(), "JinaAI", uriBuilder);

        return new JinaAIAccount(uri, model.apiKey());
    }

    public JinaAIAccount {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(apiKey);
    }
}
