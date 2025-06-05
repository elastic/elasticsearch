/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.elasticsearch.common.settings.SecureString;

import java.net.URI;
import java.util.Objects;

public record AnthropicAccount(URI uri, SecureString apiKey) {

    public static AnthropicAccount of(AnthropicModel model) {
        return new AnthropicAccount(model.getUri(), model.apiKey());
    }

    public AnthropicAccount {
        Objects.requireNonNull(uri);
        Objects.requireNonNull(apiKey);
    }
}
