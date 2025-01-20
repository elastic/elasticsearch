/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import org.elasticsearch.core.Nullable;

import java.util.List;

public record AmazonBedrockConverseRequestEntity(
    List<String> messages,
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Integer maxTokenCount,
    @Nullable List<String> additionalModelFields
) {
    public AmazonBedrockConverseRequestEntity(
        List<String> messages,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Integer maxTokenCount
    ) {
        this(messages, temperature, topP, maxTokenCount, null);
    }
}
