/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;

import java.util.List;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public final class AmazonBedrockEmbeddingsEntityFactory {
    public static ToXContentObject createEntity(
        AmazonBedrockEmbeddingsModel model,
        List<String> inputs,
        Truncator truncator,
        @Nullable Integer maxInputTokens
    ) {
        var serviceSettings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
        var truncatedInput = truncate(inputs, serviceSettings.maxInputTokens());

        switch (serviceSettings.provider()) {
            case AmazonTitan -> {
                // TODO -- can we just use the first one here? Or do we need to warn?
                return new AmazonBedrockTitanEmbeddingsRequestEntity(truncatedInput.input().get(0));
            }
            case Cohere -> {
                return new AmazonBedrockCohereEmbeddingsRequestEntity(truncatedInput.input());
            }
            default -> {
                return null;
            }
        }

    }
}
