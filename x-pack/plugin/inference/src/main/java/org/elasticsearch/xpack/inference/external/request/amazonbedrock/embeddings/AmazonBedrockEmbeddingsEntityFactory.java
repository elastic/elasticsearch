/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockJsonWriter;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;

public final class AmazonBedrockEmbeddingsEntityFactory {
    public static AmazonBedrockJsonWriter createEntity(AmazonBedrockEmbeddingsModel model, Truncator.TruncationResult truncationResult) {
        var serviceSettings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();

        var truncatedInput = truncationResult.input();
        if (truncatedInput == null || truncatedInput.isEmpty()) {
            throw new ElasticsearchException("[input] cannot be null or empty");
        }

        switch (serviceSettings.provider()) {
            case AMAZONTITAN -> {
                if (truncatedInput.size() > 1) {
                    throw new ElasticsearchException("[input] cannot contain more than one string");
                }
                return new AmazonBedrockTitanEmbeddingsRequestEntity(truncatedInput.get(0));
            }
            case COHERE -> {
                return new AmazonBedrockCohereEmbeddingsRequestEntity(truncatedInput);
            }
            default -> {
                return null;
            }
        }

    }
}
