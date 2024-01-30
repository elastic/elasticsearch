/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.TextEmbedding;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.inference.services.settings.MlNodeDeployedServiceSettings;

import static org.elasticsearch.TransportVersions.ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;

public class TextEmbeddingServiceSettings extends MlNodeDeployedServiceSettings {

    public static final String NAME = "text_embedding_service_settings";

    public TextEmbeddingServiceSettings(int numAllocations, int numThreads, String modelVariant) {
        super(numAllocations, numThreads, modelVariant);
    }

    @Override
    public String getWriteableName() {
        return TextEmbeddingServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;
    }

}
