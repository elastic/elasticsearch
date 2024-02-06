/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.textembedding;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings;

import java.io.IOException;

import static org.elasticsearch.TransportVersions.ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;

public class TextEmbeddingInternalServiceSettings extends InternalServiceSettings {

    public static final String NAME = "text_embedding_internal_service_settings";

    public TextEmbeddingInternalServiceSettings(int numAllocations, int numThreads, String modelVariant) {
        super(numAllocations, numThreads, modelVariant);
    }

    public TextEmbeddingInternalServiceSettings(StreamInput in) throws IOException {
        super(in.readVInt(), in.readVInt(), in.readString());
    }

    @Override
    public String getWriteableName() {
        return TextEmbeddingInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;
    }

}
