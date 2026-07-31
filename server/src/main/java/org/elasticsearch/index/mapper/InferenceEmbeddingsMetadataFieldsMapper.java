/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.util.Map;

/**
 * An abstract {@link MetadataFieldMapper} used as a placeholder for the {@code _inference_embeddings}
 * metadata field whose concrete implementation lives in the inference plugin.
 */
public abstract class InferenceEmbeddingsMetadataFieldsMapper extends MetadataFieldMapper {

    public static final String NAME = "_inference_embeddings";
    public static final String CONTENT_TYPE = "_inference_embeddings";

    protected InferenceEmbeddingsMetadataFieldsMapper(MappedFieldType inferenceEmbeddingsFieldType) {
        super(inferenceEmbeddingsFieldType);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public InferenceEmbeddingsMetadataFieldType fieldType() {
        return (InferenceEmbeddingsMetadataFieldType) super.fieldType();
    }

    /**
     * The field type for {@link InferenceEmbeddingsMetadataFieldsMapper}.
     */
    public abstract static class InferenceEmbeddingsMetadataFieldType extends MappedFieldType {
        public InferenceEmbeddingsMetadataFieldType() {
            super(NAME, IndexType.NONE, false, Map.of());
        }
    }
}
