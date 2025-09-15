/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

/**
 * Contains any model-specific settings that are stored in SageMakerServiceSettings.
 */
public interface SageMakerStoredServiceSchema extends ServiceSettings {
    SageMakerStoredServiceSchema NO_OP = new SageMakerStoredServiceSchema() {

        private static final String NAME = "noop_sagemaker_service_schema";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ML_INFERENCE_SAGEMAKER_8_19;
        }

        @Override
        public void writeTo(StreamOutput out) {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }
    };

    /**
     * The model is part of the SageMaker Endpoint definition and is not declared in the service settings.
     */
    @Override
    default String modelId() {
        return null;
    }

    @Override
    default ToXContentObject getFilteredXContentObject() {
        return this;
    }

    /**
     * These extra service settings serialize flatly alongside the overall SageMaker ServiceSettings.
     */
    @Override
    default boolean isFragment() {
        return true;
    }

    /**
     * If this Schema supports Text Embeddings, then we need to implement this.
     * {@link org.elasticsearch.xpack.inference.services.validation.TextEmbeddingModelValidator} will set the dimensions if the user
     * does not do it, so we need to store the dimensions and flip the {@link #dimensionsSetByUser()} boolean.
     */
    default SageMakerStoredServiceSchema updateModelWithEmbeddingDetails(Integer dimensions) {
        return this;
    }
}
