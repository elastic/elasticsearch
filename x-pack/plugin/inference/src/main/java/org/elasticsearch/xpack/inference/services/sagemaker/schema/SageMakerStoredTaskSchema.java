/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Map;

/**
 * Contains any model-specific settings that are stored in SageMakerTaskSettings.
 * Because TaskSettings are updatable, this object must be able to mutate itself, which we handle through the {@link Builder}.
 */
public interface SageMakerStoredTaskSchema extends ToXContentFragment, VersionedNamedWriteable {
    SageMakerStoredTaskSchema NO_OP = new SageMakerStoredTaskSchema() {

        private static final String NAME = "noop_sagemaker_task_schema";
        private static final Builder NO_OP_BUILDER = new Builder() {
            @Override
            public Builder fromMap(Map<String, Object> map, ValidationException exception) {
                return this;
            }

            @Override
            public SageMakerStoredTaskSchema build() {
                return NO_OP;
            }
        };

        @Override
        public Builder toBuilder() {
            return NO_OP_BUILDER;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ML_INFERENCE_SAGEMAKER;
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }
    };

    default SageMakerStoredTaskSchema update(Map<String, Object> map, ValidationException exception) {
        return toBuilder().fromMap(map, exception).build();
    }

    /**
     * This is called during {@link #update(Map, ValidationException)}.
     * Implementations should set the current field values in the Builder, as the update function is expected to overwrite them.
     */
    Builder toBuilder();

    interface Builder {
        /**
         * The map will either come from the PUT request or the stored value in the model index.
         * It must match the map written by toXContent.
         */
        Builder fromMap(Map<String, Object> map, ValidationException exception);

        SageMakerStoredTaskSchema build();
    }
}
