/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Map;

/**
 * Contains any model-specific settings that are stored in SageMakerTaskSettings.
 */
public interface SageMakerStoredTaskSchema extends TaskSettings {
    SageMakerStoredTaskSchema NO_OP = new SageMakerStoredTaskSchema() {

        private static final TransportVersion ML_INFERENCE_SAGEMAKER = TransportVersion.fromName("ml_inference_sagemaker");

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public SageMakerStoredTaskSchema updatedTaskSettings(Map<String, Object> newSettings) {
            return this;
        }

        private static final String NAME = "noop_sagemaker_task_schema";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            assert false : "should never be called when supportsVersion is used";
            return ML_INFERENCE_SAGEMAKER;
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return version.supports(ML_INFERENCE_SAGEMAKER);
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }
    };

    /**
     * These extra service settings serialize flatly alongside the overall SageMaker ServiceSettings.
     */
    @Override
    default boolean isFragment() {
        return true;
    }

    @Override
    SageMakerStoredTaskSchema updatedTaskSettings(Map<String, Object> newSettings);

    default SageMakerStoredTaskSchema override(Map<String, Object> newSettings) {
        return updatedTaskSettings(newSettings);
    }
}
