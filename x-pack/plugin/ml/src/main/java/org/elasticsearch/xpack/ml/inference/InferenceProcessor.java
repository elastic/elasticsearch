/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public class InferenceProcessor extends AbstractProcessor {

    private static final Logger LOGGER = LogManager.getLogger(InferenceProcessor.class);
    public static final String TYPE = "inference";
    private static final String MODEL_NAME = "model";
    private static final String TARGET_FIELD = "target_field";

    private final String targetField;
    private final String modelId;


    public InferenceProcessor(String tag, String modelId, String targetField) {
        super(tag);
        this.targetField = targetField;
        this.modelId = modelId;
    }
    @Override
    public IngestDocument execute(IngestDocument document) {
        document.setFieldValue(targetField, Boolean.TRUE);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getModelId() {
        return modelId;
    }

    public String getTargetField() {
        return targetField;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public InferenceProcessor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) {

            LOGGER.info("Inference processor tag [{}]", tag);
            String model = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_NAME);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, TARGET_FIELD);
            return new InferenceProcessor(tag, model, targetField);
        }
    }
}
