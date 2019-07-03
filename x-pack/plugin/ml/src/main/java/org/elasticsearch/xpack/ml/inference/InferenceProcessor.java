/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public class InferenceProcessor extends AbstractProcessor {

    public static final String TYPE = "inference";

    private final String targetField;


    public InferenceProcessor(String tag, String targetField) {
        super(tag);
        this.targetField = targetField;
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

    public static final class Factory implements Processor.Factory {

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) {

            String targetField = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "target_field");
            return new InferenceProcessor(tag, targetField);
        }
    }
}
