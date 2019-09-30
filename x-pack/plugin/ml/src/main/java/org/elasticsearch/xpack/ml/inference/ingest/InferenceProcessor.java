/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.ml.inference.action.InferModelAction;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;


public class InferenceProcessor extends AbstractProcessor {

    public static final String TYPE = "inference";
    private static final String MODEL_ID = "model_id";
    private static final String MODEL_TYPE = "model_type";
    private static final String TOP_N_CLASSES = "top_n_classes";
    private static final String TARGET_FIELD = "target_field";
    private static final String FIELD_MAPPINGS = "field_mappings";
    private static final String IGNORE_MISSING = "ignore_missing";

    private final Client client;
    private final InferModelRequestGenerator requestGenerator;
    public InferenceProcessor(Client client, String tag, InferModelRequestGenerator requestGenerator) {
        super(tag);
        this.client = client;
        this.requestGenerator = requestGenerator;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        //TODO actually work
        handler.accept(ingestDocument, null);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        throw new UnsupportedOperationException("should never be called");
    }

    @Override
    public String getType() {
        return null;
    }

    public static final class Factory implements Processor.Factory {

        private final Client client;
        public Factory(Client client) {
            this.client = client;
        }

        @Override
        public InferenceProcessor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config)
            throws Exception {
            // TODO add cluster state listener info for processor limits
            String modelId = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_ID);
            String modelType = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_TYPE, "local");
            Map<String, String> fieldMapping = ConfigurationUtils.readMap(TYPE, tag, config, FIELD_MAPPINGS);
            Integer modelVersion = ConfigurationUtils.readIntProperty(TYPE, tag, config, MODEL_TYPE, 0);
            Integer topNClasses = ConfigurationUtils.readIntProperty(TYPE, tag, config, TOP_N_CLASSES, null);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, IGNORE_MISSING, false);
            InferModelRequestGenerator inferModelRequestGenerator = new InferModelRequestGenerator(modelId,
                modelVersion.longValue(),
                topNClasses,
                fieldMapping);
            return new InferenceProcessor(client, tag, inferModelRequestGenerator);
        }
    }

    private static class InferModelRequestGenerator {

        private final String modelId;
        private final Long modelVersion;
        private final Integer topNClasses;
        private final Map<String, String> fieldMapping;

        InferModelRequestGenerator(String modelId, Long modelVersion, Integer topNClasses, Map<String, String> fieldMapping) {
            this.modelId = modelId;
            this.modelVersion = modelVersion;
            this.topNClasses = topNClasses;
            this.fieldMapping = fieldMapping;
        }

        InferModelAction.Request buildRequest(IngestDocument document) {
            Map<String, Object> fields = new HashMap<>(document.getSourceAndMetadata());
            if (fieldMapping != null) {
                fieldMapping.forEach((src, dest) -> {
                    fields.put(dest, fields.get(src));
                });
            }
            return new InferModelAction.Request(modelId, modelVersion, fields, topNClasses);
        }
    }

}
