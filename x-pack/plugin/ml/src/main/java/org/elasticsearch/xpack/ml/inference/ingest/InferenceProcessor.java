/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;

import java.util.function.BiConsumer;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.Processor;

import java.util.Map;
import java.util.function.Consumer;

public class InferenceProcessor extends AbstractProcessor {

    public static final String TYPE = "inference";
    public static final String MODEL_ID = "model_id";

    private final Client client;
    private final String modelId;

    public InferenceProcessor(Client client, String tag, String modelId) {
        super(tag);
        this.client = client;
        this.modelId = modelId;
    }

    public String getModelId() {
        return modelId;
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
        return TYPE;
    }

    public static class Factory implements Processor.Factory, Consumer<ClusterState> {

        private final Client client;
        private final ClusterService clusterService;

        public Factory(Client client, ClusterService clusterService, Settings settings) {
            this.client = client;
            this.clusterService = clusterService;
        }

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config)
            throws Exception {
            String modelId = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_ID);
            return new InferenceProcessor(client, tag, modelId);
        }

        @Override
        public void accept(ClusterState clusterState) {

        }
    }
}
