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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class InferenceProcessor extends AbstractProcessor {

    public static final String TYPE = "inference";
    private static final String MODEL_ID = "model_id";
    private static final String MODEL_TYPE = "model_type";
    private static final String IGNORE_MISSING = "ignore_missing";


    private final Model model;

    public InferenceProcessor(String tag, Model model) {
        super(tag);
        this.model = model;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        model.infer(ingestDocument, handler);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        assert false : "The async override of execute() must be used";
        return null;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private Map<String, Model> loadedModels;
        private Map<String, ModelLoader> modelLoaders;

        // If a client is needed here then in the Node ctor Plugin.createComponents
        // should be called before the IngestService is created
        public Factory(Map<String, ModelLoader> modelLoaders) {
            loadedModels = new HashMap<>();
            this.modelLoaders = modelLoaders;
        }

        @Override
        public InferenceProcessor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config)
                throws Exception {
            String modelId = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_ID);
            String modelType = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_TYPE);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, IGNORE_MISSING, false);

            if (loadedModels.containsKey(modelId)) {

                ModelLoader loader = modelLoaders.get(modelType);
                // read the model's config even though we don't need it here.
                // it is an error to leave options in the config map
                loader.consumeConfiguration(tag, config);

                return new InferenceProcessor(tag, loadedModels.get(modelId));
            } else {
                ModelLoader loader = modelLoaders.get(modelType);
                if (loader == null) {
                    throw new IllegalStateException("Cannot find loader for model type " + modelType);
                }

                Model model = loader.load(modelId, tag, ignoreMissing, config);
                loadedModels.put(modelId, model);
                return new InferenceProcessor(tag, model);
            }
        }
    }
}
