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

import java.util.HashMap;
import java.util.Map;

public class InferenceProcessor extends AbstractProcessor {

    private static final Logger logger = LogManager.getLogger(InferenceProcessor.class);
    public static final String TYPE = "inference";
    private static final String MODEL_NAME = "model";
    private static final String MODEL_TYPE = "model_type";
    private static final String IGNORE_MISSING = "ignore_missing";


    private final Model model;
    private final String modelId;


    public InferenceProcessor(String tag, String modelId, Model model) {
        super(tag);
        this.modelId = modelId;
        this.model = model;
    }
    @Override
    public IngestDocument execute(IngestDocument document) {

        return model.infer(document);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getModelId() {
        return modelId;
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
            String modelId = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_NAME);
            String modelType = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_TYPE);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, IGNORE_MISSING, false);

            if (loadedModels.containsKey(modelId)) {

                ModelLoader loader = modelLoaders.get(modelType);
                // read the model's config even though it isn't use as it is an error
                // to leave parsed config
                loader.readConfiguration(tag, config);

                return new InferenceProcessor(tag, modelId, loadedModels.get(modelId));
            } else {
                ModelLoader loader = modelLoaders.get(modelType);
                if (loader == null) {
                    throw new IllegalStateException("Cannot find loader for model type " + modelType);
                }

                logger.info("got model loader");
                Model model = loader.load(modelId, tag, ignoreMissing, config);
                loadedModels.put(modelId, model);
                return new InferenceProcessor(tag, modelId, model);
            }
        }
    }
}
