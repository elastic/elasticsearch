/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class InferenceProcessor extends AbstractProcessor {

    private static final Logger logger = LogManager.getLogger(InferenceProcessor.class);
    public static final String TYPE = "inference";
    private static final String MODEL_NAME = "model";
    private static final String TARGET_FIELD = "target_field";


    private final Model model;
    private final String modelId;


    public InferenceProcessor(String tag, String modelId, Model model) {
        super(tag);
        this.modelId = modelId;
        this.model = model;
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

    public static final class Factory implements Processor.Factory {

        private final ModelMetadataManager modelMetadataManager;

        private Map<String, Model> loadedModels;
        private Map<String, ModelLoader> modelLoaders;

        public Factory(Client client, NamedXContentRegistry xContentRegistry) {
            this.modelMetadataManager = new ModelMetadataManager(client, xContentRegistry);
            loadedModels = new HashMap<>();
        }

        @Override
        public InferenceProcessor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config)
                throws Exception {
            String modelId = ConfigurationUtils.readStringProperty(TYPE, tag, config, MODEL_NAME);

            ModelMeta modelMeta;
            try {
                 modelMeta = lookUpModelOrThrow(modelId);
            } catch (Exception e) {
                logger.error("Failed to read the metadata for model " + modelId, e);
                throw e;
            }

            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, TARGET_FIELD);

            if (loadedModels.containsKey(modelMeta.getModelId())) {
                return new InferenceProcessor(tag, modelId, loadedModels.get(modelMeta.getModelId()));
            } else {
                ModelLoader loader = modelLoaders.get(modelMeta.getType());
                if (loader == null) {
                    throw new IllegalStateException("Cannot find loader for model type " + modelMeta.getType());
                }

                Model model = loader.load();
                loadedModels.put(modelMeta.getModelId(), model);
                return new InferenceProcessor(tag, modelId, model);
            }
        }

        private ModelMeta lookUpModelOrThrow(String modelId) throws Exception {

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<ModelMeta> response = new AtomicReference<>();
            AtomicReference<Exception> fail = new AtomicReference<>();
            LatchedActionListener<ModelMeta> latchedListener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, fail::set),
                    latch);


            modelMetadataManager.getModelMeta(modelId, latchedListener);
            latch.await();

            if (fail.get() != null) {
                throw fail.get();
            }
            return response.get();
        }
    }
}
