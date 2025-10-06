/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mock;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_EIS_ELSER_INFERENCE_ID;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_FALLBACK_ELSER_INFERENCE_ID;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;

import java.util.List;

public class TestDefaultInferenceServiceExtension implements InferenceServiceExtension {

    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestDefaultInferenceService::new);
    }

    public static class TestDefaultInferenceService extends TestSparseInferenceServiceExtension.TestInferenceService {

        public static final String NAME = "default_inference_test_service";

        public TestDefaultInferenceService(InferenceServiceFactoryContext context) {
            super(context);
        }

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public List<InferenceService.DefaultConfigId> defaultConfigIds() {
            return List.of(
                new InferenceService.DefaultConfigId(DEFAULT_EIS_ELSER_INFERENCE_ID, MinimalServiceSettings.sparseEmbedding(name()), this),
                new InferenceService.DefaultConfigId(
                    DEFAULT_FALLBACK_ELSER_INFERENCE_ID,
                    MinimalServiceSettings.sparseEmbedding(name()),
                    this
                )
            );
        }

        @Override
        public void defaultConfigs(ActionListener<List<Model>> defaultsListener) {
            defaultsListener.onResponse(
                List.of(
                    new TestServiceModel(
                        DEFAULT_EIS_ELSER_INFERENCE_ID,
                        TaskType.SPARSE_EMBEDDING,
                        name(),
                        new TestSparseInferenceServiceExtension.TestServiceSettings("default_eis_model", null, false),
                        new TestTaskSettings((Integer) null),
                        new TestSecretSettings("default_eis_api_key")
                    ),
                    new TestServiceModel(
                        DEFAULT_FALLBACK_ELSER_INFERENCE_ID,
                        TaskType.SPARSE_EMBEDDING,
                        name(),
                        new TestSparseInferenceServiceExtension.TestServiceSettings("default_fallback_model", null, false),
                        new TestTaskSettings((Integer) null),
                        new TestSecretSettings("default_fallback_api_key")
                    )
                )
            );
        }
    }
}
