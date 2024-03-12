/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.action.ActionListener;

import java.util.List;
import java.util.Map;

public interface ModelRegistry {

    /**
     * Get a model.
     * Secret settings are not included
     * @param inferenceEntityId Model to get
     * @param listener Model listener
     */
    void getModel(String inferenceEntityId, ActionListener<UnparsedModel> listener);

    /**
     * Get a model with its secret settings
     * @param inferenceEntityId Model to get
     * @param listener Model listener
     */
    void getModelWithSecrets(String inferenceEntityId, ActionListener<UnparsedModel> listener);

    /**
     * Get all models of a particular task type.
     * Secret settings are not included
     * @param taskType The task type
     * @param listener Models listener
     */
    void getModelsByTaskType(TaskType taskType, ActionListener<List<UnparsedModel>> listener);

    /**
     * Get all models.
     * Secret settings are not included
     * @param listener Models listener
     */
    void getAllModels(ActionListener<List<UnparsedModel>> listener);

    void storeModel(Model model, ActionListener<Boolean> listener);

    void deleteModel(String modelId, ActionListener<Boolean> listener);

    /**
     * Semi parsed model where inference entity id, task type and service
     * are known but the settings are not parsed.
     */
    record UnparsedModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> settings,
        Map<String, Object> secrets
    ) {}

    class NoopModelRegistry implements ModelRegistry {
        @Override
        public void getModel(String modelId, ActionListener<UnparsedModel> listener) {
            fail(listener);
        }

        @Override
        public void getModelsByTaskType(TaskType taskType, ActionListener<List<UnparsedModel>> listener) {
            listener.onResponse(List.of());
        }

        @Override
        public void getAllModels(ActionListener<List<UnparsedModel>> listener) {
            listener.onResponse(List.of());
        }

        @Override
        public void storeModel(Model model, ActionListener<Boolean> listener) {
            fail(listener);
        }

        @Override
        public void deleteModel(String modelId, ActionListener<Boolean> listener) {
            fail(listener);
        }

        @Override
        public void getModelWithSecrets(String inferenceEntityId, ActionListener<UnparsedModel> listener) {
            fail(listener);
        }

        private static void fail(ActionListener<?> listener) {
            listener.onFailure(new IllegalArgumentException("No model registry configured"));
        }
    }
}
