/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class InferenceProviderRegistry {

    private final Map<String, InferenceProvider> inferenceProviderMap;

    private InferenceProviderRegistry(Map <String, InferenceProvider> inferenceProviderMap) {
         this.inferenceProviderMap = inferenceProviderMap;
    }

    private record InferenceProvider(Model model, InferenceService service) {
        private InferenceProvider {
            Objects.requireNonNull(model);
            Objects.requireNonNull(service);
        }
    }

    public static void getInstance(
        Collection<String> inferenceIds,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry inferenceServiceRegistry,
        ActionListener<InferenceProviderRegistry> listener
    ) {
        final Map<String, InferenceProvider> inferenceProviderMap = new ConcurrentHashMap<>();
        Runnable onModelLoadingComplete = () -> listener.onResponse(new InferenceProviderRegistry(inferenceProviderMap));
        try (var refs = new RefCountingRunnable(onModelLoadingComplete)) {
            for (var inferenceId : inferenceIds) {
                ActionListener<ModelRegistry.UnparsedModel> modelLoadingListener = new ActionListener<>() {
                    @Override
                    public void onResponse(ModelRegistry.UnparsedModel unparsedModel) {
                        var service = inferenceServiceRegistry.getService(unparsedModel.service());
                        if (service.isEmpty() == false) {
                            InferenceProvider inferenceProvider = new InferenceProvider(
                                service.get()
                                    .parsePersistedConfigWithSecrets(
                                        inferenceId,
                                        unparsedModel.taskType(),
                                        unparsedModel.settings(),
                                        unparsedModel.secrets()
                                    ),
                                service.get()
                            );
                            inferenceProviderMap.put(inferenceId, inferenceProvider);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // Failure on loading a model should not prevent the rest from being loaded and used.
                        // When the model is actually retrieved via the inference ID in the inference process, it will fail
                        // and the user will get the details on the inference failure.
                    }
                };

                modelRegistry.getModelWithSecrets(inferenceId, ActionListener.releaseAfter(modelLoadingListener, refs.acquire()));
            }
        }
    }

    public Model getModel(String inferenceId) {
        return inferenceProviderMap.get(inferenceId).model;
    }

    public InferenceService getInfereceService(String inferenceId) {
        return inferenceProviderMap.get(inferenceId).service;
    }
}
