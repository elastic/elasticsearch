/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class ModelLoadingService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ModelLoadingService.class);
    private final Map<String, MaybeModel> loadedModels = new HashMap<>();
    private final Map<String, Queue<ActionListener<Model>>> loadingListeners = new HashMap<>();
    private final TrainedModelProvider provider;
    private final ThreadPool threadPool;

    public ModelLoadingService(TrainedModelProvider trainedModelProvider,
                               ThreadPool threadPool,
                               ClusterService clusterService) {
        this.provider = trainedModelProvider;
        this.threadPool = threadPool;
        clusterService.addListener(this);
    }

    public void getModel(String modelId, ActionListener<Model> modelActionListener) {
        MaybeModel cachedModel = loadedModels.get(modelId);
        if (cachedModel != null) {
            if (cachedModel.isSuccess()) {
                modelActionListener.onResponse(cachedModel.getModel());
                return;
            }
        }
        if (loadModelIfNecessary(modelId, modelActionListener) == false) {
            // If we the model is not loaded and we did not kick off a new loading attempt, this means that we may be getting called
            // by a simulated pipeline
            logger.debug("[{}] not actively loading, eager loading without cache", modelId);
            provider.getTrainedModel(modelId, ActionListener.wrap(
                trainedModelConfig ->
                    modelActionListener.onResponse(new LocalModel(trainedModelConfig.getModelId(), trainedModelConfig.getDefinition())),
                modelActionListener::onFailure
            ));
        } else {
            logger.debug("[{}] is currently loading, added new listener to queue", modelId);
        }
    }

    /**
     * Returns true if the model is loaded and the listener has been given the cached model
     * Returns true if the model is CURRENTLY being loaded and the listener was added to be notified when it is loaded
     * Returns false if the model is not loaded or actively being loaded
     */
    private boolean loadModelIfNecessary(String modelId, ActionListener<Model> modelActionListener) {
        synchronized (loadingListeners) {
            MaybeModel cachedModel = loadedModels.get(modelId);
            if (cachedModel != null) {
                if (cachedModel.isSuccess()) {
                    modelActionListener.onResponse(cachedModel.getModel());
                    return true;
                }
                // If the loaded model entry is there but is not present, that means the previous load attempt ran into an issue
                // Attempt to load and cache the model if necessary
                if (loadingListeners.computeIfPresent(
                    modelId,
                    (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener)) == null) {
                    logger.debug("[{}] attempting to load and cache", modelId);
                    loadingListeners.put(modelId, addFluently(new ArrayDeque<>(), modelActionListener));
                    loadModel(modelId);
                }
                return true;
            }
            // if the cachedModel entry is null, but there are listeners present, that means it is being loaded
            return loadingListeners.computeIfPresent(modelId,
                (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener)) != null;
        }
    }

    private void loadModel(String modelId) {
        provider.getTrainedModel(modelId, ActionListener.wrap(
            trainedModelConfig -> {
                logger.debug("[{}] successfully loaded model", modelId);
                handleLoadSuccess(modelId, trainedModelConfig);
            },
            failure -> {
                logger.warn(new ParameterizedMessage("[{}] failed to load model", modelId), failure);
                handleLoadFailure(modelId, failure);
            }
        ));
    }

    private void handleLoadSuccess(String modelId, TrainedModelConfig trainedModelConfig) {
        Queue<ActionListener<Model>> listeners;
        Model loadedModel = new LocalModel(trainedModelConfig.getModelId(), trainedModelConfig.getDefinition());
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            // If there is no loadingListener that means the loading was canceled and the listener was already notified as such
            // Consequently, we should not store the retrieved model
            if (listeners != null) {
                loadedModels.put(modelId, MaybeModel.of(loadedModel));
            }
        }
        if (listeners != null) {
            for (ActionListener<Model> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
                listener.onResponse(loadedModel);
            }
        }
    }

    private void handleLoadFailure(String modelId, Exception failure) {
        Queue<ActionListener<Model>> listeners;
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            if (listeners != null) {
                // If we failed to load and there were listeners present, that means that this model is referenced by a processor
                // Add an empty entry here so that we can attempt to load and cache the model again when it is accessed again.
                loadedModels.computeIfAbsent(modelId, (key) -> MaybeModel.of(failure));
            }
        }
        if (listeners != null) {
            for (ActionListener<Model> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
                listener.onFailure(failure);
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.changedCustomMetaDataSet().contains(IngestMetadata.TYPE)) {
            ClusterState state = event.state();
            IngestMetadata currentIngestMetadata = state.metaData().custom(IngestMetadata.TYPE);
            Set<String> allReferencedModelKeys = getReferencedModelKeys(currentIngestMetadata);
            // The listeners still waiting for a model and we are canceling the load?
            List<Tuple<String, List<ActionListener<Model>>>> drainWithFailure = new ArrayList<>();
            synchronized (loadingListeners) {
                // If we had models still loading here but are no longer referenced
                // we should remove them from loadingListeners and alert the listeners
                for (String modelId : loadingListeners.keySet()) {
                    if (allReferencedModelKeys.contains(modelId) == false) {
                        drainWithFailure.add(Tuple.tuple(modelId, new ArrayList<>(loadingListeners.remove(modelId))));
                    }
                }

                // Remove all cached models that are not referenced by any processors
                loadedModels.keySet().retainAll(allReferencedModelKeys);

                // Remove all that are currently being loaded
                allReferencedModelKeys.removeAll(loadingListeners.keySet());

                // Remove all that are fully loaded, will attempt empty model loading again
                loadedModels.forEach((id, optionalModel) -> {
                    if (optionalModel.isSuccess()) {
                        allReferencedModelKeys.remove(id);
                    }
                });
                // Populate loadingListeners key so we know that we are currently loading the model
                for (String modelId : allReferencedModelKeys) {
                    loadingListeners.put(modelId, new ArrayDeque<>());
                }
            }
            for (Tuple<String, List<ActionListener<Model>>> modelAndListeners : drainWithFailure) {
                final String msg = new ParameterizedMessage(
                    "Cancelling load of model [{}] as it is no longer referenced by a pipeline",
                    modelAndListeners.v1()).getFormat();
                for (ActionListener<Model> listener : modelAndListeners.v2()) {
                    listener.onFailure(new ElasticsearchException(msg));
                }
            }
            loadModels(allReferencedModelKeys);
        }
    }

    private void loadModels(Set<String> modelIds) {
        if (modelIds.isEmpty()) {
            return;
        }
        // Execute this on a utility thread as when the callbacks occur we don't want them tying up the cluster listener thread pool
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            for (String modelId : modelIds) {
                this.loadModel(modelId);
            }
        });
    }

    private static <T> Queue<T> addFluently(Queue<T> queue, T object) {
        queue.add(object);
        return queue;
    }

    private static Set<String> getReferencedModelKeys(IngestMetadata ingestMetadata) {
        Set<String> allReferencedModelKeys = new HashSet<>();
        if (ingestMetadata != null) {
            ingestMetadata.getPipelines().forEach((pipelineId, pipelineConfiguration) -> {
                Object processors = pipelineConfiguration.getConfigAsMap().get("processors");
                if (processors instanceof List<?>) {
                    for(Object processor : (List<?>)processors) {
                        if (processor instanceof Map<?, ?>) {
                            Object processorConfig = ((Map<?, ?>)processor).get(InferenceProcessor.TYPE);
                            if (processorConfig instanceof Map<?, ?>) {
                                Object modelId = ((Map<?, ?>)processorConfig).get(InferenceProcessor.MODEL_ID);
                                if (modelId != null) {
                                    assert modelId instanceof String;
                                    // TODO also read model version
                                    allReferencedModelKeys.add(modelId.toString());
                                }
                            }
                        }
                    }
                }
            });
        }
        return allReferencedModelKeys;
    }

    private static class MaybeModel {

        private final Model model;
        private final Exception exception;

        static MaybeModel of(Model model) {
            return new MaybeModel(model, null);
        }

        static MaybeModel of(Exception exception) {
            return new MaybeModel(null, exception);
        }

        private MaybeModel(Model model, Exception exception) {
            this.model = model;
            this.exception = exception;
        }

        Model getModel() {
            return model;
        }

        Exception getException() {
            return exception;
        }

        boolean isSuccess() {
            return this.model != null;
        }

        boolean isFailure() {
            return this.exception != null;
        }

    }

}
