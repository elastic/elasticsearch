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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ModelLoadingService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ModelLoadingService.class);
    // TODO should these be ConcurrentHashMaps if all interactions are synchronized?
    private final ConcurrentHashMap<String, Optional<Model>> loadedModels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Queue<ActionListener<Model>>> loadingListeners = new ConcurrentHashMap<>();
    private final TrainedModelProvider provider;
    private final ThreadPool threadPool;

    public ModelLoadingService(TrainedModelProvider trainedModelProvider, ThreadPool threadPool, ClusterService clusterService) {
        this.provider = trainedModelProvider;
        this.threadPool = threadPool;
        clusterService.addListener(this);
    }

    public void getModel(String modelId, long modelVersion, ActionListener<Model> modelActionListener) {
        String key = modelKey(modelId, modelVersion);
        Optional<Model> cachedModel = loadedModels.get(key);
        if (cachedModel != null) {
            if (cachedModel.isPresent()) {
                modelActionListener.onResponse(cachedModel.get());
                return;
            }
        }
        if (loadModelIfNecessary(key, modelId, modelVersion, modelActionListener) == false) {
            // If we the model is not loaded and we did not kick off a new loading attempt, this means that we may be getting called
            // by a simulated pipeline
            logger.debug("[{}] version [{}] not actively loading, eager loading without cache", modelId, modelVersion);
            provider.getTrainedModel(modelId, modelVersion, ActionListener.wrap(
                trainedModelConfig ->
                    modelActionListener.onResponse(new LocalModel(trainedModelConfig.getModelId(), trainedModelConfig.getDefinition())),
                modelActionListener::onFailure
            ));
        } else {
            logger.debug("[{}] version [{}] is currently loading, added new listener to queue", modelId, modelVersion);
        }
    }

    /**
     * Returns true if the model is loaded and the listener has been given the cached model
     * Returns true if the model is CURRENTLY being loaded and the listener was added to be notified when it is loaded
     * Returns false if the model is not loaded or actively being loaded
     */
    private boolean loadModelIfNecessary(String key, String modelId, long modelVersion, ActionListener<Model> modelActionListener) {
        synchronized (loadingListeners) {
            Optional<Model> cachedModel = loadedModels.get(key);
            if (cachedModel != null) {
                if (cachedModel.isPresent()) {
                    modelActionListener.onResponse(cachedModel.get());
                    return true;
                }
                // If the loaded model entry is there but is not present, that means the previous load attempt ran into an issue
                // Attempt to load and cache the model if necessary
                if (loadingListeners.computeIfPresent(
                    key,
                    (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener)) == null) {
                    logger.debug("[{}] version [{}] attempting to load and cache", modelId, modelVersion);
                    loadingListeners.put(key, addFluently(new ArrayDeque<>(), modelActionListener));
                    loadModel(key, modelId, modelVersion);
                }
                return true;
            }
            // if the cachedModel entry is null, but there are listeners present, that means it is being loaded
            return loadingListeners.computeIfPresent(key,
                (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener)) != null;
        }
    }

    private void loadModel(String modelKey, String modelId, long modelVersion) {
        provider.getTrainedModel(modelId, modelVersion, ActionListener.wrap(
            trainedModelConfig -> {
                logger.debug("[{}] successfully loaded model", modelKey);
                handleLoadSuccess(modelKey, trainedModelConfig);
            },
            failure -> {
                logger.warn(new ParameterizedMessage("[{}] failed to load model", modelKey), failure);
                handleLoadFailure(modelKey, failure);
            }
        ));
    }

    private void handleLoadSuccess(String modelKey, TrainedModelConfig trainedModelConfig) {
        Queue<ActionListener<Model>> listeners;
        Model loadedModel = new LocalModel(trainedModelConfig.getModelId(), trainedModelConfig.getDefinition());
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelKey);
            // If there is no loadingListener that means the loading was canceled and the listener was already notified as such
            // Consequently, we should not store the retrieved model
            if (listeners != null) {
                loadedModels.put(modelKey, Optional.of(loadedModel));
            }
        }
        if (listeners != null) {
            for(ActionListener<Model> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
                listener.onResponse(loadedModel);
            }
        }
    }

    private void handleLoadFailure(String modelKey, Exception failure) {
        Queue<ActionListener<Model>> listeners;
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelKey);
            if (listeners != null) {
                // If we failed to load and there were listeners present, that means that this model is referenced by a processor
                // Add an empty entry here so that we can attempt to load and cache the model again when it is accessed again.
                loadedModels.computeIfAbsent(modelKey, (key) -> Optional.empty());
            }
        }
        if (listeners != null) {
            for(ActionListener<Model> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
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
            Queue<ActionListener<Model>> drainWithFailure = new ArrayDeque<>();
            synchronized (loadingListeners) {
                // If we had models still loading here but are no longer referenced
                // we should remove them from loadingListeners and alert the listeners
                Iterator<String> keyIterator = loadingListeners.keys().asIterator();
                while(keyIterator.hasNext()) {
                    String modelKey = keyIterator.next();
                    if (allReferencedModelKeys.contains(modelKey) == false) {
                        drainWithFailure.addAll(loadingListeners.remove(modelKey));
                    }
                }

                // Remove all cached models that are not referenced by any processors
                loadedModels.keySet().retainAll(allReferencedModelKeys);

                // After removing the unreferenced models, now we need to know what referenced models should be loaded

                // Remove all that are currently being loaded
                allReferencedModelKeys.removeAll(loadingListeners.keySet());

                // Remove all that are fully loaded, will attempt empty model loading again
                loadedModels.forEach((id, optionalModel) -> {
                    if(optionalModel.isPresent()) {
                        allReferencedModelKeys.remove(id);
                    }
                });
                // Populate loadingListeners key so we know that we are currently loading the model
                for(String modelId : allReferencedModelKeys) {
                    loadingListeners.put(modelId, new ArrayDeque<>());
                }
            }
            for(ActionListener<Model> listener = drainWithFailure.poll(); listener != null; listener = drainWithFailure.poll()) {
                listener.onFailure(
                    new ElasticsearchException("Cancelling model load and inference as it is no longer referenced by a pipeline"));
            }
            loadModels(allReferencedModelKeys);
        }
    }

    private void loadModels(Set<String> modelKeys) {
        if (modelKeys.isEmpty()) {
            return;
        }
        // Execute this on a utility thread as when the callbacks occur we don't want them tying up the cluster listener thread pool
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            for(String modelKey : modelKeys) {
                Tuple<String, Long> modelIdAndVersion = splitModelKey(modelKey);
                this.loadModel(modelKey, modelIdAndVersion.v1(), modelIdAndVersion.v2());
            }
        });
    }

    private static <T> Queue<T> addFluently(Queue<T> queue, T object) {
        queue.add(object);
        return queue;
    }

    private static String modelKey(String modelId, long modelVersion) {
        return modelId + "_" + modelVersion;
    }

    private static Tuple<String, Long> splitModelKey(String modelKey) {
        int delim = modelKey.lastIndexOf('_');
        String modelId = modelKey.substring(0, delim);
        Long version = Long.valueOf(modelKey.substring(delim + 1));
        return Tuple.tuple(modelId, version);
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
                                    allReferencedModelKeys.add(modelKey(modelId.toString(), 0));
                                }
                            }
                        }
                    }
                }
            });
        }
        return allReferencedModelKeys;
    }

}
