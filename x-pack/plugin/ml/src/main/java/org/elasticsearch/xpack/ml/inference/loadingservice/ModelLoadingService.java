/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class ModelLoadingService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ModelLoadingService.class);
    private final ConcurrentHashMap<String, Model> loadedModels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Queue<ActionListener<Model>>> loadingListeners = new ConcurrentHashMap<>();
    private final TrainedModelProvider provider;

    public ModelLoadingService(TrainedModelProvider trainedModelProvider) {
        this.provider = trainedModelProvider;
    }

    public void getModelAndCache(String modelId, long modelVersion, ActionListener<Model> modelActionListener) {
        String key = modelKey(modelId, modelVersion);
        Model cachedModel = loadedModels.get(key);
        if (cachedModel != null) {
            modelActionListener.onResponse(cachedModel);
            return;
        }
        SetOnce<Boolean> newLoad = new SetOnce<>();
        synchronized (loadingListeners) {
            cachedModel = loadedModels.get(key);
            if (cachedModel != null) {
                modelActionListener.onResponse(cachedModel);
                return;
            }
            loadingListeners.compute(key, (modelKey, listeners) -> {
                if (listeners == null) {
                    newLoad.set(true);
                    Deque<ActionListener<Model>> listenerDeque = new ArrayDeque<>();
                    listenerDeque.addLast(modelActionListener);
                    return listenerDeque;
                }
                newLoad.set(false);
                listeners.add(modelActionListener);
                return listeners;
            });
        }
        if (newLoad.get()) {
            // TODO support loading other types of models?
            loadModel(key, modelId, modelVersion);
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
        Model loadedModel = new LocalModel(trainedModelConfig.getDefinition());
        synchronized (loadingListeners) {
            loadedModels.put(modelKey, loadedModel);
            listeners = loadingListeners.remove(modelKey);
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
            // TODO do we want to cache the failure?
            listeners = loadingListeners.remove(modelKey);
        }
        if (listeners != null) {
            for(ActionListener<Model> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
                listener.onFailure(failure);
            }
        }
    }

    private String modelKey(String modelId, long modelVersion) {
        return modelId + "_" + modelVersion;
    }

    private Tuple<String, Long> splitModelKey(String modelKey) {
        int delimIdx = modelKey.lastIndexOf("_");
        String modelId = modelKey.substring(0, delimIdx);
        Long modelVersion = Long.valueOf(modelKey.substring(delimIdx + 1));
        return Tuple.tuple(modelId, modelVersion);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO
    }
}
