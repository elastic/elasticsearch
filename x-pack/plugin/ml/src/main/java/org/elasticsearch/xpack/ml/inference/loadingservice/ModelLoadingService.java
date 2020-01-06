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
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This is a thread safe model loading service.
 *
 * It will cache local models that are referenced by processors in memory (as long as it is instantiated on an ingest node).
 *
 * If more than one processor references the same model, that model will only be cached once.
 */
public class ModelLoadingService implements ClusterStateListener {

    /**
     * The maximum size of the local model cache here in the loading service
     *
     * Once the limit is reached, LRU models are evicted in favor of new models
     */
    public static final Setting<ByteSizeValue> INFERENCE_MODEL_CACHE_SIZE =
        Setting.memorySizeSetting("xpack.ml.inference_model.cache_size",
            "40%",
            Setting.Property.NodeScope);

    /**
     * How long should a model stay in the cache since its last access
     *
     * If nothing references a model via getModel for this configured timeValue, it will be evicted.
     *
     * Specifically, in the ingest scenario, a processor will call getModel whenever it needs to run inference. So, if a processor is not
     * executed for an extended period of time, the model will be evicted and will have to be loaded again when getModel is called.
     *
     */
    public static final Setting<TimeValue> INFERENCE_MODEL_CACHE_TTL =
        Setting.timeSetting("xpack.ml.inference_model.time_to_live",
            new TimeValue(5, TimeUnit.MINUTES),
            new TimeValue(1, TimeUnit.MILLISECONDS),
            Setting.Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(ModelLoadingService.class);
    private final Cache<String, LocalModel> localModelCache;
    private final Set<String> referencedModels = new HashSet<>();
    private final Map<String, Queue<ActionListener<Model>>> loadingListeners = new HashMap<>();
    private final TrainedModelProvider provider;
    private final Set<String> shouldNotAudit;
    private final ThreadPool threadPool;
    private final InferenceAuditor auditor;
    private final ByteSizeValue maxCacheSize;
    private final NamedXContentRegistry namedXContentRegistry;

    public ModelLoadingService(TrainedModelProvider trainedModelProvider,
                               InferenceAuditor auditor,
                               ThreadPool threadPool,
                               ClusterService clusterService,
                               NamedXContentRegistry namedXContentRegistry,
                               Settings settings) {
        this.provider = trainedModelProvider;
        this.threadPool = threadPool;
        this.maxCacheSize = INFERENCE_MODEL_CACHE_SIZE.get(settings);
        this.auditor = auditor;
        this.shouldNotAudit = new HashSet<>();
        this.namedXContentRegistry = namedXContentRegistry;
        this.localModelCache = CacheBuilder.<String, LocalModel>builder()
            .setMaximumWeight(this.maxCacheSize.getBytes())
            .weigher((id, localModel) -> localModel.ramBytesUsed())
            .removalListener(this::cacheEvictionListener)
            .setExpireAfterAccess(INFERENCE_MODEL_CACHE_TTL.get(settings))
            .build();
        clusterService.addListener(this);
    }

    /**
     * Gets the model referenced by `modelId` and responds to the listener.
     *
     * This method first checks the local LRU cache for the model. If it is present, it is returned from cache.
     *
     * If it is not present, one of the following occurs:
     *
     *  - If the model is referenced by a pipeline and is currently being loaded, the `modelActionListener`
     *    is added to the list of listeners to be alerted when the model is fully loaded.
     *  - If the model is referenced by a pipeline and is currently NOT being loaded, a new load attempt is made and the resulting
     *    model will attempt to be cached for future reference
     *  - If the models is NOT referenced by a pipeline, the model is simply loaded from the index and given to the listener.
     *    It is not cached.
     *
     * @param modelId the model to get
     * @param modelActionListener the listener to alert when the model has been retrieved.
     */
    public void getModel(String modelId, ActionListener<Model> modelActionListener) {
        LocalModel cachedModel = localModelCache.get(modelId);
        if (cachedModel != null) {
            modelActionListener.onResponse(cachedModel);
            logger.trace("[{}] loaded from cache", modelId);
            return;
        }
        if (loadModelIfNecessary(modelId, modelActionListener) == false) {
            // If we the model is not loaded and we did not kick off a new loading attempt, this means that we may be getting called
            // by a simulated pipeline
            logger.trace("[{}] not actively loading, eager loading without cache", modelId);
            provider.getTrainedModel(modelId, true, ActionListener.wrap(
                trainedModelConfig ->
                    modelActionListener.onResponse(new LocalModel(
                        trainedModelConfig.getModelId(),
                        trainedModelConfig.ensureParsedDefinition(namedXContentRegistry).getModelDefinition(),
                        trainedModelConfig.getInput())),
                modelActionListener::onFailure
            ));
        } else {
            logger.trace("[{}] is loading or loaded, added new listener to queue", modelId);
        }
    }

    /**
     * Returns true if the model is loaded and the listener has been given the cached model
     * Returns true if the model is CURRENTLY being loaded and the listener was added to be notified when it is loaded
     * Returns false if the model is not loaded or actively being loaded
     */
    private boolean loadModelIfNecessary(String modelId, ActionListener<Model> modelActionListener) {
        synchronized (loadingListeners) {
            Model cachedModel = localModelCache.get(modelId);
            if (cachedModel != null) {
                modelActionListener.onResponse(cachedModel);
                return true;
            }
            // It is referenced by a pipeline, but the cache does not contain it
            if (referencedModels.contains(modelId)) {
                // If the loaded model is referenced there but is not present,
                // that means the previous load attempt failed or the model has been evicted
                // Attempt to load and cache the model if necessary
                if (loadingListeners.computeIfPresent(
                    modelId,
                    (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener)) == null) {
                    logger.trace("[{}] attempting to load and cache", modelId);
                    loadingListeners.put(modelId, addFluently(new ArrayDeque<>(), modelActionListener));
                    loadModel(modelId);
                }
                return true;
            }
            // if the cachedModel entry is null, but there are listeners present, that means it is being loaded
            return loadingListeners.computeIfPresent(modelId,
                (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener)) != null;
        } // synchronized (loadingListeners)
    }

    private void loadModel(String modelId) {
        provider.getTrainedModel(modelId, true, ActionListener.wrap(
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

    private void handleLoadSuccess(String modelId, TrainedModelConfig trainedModelConfig) throws IOException {
        Queue<ActionListener<Model>> listeners;
        LocalModel loadedModel = new LocalModel(
            trainedModelConfig.getModelId(),
            trainedModelConfig.ensureParsedDefinition(namedXContentRegistry).getModelDefinition(),
            trainedModelConfig.getInput());
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            // If there is no loadingListener that means the loading was canceled and the listener was already notified as such
            // Consequently, we should not store the retrieved model
            if (listeners == null) {
                return;
            }
            localModelCache.put(modelId, loadedModel);
            shouldNotAudit.remove(modelId);
        } // synchronized (loadingListeners)
        for (ActionListener<Model> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
            listener.onResponse(loadedModel);
        }
    }

    private void handleLoadFailure(String modelId, Exception failure) {
        Queue<ActionListener<Model>> listeners;
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            if (listeners == null) {
                return;
            }
        } // synchronized (loadingListeners)
        // If we failed to load and there were listeners present, that means that this model is referenced by a processor
        // Alert the listeners to the failure
        for (ActionListener<Model> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
            listener.onFailure(failure);
        }
    }

    private void cacheEvictionListener(RemovalNotification<String, LocalModel> notification) {
        if (notification.getRemovalReason() == RemovalNotification.RemovalReason.EVICTED) {
            String msg = new ParameterizedMessage(
                "model cache entry evicted." +
                    "current cache [{}] current max [{}] model size [{}]. " +
                    "If this is undesired, consider updating setting [{}] or [{}].",
                new ByteSizeValue(localModelCache.weight()).getStringRep(),
                maxCacheSize.getStringRep(),
                new ByteSizeValue(notification.getValue().ramBytesUsed()).getStringRep(),
                INFERENCE_MODEL_CACHE_SIZE.getKey(),
                INFERENCE_MODEL_CACHE_TTL.getKey()).getFormattedMessage();
            auditIfNecessary(notification.getKey(), msg);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // If ingest data has not changed or if the current node is not an ingest node, don't bother caching models
        if (event.changedCustomMetaDataSet().contains(IngestMetadata.TYPE) == false ||
            event.state().nodes().getLocalNode().isIngestNode() == false) {
            return;
        }

        ClusterState state = event.state();
        IngestMetadata currentIngestMetadata = state.metaData().custom(IngestMetadata.TYPE);
        Set<String> allReferencedModelKeys = getReferencedModelKeys(currentIngestMetadata);
        if (allReferencedModelKeys.equals(referencedModels)) {
            return;
        }
        // The listeners still waiting for a model and we are canceling the load?
        List<Tuple<String, List<ActionListener<Model>>>> drainWithFailure = new ArrayList<>();
        Set<String> referencedModelsBeforeClusterState = null;
        Set<String> loadingModelBeforeClusterState = null;
        Set<String> removedModels = null;
        synchronized (loadingListeners) {
            referencedModelsBeforeClusterState = new HashSet<>(referencedModels);
            if (logger.isTraceEnabled()) {
                loadingModelBeforeClusterState = new HashSet<>(loadingListeners.keySet());
            }
            // If we had models still loading here but are no longer referenced
            // we should remove them from loadingListeners and alert the listeners
            for (String modelId : loadingListeners.keySet()) {
                if (allReferencedModelKeys.contains(modelId) == false) {
                    drainWithFailure.add(Tuple.tuple(modelId, new ArrayList<>(loadingListeners.remove(modelId))));
                }
            }
            removedModels = Sets.difference(referencedModelsBeforeClusterState, allReferencedModelKeys);

            // Remove all cached models that are not referenced by any processors
            removedModels.forEach(localModelCache::invalidate);
            // Remove the models that are no longer referenced
            referencedModels.removeAll(removedModels);
            shouldNotAudit.removeAll(removedModels);

            // Remove all that are still referenced, i.e. the intersection of allReferencedModelKeys and referencedModels
            allReferencedModelKeys.removeAll(referencedModels);
            referencedModels.addAll(allReferencedModelKeys);

            // Populate loadingListeners key so we know that we are currently loading the model
            for (String modelId : allReferencedModelKeys) {
                loadingListeners.put(modelId, new ArrayDeque<>());
            }
        } // synchronized (loadingListeners)
        if (logger.isTraceEnabled()) {
            if (loadingListeners.keySet().equals(loadingModelBeforeClusterState) == false) {
                logger.trace("cluster state event changed loading models: before {} after {}", loadingModelBeforeClusterState,
                    loadingListeners.keySet());
            }
            if (referencedModels.equals(referencedModelsBeforeClusterState) == false) {
                logger.trace("cluster state event changed referenced models: before {} after {}", referencedModelsBeforeClusterState,
                    referencedModels);
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
        removedModels.forEach(this::auditUnreferencedModel);
        loadModels(allReferencedModelKeys);
    }

    private void auditIfNecessary(String modelId, String msg) {
        if (shouldNotAudit.contains(modelId)) {
            logger.trace("[{}] {}", modelId, msg);
            return;
        }
        auditor.warning(modelId, msg);
        shouldNotAudit.add(modelId);
        logger.warn("[{}] {}", modelId, msg);
    }

    private void loadModels(Set<String> modelIds) {
        if (modelIds.isEmpty()) {
            return;
        }
        // Execute this on a utility thread as when the callbacks occur we don't want them tying up the cluster listener thread pool
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            for (String modelId : modelIds) {
                auditNewReferencedModel(modelId);
                this.loadModel(modelId);
            }
        });
    }

    private void auditNewReferencedModel(String modelId) {
        auditor.info(modelId, "referenced by ingest processors. Attempting to load model into cache");
    }

    private void auditUnreferencedModel(String modelId) {
        auditor.info(modelId, "no longer referenced by any processors");
    }

    private static <T> Queue<T> addFluently(Queue<T> queue, T object) {
        queue.add(object);
        return queue;
    }

    private static Set<String> getReferencedModelKeys(IngestMetadata ingestMetadata) {
        Set<String> allReferencedModelKeys = new HashSet<>();
        if (ingestMetadata == null) {
            return allReferencedModelKeys;
        }
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
                                allReferencedModelKeys.add(modelId.toString());
                            }
                        }
                    }
                }
            }
        });
        return allReferencedModelKeys;
    }

}
