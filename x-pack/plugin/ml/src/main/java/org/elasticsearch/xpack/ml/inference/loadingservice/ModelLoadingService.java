/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.MessageSupplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This is a thread safe model loading service with LRU cache.
 * Cache entries have a TTL before they are evicted.
 *
 * In the case of a pipeline processor requesting the model if the processor is in simulate
 * mode the model is not cached. All other uses will cache the model
 *
 * If more than one processor references the same model, that model will only be cached once.
 *
 * LocalModels are created with a reference count of 1 accounting for the reference the
 * cache holds. When models are evicted from the cache the reference count is decremented.
 * The {@code getModelForX} methods automatically increment the model's reference count
 * it is up to the consumer to call {@link LocalModel#release()} when the model is no
 * longer used.
 */
public class ModelLoadingService implements ClusterStateListener {

    /**
     * The maximum size of the local model cache here in the loading service
     * <p>
     * Once the limit is reached, LRU models are evicted in favor of new models
     */
    public static final Setting<ByteSizeValue> INFERENCE_MODEL_CACHE_SIZE =
        Setting.memorySizeSetting("xpack.ml.inference_model.cache_size",
            "40%",
            Setting.Property.NodeScope);

    /**
     * How long should a model stay in the cache since its last access
     * <p>
     * If nothing references a model via getModel for this configured timeValue, it will be evicted.
     * <p>
     * Specifically, in the ingest scenario, a processor will call getModel whenever it needs to run inference. So, if a processor is not
     * executed for an extended period of time, the model will be evicted and will have to be loaded again when getModel is called.
     */
    public static final Setting<TimeValue> INFERENCE_MODEL_CACHE_TTL =
        Setting.timeSetting("xpack.ml.inference_model.time_to_live",
            new TimeValue(5, TimeUnit.MINUTES),
            new TimeValue(1, TimeUnit.MILLISECONDS),
            Setting.Property.NodeScope);

    // The feature requesting the model
    public enum Consumer {
        PIPELINE, SEARCH
    }

    private static class ModelAndConsumer {
        private final LocalModel model;
        private final EnumSet<Consumer> consumers;

        private ModelAndConsumer(LocalModel model, Consumer consumer) {
            this.model = model;
            this.consumers = EnumSet.of(consumer);
        }
    }


    private static final Logger logger = LogManager.getLogger(ModelLoadingService.class);
    private final TrainedModelStatsService modelStatsService;
    private final Cache<String, ModelAndConsumer> localModelCache;
    private final Set<String> referencedModels = new HashSet<>();
    private final Map<String, Queue<ActionListener<LocalModel>>> loadingListeners = new HashMap<>();
    private final TrainedModelProvider provider;
    private final Set<String> shouldNotAudit;
    private final ThreadPool threadPool;
    private final InferenceAuditor auditor;
    private final ByteSizeValue maxCacheSize;
    private final String localNode;
    private final CircuitBreaker trainedModelCircuitBreaker;

    public ModelLoadingService(TrainedModelProvider trainedModelProvider,
                               InferenceAuditor auditor,
                               ThreadPool threadPool,
                               ClusterService clusterService,
                               TrainedModelStatsService modelStatsService,
                               Settings settings,
                               String localNode,
                               CircuitBreaker trainedModelCircuitBreaker) {
        this.provider = trainedModelProvider;
        this.threadPool = threadPool;
        this.maxCacheSize = INFERENCE_MODEL_CACHE_SIZE.get(settings);
        this.auditor = auditor;
        this.modelStatsService = modelStatsService;
        this.shouldNotAudit = new HashSet<>();
        this.localModelCache = CacheBuilder.<String, ModelAndConsumer>builder()
            .setMaximumWeight(this.maxCacheSize.getBytes())
            .weigher((id, modelAndConsumer) -> modelAndConsumer.model.ramBytesUsed())
            // explicit declaration of the listener lambda necessary for Eclipse IDE 4.14
            .removalListener(notification -> cacheEvictionListener(notification))
            .setExpireAfterAccess(INFERENCE_MODEL_CACHE_TTL.get(settings))
            .build();
        clusterService.addListener(this);
        this.localNode = localNode;
        this.trainedModelCircuitBreaker = ExceptionsHelper.requireNonNull(trainedModelCircuitBreaker, "trainedModelCircuitBreaker");
    }

    boolean isModelCached(String modelId) {
        return localModelCache.get(modelId) != null;
    }

    /**
     * Load the model for use by an ingest pipeline. The model will not be cached if there is no
     * ingest pipeline referencing it i.e. it is used in simulate mode
     *
     * @param modelId  the model to get
     * @param modelActionListener the listener to alert when the model has been retrieved
     */
    public void getModelForPipeline(String modelId, ActionListener<LocalModel> modelActionListener) {
        getModel(modelId, Consumer.PIPELINE, modelActionListener);
    }

    /**
     * Load the model for use by at search. Models requested by search are always cached.
     *
     * @param modelId  the model to get
     * @param modelActionListener the listener to alert when the model has been retrieved
     */
    public void getModelForSearch(String modelId, ActionListener<LocalModel> modelActionListener) {
        getModel(modelId, Consumer.SEARCH, modelActionListener);
    }

    /**
     * Gets the model referenced by `modelId` and responds to the listener.
     * <p>
     * This method first checks the local LRU cache for the model. If it is present, it is returned from cache.
     * <p>
     * In the case of search if the model is not present one of the following occurs:
     * - If it is currently being loaded the `modelActionListener`
     * is added to the list of listeners to be alerted when the model is fully loaded.
     * - Otherwise the model is loaded and cached
     *
     * In the case of an ingest processor if it is not present, one of the following occurs:
     * <p>
     * - If the model is referenced by a pipeline and is currently being loaded, the `modelActionListener`
     * is added to the list of listeners to be alerted when the model is fully loaded.
     * - If the model is referenced by a pipeline and is currently NOT being loaded, a new load attempt is made and the resulting
     * model will attempt to be cached for future reference
     * - If the models is NOT referenced by a pipeline, the model is simply loaded from the index and given to the listener.
     * It is not cached.
     *
     * The main difference being that models for search are always cached whereas pipeline models
     * are only cached if they are referenced by an ingest pipeline
     *
     * @param modelId             the model to get
     * @param consumer            which feature is requesting the model
     * @param modelActionListener the listener to alert when the model has been retrieved.
     */
    private void getModel(String modelId, Consumer consumer, ActionListener<LocalModel> modelActionListener) {
        ModelAndConsumer cachedModel = localModelCache.get(modelId);
        if (cachedModel != null) {
            cachedModel.consumers.add(consumer);
            try {
                cachedModel.model.acquire();
            } catch (CircuitBreakingException e) {
                modelActionListener.onFailure(e);
                return;
            }
            modelActionListener.onResponse(cachedModel.model);
            logger.trace(() -> new ParameterizedMessage("[{}] loaded from cache", modelId));
            return;
        }

        if (loadModelIfNecessary(modelId, consumer, modelActionListener)) {
            logger.trace(() -> new ParameterizedMessage("[{}] is loading or loaded, added new listener to queue", modelId));
        }
    }

    /**
     * If the model is cached it is returned directly to the listener
     * else if the model is CURRENTLY being loaded the listener is added to be notified when it is loaded
     * else the model load is initiated.
     *
     * @param modelId The model to get
     * @param consumer The model consumer
     * @param modelActionListener The listener
     * @return If the model is cached or currently being loaded true is returned. If a new load is started
     * false is returned to indicate a new load event
     */
    private boolean loadModelIfNecessary(String modelId, Consumer consumer, ActionListener<LocalModel> modelActionListener) {
        synchronized (loadingListeners) {
            ModelAndConsumer cachedModel = localModelCache.get(modelId);
            if (cachedModel != null) {
                cachedModel.consumers.add(consumer);
                try {
                    cachedModel.model.acquire();
                } catch (CircuitBreakingException e) {
                    modelActionListener.onFailure(e);
                    return true;
                }
                modelActionListener.onResponse(cachedModel.model);
                return true;
            }

            // Add the listener to the queue if the model is loading
            Queue<ActionListener<LocalModel>> listeners = loadingListeners.computeIfPresent(modelId,
                (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener));

            // The cachedModel entry is null, but there are listeners present, that means it is being loaded
            if (listeners != null) {
                return true;
            }

            if (Consumer.PIPELINE == consumer && referencedModels.contains(modelId) == false) {
                // The model is requested by a pipeline but not referenced by any ingest pipelines.
                // This means it is a simulate call and the model should not be cached
                loadWithoutCaching(modelId, modelActionListener);
            } else {
                logger.trace(() -> new ParameterizedMessage("[{}] attempting to load and cache", modelId));
                loadingListeners.put(modelId, addFluently(new ArrayDeque<>(), modelActionListener));
                loadModel(modelId, consumer);
            }

            return false;
        } // synchronized (loadingListeners)
    }

    private void loadModel(String modelId, Consumer consumer) {
        provider.getTrainedModel(modelId, false, ActionListener.wrap(
            trainedModelConfig -> {
                trainedModelCircuitBreaker.addEstimateBytesAndMaybeBreak(trainedModelConfig.getEstimatedHeapMemory(), modelId);
                provider.getTrainedModelForInference(modelId, ActionListener.wrap(
                    inferenceDefinition -> {
                        try {
                            // Since we have used the previously stored estimate to help guard against OOM we need
                            // to adjust the memory so that the memory this model uses in the circuit breaker
                            // is the most accurate estimate.
                            updateCircuitBreakerEstimate(modelId, inferenceDefinition, trainedModelConfig);
                        } catch (CircuitBreakingException ex) {
                            handleLoadFailure(modelId, ex);
                            return;
                        }

                        handleLoadSuccess(modelId, consumer, trainedModelConfig, inferenceDefinition);
                    },
                    failure -> {
                        // We failed to get the definition, remove the initial estimation.
                        trainedModelCircuitBreaker.addWithoutBreaking(-trainedModelConfig.getEstimatedHeapMemory());
                        logger.warn(new ParameterizedMessage("[{}] failed to load model definition", modelId), failure);
                        handleLoadFailure(modelId, failure);
                    }
                ));
            },
            failure -> {
                logger.warn(new ParameterizedMessage("[{}] failed to load model configuration", modelId), failure);
                handleLoadFailure(modelId, failure);
            }
        ));
    }

    private void loadWithoutCaching(String modelId, ActionListener<LocalModel> modelActionListener) {
        // If we the model is not loaded and we did not kick off a new loading attempt, this means that we may be getting called
        // by a simulated pipeline
        logger.trace(() -> new ParameterizedMessage("[{}] not actively loading, eager loading without cache", modelId));
        provider.getTrainedModel(modelId, false, ActionListener.wrap(
            trainedModelConfig -> {
                // Verify we can pull the model into memory without causing OOM
                trainedModelCircuitBreaker.addEstimateBytesAndMaybeBreak(trainedModelConfig.getEstimatedHeapMemory(), modelId);
                provider.getTrainedModelForInference(modelId, ActionListener.wrap(
                    inferenceDefinition -> {
                        InferenceConfig inferenceConfig = trainedModelConfig.getInferenceConfig() == null ?
                            inferenceConfigFromTargetType(inferenceDefinition.getTargetType()) :
                            trainedModelConfig.getInferenceConfig();

                        try {
                            updateCircuitBreakerEstimate(modelId, inferenceDefinition, trainedModelConfig);
                        } catch (CircuitBreakingException ex) {
                            modelActionListener.onFailure(ex);
                            return;
                        }

                        modelActionListener.onResponse(new LocalModel(
                            trainedModelConfig.getModelId(),
                            localNode,
                            inferenceDefinition,
                            trainedModelConfig.getInput(),
                            trainedModelConfig.getDefaultFieldMap(),
                            inferenceConfig,
                            trainedModelConfig.getLicenseLevel(),
                            modelStatsService,
                            trainedModelCircuitBreaker));
                    },
                    // Failure getting the definition, remove the initial estimation value
                    e -> {
                        trainedModelCircuitBreaker.addWithoutBreaking(-trainedModelConfig.getEstimatedHeapMemory());
                        modelActionListener.onFailure(e);
                    }
                ));
            },
            modelActionListener::onFailure
        ));
    }

    private void updateCircuitBreakerEstimate(String modelId, InferenceDefinition inferenceDefinition,
                                              TrainedModelConfig trainedModelConfig) throws CircuitBreakingException {
        long estimateDiff = inferenceDefinition.ramBytesUsed() - trainedModelConfig.getEstimatedHeapMemory();
        if (estimateDiff < 0) {
            trainedModelCircuitBreaker.addWithoutBreaking(estimateDiff);
        } else if (estimateDiff > 0) { // rare case where estimate is now HIGHER
            try {
                trainedModelCircuitBreaker.addEstimateBytesAndMaybeBreak(estimateDiff, modelId);
            } catch (CircuitBreakingException ex) { // if we failed here, we should remove the initial estimate as well
                trainedModelCircuitBreaker.addWithoutBreaking(-trainedModelConfig.getEstimatedHeapMemory());
                throw ex;
            }
        }
    }

    private void handleLoadSuccess(String modelId,
                                   Consumer consumer,
                                   TrainedModelConfig trainedModelConfig,
                                   InferenceDefinition inferenceDefinition) {
        Queue<ActionListener<LocalModel>> listeners;
        InferenceConfig inferenceConfig = trainedModelConfig.getInferenceConfig() == null ?
            inferenceConfigFromTargetType(inferenceDefinition.getTargetType()) :
            trainedModelConfig.getInferenceConfig();
        LocalModel loadedModel = new LocalModel(
            trainedModelConfig.getModelId(),
            localNode,
            inferenceDefinition,
            trainedModelConfig.getInput(),
            trainedModelConfig.getDefaultFieldMap(),
            inferenceConfig,
            trainedModelConfig.getLicenseLevel(),
            modelStatsService,
            trainedModelCircuitBreaker);
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            // If there is no loadingListener that means the loading was canceled and the listener was already notified as such
            // Consequently, we should not store the retrieved model
            if (listeners == null) {
                loadedModel.release();
                return;
            }

            // temporarily increase the reference count before adding to
            // the cache in case the model is evicted before the listeners
            // are called in which case acquire() would throw.
            loadedModel.acquire();
            localModelCache.put(modelId, new ModelAndConsumer(loadedModel, consumer));
            shouldNotAudit.remove(modelId);
        } // synchronized (loadingListeners)
        for (ActionListener<LocalModel> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
            loadedModel.acquire();
            listener.onResponse(loadedModel);
        }
        // account for the acquire in the synchronized block above
        loadedModel.release();
    }

    private void handleLoadFailure(String modelId, Exception failure) {
        Queue<ActionListener<LocalModel>> listeners;
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            if (listeners == null) {
                return;
            }
        } // synchronized (loadingListeners)
        // If we failed to load and there were listeners present, that means that this model is referenced by a processor
        // Alert the listeners to the failure
        for (ActionListener<LocalModel> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
            listener.onFailure(failure);
        }
    }

    private void cacheEvictionListener(RemovalNotification<String, ModelAndConsumer> notification) {
        try {
            if (notification.getRemovalReason() == RemovalNotification.RemovalReason.EVICTED) {
                MessageSupplier msg = () -> new ParameterizedMessage(
                    "model cache entry evicted." +
                        "current cache [{}] current max [{}] model size [{}]. " +
                        "If this is undesired, consider updating setting [{}] or [{}].",
                    new ByteSizeValue(localModelCache.weight()).getStringRep(),
                    maxCacheSize.getStringRep(),
                    new ByteSizeValue(notification.getValue().model.ramBytesUsed()).getStringRep(),
                    INFERENCE_MODEL_CACHE_SIZE.getKey(),
                    INFERENCE_MODEL_CACHE_TTL.getKey());
                auditIfNecessary(notification.getKey(), msg);
            }
            // If the model is no longer referenced, flush the stats to persist as soon as possible
            notification.getValue().model.persistStats(referencedModels.contains(notification.getKey()) == false);
        } finally {
            notification.getValue().model.release();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // If ingest data has not changed or if the current node is not an ingest node, don't bother caching models
        if (event.changedCustomMetadataSet().contains(IngestMetadata.TYPE) == false ||
            event.state().nodes().getLocalNode().isIngestNode() == false) {
            return;
        }

        ClusterState state = event.state();
        IngestMetadata currentIngestMetadata = state.metadata().custom(IngestMetadata.TYPE);
        Set<String> allReferencedModelKeys = getReferencedModelKeys(currentIngestMetadata);
        if (allReferencedModelKeys.equals(referencedModels)) {
            return;
        }
        // The listeners still waiting for a model and we are canceling the load?
        List<Tuple<String, List<ActionListener<LocalModel>>>> drainWithFailure = new ArrayList<>();
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
            // and are not used in search
            removedModels.forEach(modelId -> {
                ModelAndConsumer modelAndConsumer = localModelCache.get(modelId);
                if (modelAndConsumer != null && modelAndConsumer.consumers.contains(Consumer.SEARCH) == false) {
                    localModelCache.invalidate(modelId);
                }
            });
            // Remove the models that are no longer referenced
            referencedModels.removeAll(removedModels);
            shouldNotAudit.removeAll(removedModels);

            // Remove all that are still referenced, i.e. the intersection of allReferencedModelKeys and referencedModels
            allReferencedModelKeys.removeAll(referencedModels);
            referencedModels.addAll(allReferencedModelKeys);

            // Populate loadingListeners key so we know that we are currently loading the model
            for (String modelId : allReferencedModelKeys) {
                loadingListeners.computeIfAbsent(modelId, (s) -> new ArrayDeque<>());
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
        for (Tuple<String, List<ActionListener<LocalModel>>> modelAndListeners : drainWithFailure) {
            final String msg = new ParameterizedMessage(
                "Cancelling load of model [{}] as it is no longer referenced by a pipeline",
                modelAndListeners.v1()).getFormat();
            for (ActionListener<LocalModel> listener : modelAndListeners.v2()) {
                listener.onFailure(new ElasticsearchException(msg));
            }
        }
        removedModels.forEach(this::auditUnreferencedModel);
        loadModelsForPipeline(allReferencedModelKeys);
    }

    private void auditIfNecessary(String modelId, MessageSupplier msg) {
        if (shouldNotAudit.contains(modelId)) {
            logger.trace(() -> new ParameterizedMessage("[{}] {}", modelId, msg.get().getFormattedMessage()));
            return;
        }
        auditor.info(modelId, msg.get().getFormattedMessage());
        shouldNotAudit.add(modelId);
        logger.info("[{}] {}", modelId, msg.get().getFormattedMessage());
    }

    private void loadModelsForPipeline(Set<String> modelIds) {
        if (modelIds.isEmpty()) {
            return;
        }
        // Execute this on a utility thread as when the callbacks occur we don't want them tying up the cluster listener thread pool
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            for (String modelId : modelIds) {
                auditNewReferencedModel(modelId);
                this.loadModel(modelId, Consumer.PIPELINE);
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
                for (Object processor : (List<?>) processors) {
                    if (processor instanceof Map<?, ?>) {
                        Object processorConfig = ((Map<?, ?>) processor).get(InferenceProcessor.TYPE);
                        if (processorConfig instanceof Map<?, ?>) {
                            Object modelId = ((Map<?, ?>) processorConfig).get(InferenceResults.MODEL_ID_RESULTS_FIELD);
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

    private static InferenceConfig inferenceConfigFromTargetType(TargetType targetType) {
        switch (targetType) {
            case REGRESSION:
                return RegressionConfig.EMPTY_PARAMS;
            case CLASSIFICATION:
                return ClassificationConfig.EMPTY_PARAMS;
            default:
                throw ExceptionsHelper.badRequestException("unsupported target type [{}]", targetType);
        }
    }

    /**
     * Register a listener for notification when a model is loaded.
     * <p>
     * This method is primarily intended for testing (hence package private)
     * and shouldn't be required outside of testing.
     *
     * @param modelId             Model Id
     * @param modelLoadedListener To be notified
     */
    void addModelLoadedListener(String modelId, ActionListener<LocalModel> modelLoadedListener) {
        synchronized (loadingListeners) {
            loadingListeners.compute(modelId, (modelKey, listenerQueue) -> {
                if (listenerQueue == null) {
                    return addFluently(new ArrayDeque<>(), modelLoadedListener);
                } else {
                    return addFluently(listenerQueue, modelLoadedListener);
                }
            });
        }
    }
}
