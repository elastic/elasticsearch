/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.xpack.ml.MachineLearning.ML_MODEL_INFERENCE_FEATURE;

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
    public static final Setting<ByteSizeValue> INFERENCE_MODEL_CACHE_SIZE = Setting.memorySizeSetting(
        "xpack.ml.inference_model.cache_size",
        "40%",
        Setting.Property.NodeScope
    );

    /**
     * How long should a model stay in the cache since its last access
     * <p>
     * If nothing references a model via getModel for this configured timeValue, it will be evicted.
     * <p>
     * Specifically, in the ingest scenario, a processor will call getModel whenever it needs to run inference. So, if a processor is not
     * executed for an extended period of time, the model will be evicted and will have to be loaded again when getModel is called.
     */
    public static final Setting<TimeValue> INFERENCE_MODEL_CACHE_TTL = Setting.timeSetting(
        "xpack.ml.inference_model.time_to_live",
        new TimeValue(5, TimeUnit.MINUTES),
        new TimeValue(1, TimeUnit.MILLISECONDS),
        Setting.Property.NodeScope
    );

    // The feature requesting the model
    public enum Consumer {
        PIPELINE,
        SEARCH,
        INTERNAL
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
    // Referenced models can be model aliases or IDs
    private final Set<String> referencedModels = new HashSet<>();
    private final Map<String, String> modelAliasToId = new HashMap<>();
    private final Map<String, Set<String>> modelIdToModelAliases = new HashMap<>();
    private final Map<String, Set<String>> modelIdToUpdatedModelAliases = new HashMap<>();
    private final Map<String, Queue<ActionListener<LocalModel>>> loadingListeners = new HashMap<>();
    private final TrainedModelProvider provider;
    private final Set<String> shouldNotAudit;
    private final ThreadPool threadPool;
    private final InferenceAuditor auditor;
    private final ByteSizeValue maxCacheSize;
    private final String localNode;
    private final CircuitBreaker trainedModelCircuitBreaker;
    private final XPackLicenseState licenseState;

    public ModelLoadingService(
        TrainedModelProvider trainedModelProvider,
        InferenceAuditor auditor,
        ThreadPool threadPool,
        ClusterService clusterService,
        TrainedModelStatsService modelStatsService,
        Settings settings,
        String localNode,
        CircuitBreaker trainedModelCircuitBreaker,
        XPackLicenseState licenseState
    ) {
        this.provider = trainedModelProvider;
        this.threadPool = threadPool;
        this.maxCacheSize = INFERENCE_MODEL_CACHE_SIZE.get(settings);
        this.auditor = auditor;
        this.modelStatsService = modelStatsService;
        this.shouldNotAudit = new HashSet<>();
        this.localModelCache = CacheBuilder.<String, ModelAndConsumer>builder()
            .setMaximumWeight(this.maxCacheSize.getBytes())
            .weigher((id, modelAndConsumer) -> modelAndConsumer.model.ramBytesUsed())
            .removalListener(this::cacheEvictionListener)
            .setExpireAfterAccess(INFERENCE_MODEL_CACHE_TTL.get(settings))
            .build();
        clusterService.addListener(this);
        this.localNode = localNode;
        this.trainedModelCircuitBreaker = ExceptionsHelper.requireNonNull(trainedModelCircuitBreaker, "trainedModelCircuitBreaker");
        this.licenseState = licenseState;
    }

    // for testing
    String getModelId(String modelIdOrAlias) {
        return modelAliasToId.getOrDefault(modelIdOrAlias, modelIdOrAlias);
    }

    boolean isModelCached(String modelId) {
        return localModelCache.get(modelAliasToId.getOrDefault(modelId, modelId)) != null;
    }

    public ByteSizeValue getMaxCacheSize() {
        return maxCacheSize;
    }

    /**
     * This method is intended for use in telemetry, not making decisions about what will fit in the cache.
     * The value returned could immediately be out-of-date if cache changes are in progress. It is good
     * enough for external reporting of vaguely up-to-date status, but not for anything related to immediate
     * additions to the cache.
     */
    public ByteSizeValue getCurrentCacheSize() {
        return ByteSizeValue.ofBytes(localModelCache.weight());
    }

    /**
     * Load the model for use by an ingest pipeline. The model will not be cached if there is no
     * ingest pipeline referencing it i.e. it is used in simulate mode
     *
     * @param modelId  the model to get
     * @param modelActionListener the listener to alert when the model has been retrieved
     */
    public void getModelForPipeline(String modelId, TaskId parentTaskId, ActionListener<LocalModel> modelActionListener) {
        getModel(modelId, Consumer.PIPELINE, parentTaskId, modelActionListener);
    }

    /**
     * Load the model for internal use. Note, this decompresses the model if the stored estimate doesn't trip circuit breakers.
     * Consequently, it assumes the model was created by an ML process
     * @param modelId  the model to get
     * @param modelActionListener the listener to alert when the model has been retrieved
     */
    public void getModelForInternalInference(String modelId, ActionListener<LocalModel> modelActionListener) {
        getModel(modelId, Consumer.INTERNAL, null, modelActionListener);
    }

    /**
     * Load the model for use by at search. Models requested by search are always cached.
     *
     * @param modelId  the model to get
     * @param modelActionListener the listener to alert when the model has been retrieved
     */
    public void getModelForSearch(String modelId, ActionListener<LocalModel> modelActionListener) {
        getModel(modelId, Consumer.SEARCH, null, modelActionListener);
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
     * @param modelIdOrAlias      the model id or model alias to get
     * @param consumer            which feature is requesting the model
     * @param parentTaskId        The parent task id
     * @param modelActionListener the listener to alert when the model has been retrieved.
     */
    private void getModel(String modelIdOrAlias, Consumer consumer, TaskId parentTaskId, ActionListener<LocalModel> modelActionListener) {
        final String modelId = modelAliasToId.getOrDefault(modelIdOrAlias, modelIdOrAlias);
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
            logger.trace(() -> format("[%s] (model_alias [%s]) loaded from cache", modelId, modelIdOrAlias));
            return;
        }

        if (loadModelIfNecessary(modelIdOrAlias, consumer, parentTaskId, modelActionListener)) {
            logger.trace(
                () -> format("[%s] (model_alias [%s]) is loading or loaded, added new listener to queue", modelId, modelIdOrAlias)
            );
        }
    }

    /**
     * If the model is cached it is returned directly to the listener
     * else if the model is CURRENTLY being loaded the listener is added to be notified when it is loaded
     * else the model load is initiated.
     *
     * @param modelIdOrAlias The model to get
     * @param consumer The model consumer
     * @param modelActionListener The listener
     * @param parentTaskId The parent task id
     * @return If the model is cached or currently being loaded true is returned. If a new load is started
     * false is returned to indicate a new load event
     */
    private boolean loadModelIfNecessary(
        String modelIdOrAlias,
        Consumer consumer,
        TaskId parentTaskId,
        ActionListener<LocalModel> modelActionListener
    ) {
        synchronized (loadingListeners) {
            final String modelId = modelAliasToId.getOrDefault(modelIdOrAlias, modelIdOrAlias);
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
            Queue<ActionListener<LocalModel>> listeners = loadingListeners.computeIfPresent(
                modelId,
                (storedModelKey, listenerQueue) -> addFluently(listenerQueue, modelActionListener)
            );

            // The cachedModel entry is null, but there are listeners present, that means it is being loaded
            if (listeners != null) {
                return true;
            }

            if (Consumer.SEARCH != consumer && referencedModels.contains(modelId) == false) {
                // The model is requested by a pipeline but not referenced by any ingest pipelines.
                // This means it is a simulate call and the model should not be cached
                logger.trace(
                    () -> format("[%s] (model_alias [%s]) not actively loading, eager loading without cache", modelId, modelIdOrAlias)
                );
                loadWithoutCaching(modelId, consumer, parentTaskId, modelActionListener);
            } else {
                logger.trace(() -> format("[%s] (model_alias [%s]) attempting to load and cache", modelId, modelIdOrAlias));
                loadingListeners.put(modelId, addFluently(new ArrayDeque<>(), modelActionListener));
                loadModel(modelId, consumer);
            }
            return false;
        } // synchronized (loadingListeners)
    }

    private void loadModel(String modelId, Consumer consumer) {
        // We cannot use parentTaskId here as multiple listeners may be wanting this model to be loaded
        // We don't want to cancel the loading if only ONE of them stops listening or closes connection
        // TODO Is there a way to only signal a cancel if all the listener tasks cancel???
        provider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.empty(), null, ActionListener.wrap(trainedModelConfig -> {
            if (trainedModelConfig.isAllocateOnly()) {
                if (consumer == Consumer.SEARCH) {
                    handleLoadFailure(
                        modelId,
                        new ElasticsearchStatusException(
                            "Trained model [{}] with type [{}] is currently not usable in search.",
                            RestStatus.BAD_REQUEST,
                            modelId,
                            trainedModelConfig.getModelType()
                        )
                    );
                    return;
                }
                handleLoadFailure(modelId, modelMustBeDeployedError(modelId));
                return;
            }
            auditNewReferencedModel(modelId);
            trainedModelCircuitBreaker.addEstimateBytesAndMaybeBreak(trainedModelConfig.getModelSize(), modelId);
            provider.getTrainedModelForInference(modelId, consumer == Consumer.INTERNAL, ActionListener.wrap(inferenceDefinition -> {
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
            }, failure -> {
                // We failed to get the definition, remove the initial estimation.
                trainedModelCircuitBreaker.addWithoutBreaking(-trainedModelConfig.getModelSize());
                logger.warn(() -> "[" + modelId + "] failed to load model definition", failure);
                handleLoadFailure(modelId, failure);
            }));
        }, failure -> {
            logger.warn(() -> "[" + modelId + "] failed to load model configuration", failure);
            handleLoadFailure(modelId, failure);
        }));
    }

    private void loadWithoutCaching(
        String modelId,
        Consumer consumer,
        TaskId parentTaskId,
        ActionListener<LocalModel> modelActionListener
    ) {
        // If we the model is not loaded and we did not kick off a new loading attempt, this means that we may be getting called
        // by a simulated pipeline
        provider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.empty(), parentTaskId, ActionListener.wrap(trainedModelConfig -> {
            // If the model should be allocated, we should fail here
            if (trainedModelConfig.isAllocateOnly()) {
                if (consumer == Consumer.SEARCH) {
                    modelActionListener.onFailure(
                        new ElasticsearchStatusException(
                            "model [{}] with type [{}] is currently not usable in search.",
                            RestStatus.BAD_REQUEST,
                            modelId,
                            trainedModelConfig.getModelType()
                        )
                    );
                    return;
                }
                modelActionListener.onFailure(modelMustBeDeployedError(modelId));
                return;
            }
            // Verify we can pull the model into memory without causing OOM
            trainedModelCircuitBreaker.addEstimateBytesAndMaybeBreak(trainedModelConfig.getModelSize(), modelId);
            provider.getTrainedModelForInference(modelId, consumer == Consumer.INTERNAL, ActionListener.wrap(inferenceDefinition -> {
                InferenceConfig inferenceConfig = trainedModelConfig.getInferenceConfig() == null
                    ? inferenceConfigFromTargetType(inferenceDefinition.getTargetType())
                    : trainedModelConfig.getInferenceConfig();
                try {
                    updateCircuitBreakerEstimate(modelId, inferenceDefinition, trainedModelConfig);
                } catch (CircuitBreakingException ex) {
                    modelActionListener.onFailure(ex);
                    return;
                }

                modelActionListener.onResponse(
                    new LocalModel(
                        trainedModelConfig.getModelId(),
                        localNode,
                        inferenceDefinition,
                        trainedModelConfig.getInput(),
                        trainedModelConfig.getDefaultFieldMap(),
                        inferenceConfig,
                        trainedModelConfig.getLicenseLevel(),
                        modelStatsService,
                        trainedModelCircuitBreaker
                    )
                );
            },
                // Failure getting the definition, remove the initial estimation value
                e -> {
                    trainedModelCircuitBreaker.addWithoutBreaking(-trainedModelConfig.getModelSize());
                    if (unwrapCause(e) instanceof ResourceNotFoundException) {
                        modelActionListener.onFailure(e);
                    } else {
                        modelActionListener.onFailure(
                            new ElasticsearchStatusException(
                                "failed to load model [{}] definition",
                                RestStatus.INTERNAL_SERVER_ERROR,
                                modelId,
                                e
                            )
                        );
                    }
                }
            ));
        }, modelActionListener::onFailure));
    }

    private void updateCircuitBreakerEstimate(
        String modelId,
        InferenceDefinition inferenceDefinition,
        TrainedModelConfig trainedModelConfig
    ) throws CircuitBreakingException {
        long estimateDiff = inferenceDefinition.ramBytesUsed() - trainedModelConfig.getModelSize();
        if (estimateDiff < 0) {
            trainedModelCircuitBreaker.addWithoutBreaking(estimateDiff);
        } else if (estimateDiff > 0) { // rare case where estimate is now HIGHER
            try {
                trainedModelCircuitBreaker.addEstimateBytesAndMaybeBreak(estimateDiff, modelId);
            } catch (CircuitBreakingException ex) { // if we failed here, we should remove the initial estimate as well
                trainedModelCircuitBreaker.addWithoutBreaking(-trainedModelConfig.getModelSize());
                throw ex;
            }
        }
    }

    private ElasticsearchStatusException modelMustBeDeployedError(String modelId) {
        return new ElasticsearchStatusException(
            "Model [{}] must be deployed to use. Please deploy with the start trained model deployment API.",
            RestStatus.BAD_REQUEST,
            modelId
        );
    }

    private void handleLoadSuccess(
        String modelId,
        Consumer consumer,
        TrainedModelConfig trainedModelConfig,
        InferenceDefinition inferenceDefinition
    ) {
        Queue<ActionListener<LocalModel>> listeners;
        InferenceConfig inferenceConfig = trainedModelConfig.getInferenceConfig() == null
            ? inferenceConfigFromTargetType(inferenceDefinition.getTargetType())
            : trainedModelConfig.getInferenceConfig();
        LocalModel loadedModel = new LocalModel(
            trainedModelConfig.getModelId(),
            localNode,
            inferenceDefinition,
            trainedModelConfig.getInput(),
            trainedModelConfig.getDefaultFieldMap(),
            inferenceConfig,
            trainedModelConfig.getLicenseLevel(),
            modelStatsService,
            trainedModelCircuitBreaker
        );
        final ModelAndConsumerLoader modelAndConsumerLoader = new ModelAndConsumerLoader(new ModelAndConsumer(loadedModel, consumer));
        synchronized (loadingListeners) {
            populateNewModelAlias(modelId);
            // If the model is referenced, that means it is currently in a pipeline somewhere
            // Also, if the consumer is a search consumer, we should always cache it
            if (referencedModels.contains(modelId)
                || Sets.haveNonEmptyIntersection(modelIdToModelAliases.getOrDefault(modelId, new HashSet<>()), referencedModels)
                || consumer.equals(Consumer.SEARCH)) {
                try {
                    // The local model may already be in cache. If it is, we don't bother adding it to cache.
                    // If it isn't, we flip an `isLoaded` flag, and increment the model counter to make sure if it is evicted
                    // between now and when the listeners access it, the circuit breaker reflects actual usage.
                    localModelCache.computeIfAbsent(modelId, modelAndConsumerLoader);
                    // We should start tracking on successful load. It will stop being tracked once it evacuates the cache and is no
                    // longer a referenced model
                    // NOTE: It is not possible to change the referenced models without locking on `loadingListeners`
                    // So, if the model is evacuated from cache immediately after checking that it was present,
                    // the feature usage will still be tracked.
                    if (License.OperationMode.BASIC.equals(trainedModelConfig.getLicenseLevel()) == false) {
                        ML_MODEL_INFERENCE_FEATURE.startTracking(licenseState, modelId);
                    }
                } catch (ExecutionException ee) {
                    logger.warn(() -> "[" + modelId + "] threw when attempting add to cache", ee);
                }
                shouldNotAudit.remove(modelId);
            }
            listeners = loadingListeners.remove(modelId);
            // if there are no listeners, we should just exit
            if (listeners == null) {
                // If we newly added it into cache, release the model so that the circuit breaker can still accurately keep track
                // of memory
                if (modelAndConsumerLoader.isLoaded()) {
                    loadedModel.release();
                }
                return;
            }
        } // synchronized (loadingListeners)
        for (ActionListener<LocalModel> listener = listeners.poll(); listener != null; listener = listeners.poll()) {
            loadedModel.acquire();
            listener.onResponse(loadedModel);
        }
        // account for the acquire in the synchronized block above if the model was loaded into the cache
        if (modelAndConsumerLoader.isLoaded()) {
            loadedModel.release();
        }
    }

    private void handleLoadFailure(String modelId, Exception failure) {
        Queue<ActionListener<LocalModel>> listeners;
        synchronized (loadingListeners) {
            listeners = loadingListeners.remove(modelId);
            populateNewModelAlias(modelId);
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

    private void populateNewModelAlias(String modelId) {
        Set<String> newModelAliases = modelIdToUpdatedModelAliases.remove(modelId);
        if (newModelAliases != null && newModelAliases.isEmpty() == false) {
            logger.trace(() -> format("[%s] model is now loaded, setting new model_aliases %s", modelId, newModelAliases));
            for (String modelAlias : newModelAliases) {
                modelAliasToId.put(modelAlias, modelId);
            }
        }
    }

    private void cacheEvictionListener(RemovalNotification<String, ModelAndConsumer> notification) {
        try {
            if (notification.getRemovalReason() == RemovalNotification.RemovalReason.EVICTED) {
                Supplier<String> msg = () -> Strings.format(
                    "model cache entry evicted."
                        + "current cache [%s] current max [%s] model size [%s]. "
                        + "If this is undesired, consider updating setting [%s] or [%s].",
                    ByteSizeValue.ofBytes(localModelCache.weight()).getStringRep(),
                    maxCacheSize.getStringRep(),
                    ByteSizeValue.ofBytes(notification.getValue().model.ramBytesUsed()).getStringRep(),
                    INFERENCE_MODEL_CACHE_SIZE.getKey(),
                    INFERENCE_MODEL_CACHE_TTL.getKey()
                );
                auditIfNecessary(notification.getKey(), msg);
            }
            String modelId = modelAliasToId.getOrDefault(notification.getKey(), notification.getKey());
            logger.trace(
                () -> format(
                    "Persisting stats for evicted model [%s] (model_aliases %s)",
                    modelId,
                    modelIdToModelAliases.getOrDefault(modelId, new HashSet<>())
                )
            );
            // If it's not referenced in a pipeline, stop tracking it on this node
            if (referencedModels.contains(modelId) == false) {
                ML_MODEL_INFERENCE_FEATURE.stopTracking(licenseState, modelId);
            }

            // If the model is no longer referenced, flush the stats to persist as soon as possible
            notification.getValue().model.persistStats(referencedModels.contains(modelId) == false);
        } finally {
            notification.getValue().model.release();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final boolean prefetchModels = event.state().nodes().getLocalNode().isIngestNode();
        // If we are not prefetching models and there were no model alias changes, don't bother handling the changes
        if ((prefetchModels == false)
            && (event.changedCustomMetadataSet().contains(IngestMetadata.TYPE) == false)
            && (event.changedCustomMetadataSet().contains(ModelAliasMetadata.NAME) == false)) {
            return;
        }

        ClusterState state = event.state();
        IngestMetadata currentIngestMetadata = state.metadata().custom(IngestMetadata.TYPE);
        Set<String> allReferencedModelKeys = event.changedCustomMetadataSet().contains(IngestMetadata.TYPE)
            ? getReferencedModelKeys(currentIngestMetadata)
            : new HashSet<>(referencedModels);
        Set<String> referencedModelsBeforeClusterState;
        Set<String> loadingModelBeforeClusterState = null;
        Set<String> removedModels;
        Map<String, Set<String>> addedModelViaAliases = new HashMap<>();
        Map<String, Set<String>> oldIdToAliases;
        synchronized (loadingListeners) {
            oldIdToAliases = new HashMap<>(modelIdToModelAliases);
            Map<String, String> changedAliases = gatherLazyChangedAliasesAndUpdateModelAliases(
                event,
                prefetchModels,
                allReferencedModelKeys
            );

            // if we are not prefetching, exit now.
            if (prefetchModels == false) {
                return;
            }

            referencedModelsBeforeClusterState = new HashSet<>(referencedModels);
            if (logger.isTraceEnabled()) {
                loadingModelBeforeClusterState = new HashSet<>(loadingListeners.keySet());
            }
            removedModels = Sets.difference(referencedModelsBeforeClusterState, allReferencedModelKeys);

            // Remove the models that are no longer referenced
            referencedModels.removeAll(removedModels);
            shouldNotAudit.removeAll(removedModels);

            // Remove all cached models that are not referenced by any processors
            // and are not used in search
            for (String modelAliasOrId : removedModels) {
                String modelId = changedAliases.getOrDefault(modelAliasOrId, modelAliasToId.getOrDefault(modelAliasOrId, modelAliasOrId));
                // If the "old" model_alias is referenced, we don't want to invalidate. This way the model that now has the model_alias
                // can be loaded in first
                boolean oldModelAliasesNotReferenced = Sets.haveEmptyIntersection(
                    referencedModels,
                    oldIdToAliases.getOrDefault(modelId, Collections.emptySet())
                );
                // If the model itself is referenced, we shouldn't evict.
                boolean modelIsNotReferenced = referencedModels.contains(modelId) == false;
                // If a model_alias change causes it to NOW be referenced, we shouldn't attempt to evict it
                boolean newModelAliasesNotReferenced = Sets.haveEmptyIntersection(
                    referencedModels,
                    modelIdToModelAliases.getOrDefault(modelId, Collections.emptySet())
                );
                if (oldModelAliasesNotReferenced && newModelAliasesNotReferenced && modelIsNotReferenced) {
                    ModelAndConsumer modelAndConsumer = localModelCache.get(modelId);
                    if (modelAndConsumer != null && modelAndConsumer.consumers.contains(Consumer.SEARCH) == false) {
                        logger.trace("[{} ({})] invalidated from cache", modelId, modelAliasOrId);
                        localModelCache.invalidate(modelId);
                    }
                    // The model is not cached and the model no longer referenced we should ensure that we are not tracking the
                    // license usage.
                    // It is possible that we stop referencing it BEFORE its cached, or it was previously invalidated
                    // Either way, we know we won't put it back in cache as we are synchronized on `loadingListeners`
                    if (modelAndConsumer == null) {
                        ML_MODEL_INFERENCE_FEATURE.stopTracking(licenseState, modelId);
                    }
                }
            }
            // Remove all that are still referenced, i.e. the intersection of allReferencedModelKeys and referencedModels
            allReferencedModelKeys.removeAll(referencedModels);
            for (String newlyReferencedModel : allReferencedModelKeys) {
                // check if the model_alias has changed in this round
                String modelId = changedAliases.getOrDefault(
                    newlyReferencedModel,
                    // If the model_alias hasn't changed, get the model id IF it is a model_alias, otherwise we assume it is an id
                    modelAliasToId.getOrDefault(newlyReferencedModel, newlyReferencedModel)
                );
                // Verify that it isn't an old model id but just a new model_alias
                if (referencedModels.contains(modelId) == false) {
                    addedModelViaAliases.computeIfAbsent(modelId, k -> new HashSet<>()).add(newlyReferencedModel);
                }
            }
            // For any previously referenced model, the model_alias COULD have changed, so it is actually a NEWLY referenced model
            for (Map.Entry<String, String> modelAliasAndId : changedAliases.entrySet()) {
                String modelAlias = modelAliasAndId.getKey();
                String modelId = modelAliasAndId.getValue();
                if (referencedModels.contains(modelAlias)) {
                    // we need to load the underlying model since its model_alias is referenced
                    addedModelViaAliases.computeIfAbsent(modelId, k -> new HashSet<>()).add(modelAlias);
                    // If we are in cache, keep the old translation for now, it will be updated later
                    String oldModelId = modelAliasToId.get(modelAlias);
                    if (oldModelId != null && localModelCache.get(oldModelId) != null) {
                        modelIdToUpdatedModelAliases.computeIfAbsent(modelId, k -> new HashSet<>()).add(modelAlias);
                    } else {
                        // If we are not cached, might as well add the translation right away as new callers will have to load
                        // from disk anyways.
                        modelAliasToId.put(modelAlias, modelId);
                    }
                } else {
                    // Add model_alias and id here, since the model_alias wasn't previously referenced,
                    // no reason to wait on updating the model_alias -> model_id mapping
                    modelAliasToId.put(modelAlias, modelId);
                }
            }
            // Gather ALL currently referenced model ids
            referencedModels.addAll(allReferencedModelKeys);

            // Populate loadingListeners key so we know that we are currently loading the model
            for (String modelId : addedModelViaAliases.keySet()) {
                loadingListeners.computeIfAbsent(modelId, (s) -> new ArrayDeque<>());
            }
        } // synchronized (loadingListeners)
        if (logger.isTraceEnabled()) {
            if (loadingListeners.keySet().equals(loadingModelBeforeClusterState) == false) {
                logger.trace(
                    "cluster state event changed loading models: before {} after {}",
                    loadingModelBeforeClusterState,
                    loadingListeners.keySet()
                );
            }
            if (referencedModels.equals(referencedModelsBeforeClusterState) == false) {
                logger.trace(
                    "cluster state event changed referenced models: before {} after {}",
                    referencedModelsBeforeClusterState,
                    referencedModels
                );
            }
            if (oldIdToAliases.equals(modelIdToModelAliases) == false) {
                logger.trace(
                    "model id to alias mappings changed. before {} after {}. Model alias to IDs {}",
                    oldIdToAliases,
                    modelIdToModelAliases,
                    modelAliasToId
                );
            }
            if (addedModelViaAliases.isEmpty() == false) {
                logger.trace("adding new models via model_aliases and ids: {}", addedModelViaAliases);
            }
            if (modelIdToUpdatedModelAliases.isEmpty() == false) {
                logger.trace("delayed model aliases to update {}", modelIdToModelAliases);
            }
        }
        removedModels.forEach(this::auditUnreferencedModel);
        loadModelsForPipeline(addedModelViaAliases.keySet());
    }

    private Map<String, String> gatherLazyChangedAliasesAndUpdateModelAliases(
        ClusterChangedEvent event,
        boolean prefetchModels,
        Set<String> allReferencedModelKeys
    ) {
        Map<String, String> changedAliases = new HashMap<>();
        if (event.changedCustomMetadataSet().contains(ModelAliasMetadata.NAME)) {
            final Map<java.lang.String, ModelAliasMetadata.ModelAliasEntry> modelAliasesToIds = new HashMap<>(
                ModelAliasMetadata.fromState(event.state()).modelAliases()
            );
            modelIdToModelAliases.clear();
            for (Map.Entry<java.lang.String, ModelAliasMetadata.ModelAliasEntry> aliasToId : modelAliasesToIds.entrySet()) {
                modelIdToModelAliases.computeIfAbsent(aliasToId.getValue().getModelId(), k -> new HashSet<>()).add(aliasToId.getKey());
                java.lang.String modelId = modelAliasToId.get(aliasToId.getKey());
                if (modelId != null && modelId.equals(aliasToId.getValue().getModelId()) == false) {
                    if (prefetchModels && allReferencedModelKeys.contains(aliasToId.getKey())) {
                        changedAliases.put(aliasToId.getKey(), aliasToId.getValue().getModelId());
                    } else {
                        modelAliasToId.put(aliasToId.getKey(), aliasToId.getValue().getModelId());
                    }
                }
                if (modelId == null) {
                    modelAliasToId.put(aliasToId.getKey(), aliasToId.getValue().getModelId());
                }
            }
            Set<java.lang.String> removedAliases = Sets.difference(modelAliasToId.keySet(), modelAliasesToIds.keySet());
            modelAliasToId.keySet().removeAll(removedAliases);
        }
        return changedAliases;
    }

    private void auditIfNecessary(String modelId, Supplier<String> msg) {
        if (shouldNotAudit.contains(modelId)) {
            logger.trace(() -> format("[%s] %s", modelId, msg.get()));
            return;
        }
        auditor.info(modelId, msg.get());
        shouldNotAudit.add(modelId);
        logger.info("[{}] {}", modelId, msg.get());
    }

    private void loadModelsForPipeline(Set<String> modelIds) {
        if (modelIds.isEmpty()) {
            return;
        }
        // Execute this on a utility thread as when the callbacks occur we don't want them tying up the cluster listener thread pool
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            for (String modelId : modelIds) {
                loadModel(modelId, Consumer.PIPELINE);
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
        return switch (targetType) {
            case REGRESSION -> RegressionConfig.EMPTY_PARAMS;
            case CLASSIFICATION -> ClassificationConfig.EMPTY_PARAMS;
        };
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

    private static class ModelAndConsumerLoader implements CacheLoader<String, ModelAndConsumer> {

        private boolean loaded;
        private final ModelAndConsumer modelAndConsumer;

        ModelAndConsumerLoader(ModelAndConsumer modelAndConsumer) {
            this.modelAndConsumer = modelAndConsumer;
        }

        boolean isLoaded() {
            return loaded;
        }

        @Override
        public ModelAndConsumer load(String key) throws Exception {
            loaded = true;
            modelAndConsumer.model.acquire();
            return modelAndConsumer;
        }
    }
}
