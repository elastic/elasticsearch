/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.InferenceEndpointRegistry;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.IngestModelMemoryProvider;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Master-only service that tracks JVM heap bytes required to load trained models referenced by ingest pipelines.
 */
public class IngestModelMemoryService implements ClusterStateListener, IngestModelMemoryProvider {

    private static final Logger logger = LogManager.getLogger(IngestModelMemoryService.class);

    static final TimeValue UNRESOLVED_MODEL_SIZE_RETRY_INTERVAL = TimeValue.timeValueSeconds(30);
    static final TimeValue STALE_MODEL_SIZE_WARN_THRESHOLD = TimeValue.timeValueMinutes(5);

    private final TrainedModelProvider trainedModelProvider;
    private final ThreadPool threadPool;

    private final ConcurrentHashMap<ProjectId, Set<String>> referencedModelsByProject = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, OptionalLong> globalModelSizes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> unresolvedSinceNanos = new ConcurrentHashMap<>();
    private final Set<String> fetchScheduledModelIds = ConcurrentHashMap.newKeySet();
    private final Set<String> staleWarnEmitted = ConcurrentHashMap.newKeySet();
    private final AtomicReference<HeapRequirement> cachedRequirement = new AtomicReference<>(new HeapRequirement(0L, true));
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private volatile TimeValue unresolvedModelSizeRetryInterval = UNRESOLVED_MODEL_SIZE_RETRY_INTERVAL;
    private volatile Scheduler.Cancellable retryCancellable;

    public IngestModelMemoryService(TrainedModelProvider trainedModelProvider, ThreadPool threadPool) {
        this.trainedModelProvider = trainedModelProvider;
        this.threadPool = threadPool;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedMaster() == false) {
            if (initialized.getAndSet(false)) {
                clearAllState();
            }
            return;
        }

        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        if (initialized.compareAndSet(false, true)) {
            repopulateAllProjects(event.state());
            startPeriodicRetry();
            return;
        }

        handleRemovedProjects(event);
        handleAddedProjects(event);
        handleChangedProjects(event);
    }

    private void repopulateAllProjects(ClusterState state) {
        clearAllState();
        for (ProjectMetadata project : state.metadata().projects().values()) {
            refreshProjectFromFullScan(project.id(), state);
        }
    }

    private void handleRemovedProjects(ClusterChangedEvent event) {
        for (ProjectId removed : event.projectDelta().removed()) {
            Set<String> removedModels = referencedModelsByProject.remove(removed);
            if (removedModels != null) {
                removedModels.forEach(this::reconcileGlobalAfterProjectDrop);
            }
        }
    }

    private void handleAddedProjects(ClusterChangedEvent event) {
        for (ProjectId added : event.projectDelta().added()) {
            if (ingestOrAliasRelevant(event, added)) {
                refreshProjectFromFullScan(added, event.state());
            }
        }
    }

    private void handleChangedProjects(ClusterChangedEvent event) {
        for (ProjectMetadata project : event.state().metadata().projects().values()) {
            ProjectId projectId = project.id();
            if (event.projectDelta().added().contains(projectId)) {
                continue;
            }
            if (event.customMetadataChanged(projectId, IngestMetadata.TYPE)
                || event.customMetadataChanged(projectId, ModelAliasMetadata.NAME)) {
                refreshProjectFromFullScan(projectId, event.state());
            }
        }
    }

    @Override
    public HeapRequirement getRequiredHeapBytes() {
        return cachedRequirement.get();
    }

    private void clearAllState() {
        stopPeriodicRetry();
        referencedModelsByProject.clear();
        globalModelSizes.clear();
        unresolvedSinceNanos.clear();
        fetchScheduledModelIds.clear();
        staleWarnEmitted.clear();
        recomputeHeapRequirement();
    }

    private synchronized void startPeriodicRetry() {
        if (retryCancellable != null) {
            return;
        }
        try {
            retryCancellable = threadPool.scheduleWithFixedDelay(
                this::reconcileModelSizes,
                unresolvedModelSizeRetryInterval,
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
            );
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown() == false) {
                throw e;
            }
        }
    }

    private synchronized void stopPeriodicRetry() {
        if (retryCancellable != null && retryCancellable.isCancelled() == false) {
            retryCancellable.cancel();
            retryCancellable = null;
        }
    }

    private void reconcileModelSizes() {
        for (String modelId : globalModelSizes.keySet()) {
            OptionalLong size = globalModelSizes.get(modelId);
            if (size == null) {
                continue;
            }
            if (size.isPresent()) {
                scheduleRevalidationFetch(modelId);
            } else {
                scheduleFetchIfNeeded(modelId);
                warnIfUnresolvedTooLong(modelId);
            }
        }
        recomputeHeapRequirement();
    }

    private boolean isUnresolvedLongerThanWarnThreshold(String modelId) {
        Long sinceNanos = unresolvedSinceNanos.get(modelId);
        if (sinceNanos == null) {
            return false;
        }
        return threadPool.relativeTimeInNanos() - sinceNanos > STALE_MODEL_SIZE_WARN_THRESHOLD.nanos();
    }

    private void warnIfUnresolvedTooLong(String modelId) {
        if (isUnresolvedLongerThanWarnThreshold(modelId) == false) {
            return;
        }
        if (staleWarnEmitted.add(modelId) == false) {
            return;
        }
        logger.warn(
            "Ingest model [{}] heap size has been unresolved for over {}; heap contribution is now treated as exact zero so "
                + "ingest-tier autoscaling quality is no longer pinned at MINIMUM for this reason, but the model is still "
                + "unresolved and fetches continue in the background",
            modelId,
            STALE_MODEL_SIZE_WARN_THRESHOLD
        );
    }

    private void recordUnresolvedModel(String modelId) {
        unresolvedSinceNanos.putIfAbsent(modelId, threadPool.relativeTimeInNanos());
    }

    private synchronized void recomputeHeapRequirement() {
        long total = 0L;
        boolean exact = true;
        for (var entry : globalModelSizes.entrySet()) {
            OptionalLong size = entry.getValue();
            if (size.isPresent()) {
                total += size.getAsLong();
            } else if (isUnresolvedLongerThanWarnThreshold(entry.getKey()) == false) {
                exact = false;
            }
        }
        cachedRequirement.set(new HeapRequirement(total, exact));
    }

    private boolean ingestOrAliasRelevant(ClusterChangedEvent event, ProjectId projectId) {
        return event.customMetadataChanged(projectId, IngestMetadata.TYPE)
            || event.customMetadataChanged(projectId, ModelAliasMetadata.NAME)
            || event.previousState().metadata().projects().get(projectId) == null;
    }

    private void refreshProjectFromFullScan(ProjectId projectId, ClusterState state) {
        ProjectMetadata project = state.metadata().getProject(projectId);
        Set<String> nowReferenced = IngestPipelineModelReferences.resolveReferencedModelsForProject(state, projectId);
        Set<String> endpointIds = InferenceEndpointRegistry.getInstance().inferenceEndpointIds(project);
        if (endpointIds.isEmpty() == false) {
            nowReferenced = new HashSet<>(nowReferenced);
            nowReferenced.removeAll(endpointIds);
        }
        Set<String> perProject = referencedModelsByProject.computeIfAbsent(projectId, k -> ConcurrentHashMap.newKeySet());
        Set<String> current = new HashSet<>(perProject);
        Set<String> added = Sets.difference(nowReferenced, current);
        Set<String> removed = Sets.difference(current, nowReferenced);
        for (String modelId : removed) {
            perProject.remove(modelId);
            reconcileGlobalAfterProjectDrop(modelId);
        }
        for (String modelId : added) {
            perProject.add(modelId);
            if (globalModelSizes.putIfAbsent(modelId, OptionalLong.empty()) == null) {
                recordUnresolvedModel(modelId);
            }
            scheduleFetchIfNeeded(modelId);
        }
        recomputeHeapRequirement();
    }

    private void reconcileGlobalAfterProjectDrop(String modelId) {
        boolean stillReferenced = referencedModelsByProject.values().stream().anyMatch(set -> set.contains(modelId));
        if (stillReferenced == false) {
            globalModelSizes.remove(modelId);
            unresolvedSinceNanos.remove(modelId);
            fetchScheduledModelIds.remove(modelId);
            staleWarnEmitted.remove(modelId);
            recomputeHeapRequirement();
        }
    }

    private void scheduleFetchIfNeeded(String modelId) {
        scheduleModelSizeFetch(modelId, false);
    }

    private void scheduleRevalidationFetch(String modelId) {
        scheduleModelSizeFetch(modelId, true);
    }

    private void scheduleModelSizeFetch(String modelId, boolean revalidate) {
        OptionalLong currentSize = globalModelSizes.get(modelId);
        if (currentSize == null) {
            return;
        }
        if (revalidate == false && currentSize.isPresent()) {
            return;
        }
        if (fetchScheduledModelIds.add(modelId) == false) {
            return;
        }
        final boolean wasResolved = currentSize.isPresent();
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
            .execute(
                () -> trainedModelProvider.getTrainedModel(
                    modelId,
                    GetTrainedModelsAction.Includes.empty(),
                    null,
                    ActionListener.wrap(config -> {
                        fetchScheduledModelIds.remove(modelId);
                        if (isDfaModel(config)) {
                            propagateModelSize(modelId, toStoredSize(config.getModelSize()));
                        } else {
                            untrackModel(modelId);
                        }
                    }, e -> {
                        fetchScheduledModelIds.remove(modelId);
                        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                            logger.debug("Ingest model [{}] config not found yet; will retry", modelId);
                            if (wasResolved) {
                                handleRevalidationFetchFailure(modelId);
                            }
                        } else {
                            logger.warn("Could not fetch config for ingest model [{}]: {}", modelId, e.getMessage());
                            if (wasResolved) {
                                handleRevalidationFetchFailure(modelId);
                            }
                        }
                    })
                )
            );
    }

    private void handleRevalidationFetchFailure(String modelId) {
        boolean stillReferenced = referencedModelsByProject.values().stream().anyMatch(set -> set.contains(modelId));
        if (stillReferenced == false) {
            globalModelSizes.remove(modelId);
            unresolvedSinceNanos.remove(modelId);
            staleWarnEmitted.remove(modelId);
            recomputeHeapRequirement();
            return;
        }
        globalModelSizes.put(modelId, OptionalLong.empty());
        recordUnresolvedModel(modelId);
        recomputeHeapRequirement();
    }

    private static OptionalLong toStoredSize(long modelSizeBytes) {
        return OptionalLong.of(modelSizeBytes);
    }

    private static boolean isDfaModel(TrainedModelConfig config) {
        return Optional.ofNullable(config.getModelType()).orElse(TrainedModelType.TREE_ENSEMBLE) == TrainedModelType.TREE_ENSEMBLE;
    }

    private void untrackModel(String modelId) {
        globalModelSizes.remove(modelId);
        unresolvedSinceNanos.remove(modelId);
        fetchScheduledModelIds.remove(modelId);
        staleWarnEmitted.remove(modelId);
        recomputeHeapRequirement();
    }

    private void propagateModelSize(String modelId, OptionalLong size) {
        boolean stillReferenced = referencedModelsByProject.values().stream().anyMatch(set -> set.contains(modelId));
        if (stillReferenced == false) {
            globalModelSizes.remove(modelId);
            unresolvedSinceNanos.remove(modelId);
            fetchScheduledModelIds.remove(modelId);
            staleWarnEmitted.remove(modelId);
            recomputeHeapRequirement();
            return;
        }
        globalModelSizes.put(modelId, size);
        if (size.isPresent()) {
            unresolvedSinceNanos.remove(modelId);
            staleWarnEmitted.remove(modelId);
        } else {
            recordUnresolvedModel(modelId);
        }
        recomputeHeapRequirement();
    }

    // Visible for tests
    OptionalLong getGlobalModelSizeForTests(String modelId) {
        return globalModelSizes.get(modelId);
    }

    boolean isTrackingModelForProjectForTests(ProjectId projectId, String modelId) {
        return referencedModelsByProject.getOrDefault(projectId, Set.of()).contains(modelId);
    }

    void propagateModelSizeForTests(String modelId, OptionalLong size) {
        propagateModelSize(modelId, size);
    }

    void reconcileModelSizesForTests() {
        reconcileModelSizes();
    }

    void setUnresolvedSinceNanosForTests(String modelId, long sinceNanos) {
        unresolvedSinceNanos.put(modelId, sinceNanos);
    }

    boolean isPeriodicRetryRunningForTests() {
        return retryCancellable != null;
    }

    boolean isFetchScheduledForTests(String modelId) {
        return fetchScheduledModelIds.contains(modelId);
    }

    boolean hasScheduledFetchesForTests() {
        return fetchScheduledModelIds.isEmpty() == false;
    }

    void setUnresolvedModelSizeRetryIntervalForTests(TimeValue interval) {
        this.unresolvedModelSizeRetryInterval = interval;
    }
}
