/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.datastreams.lifecycle.FrozenTransitionInfoProvider;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo.TransitionState;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.metadata.DataStream.DatastreamIndexTypes.BACKING_INDICES;
import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * Periodically evaluates the health of the DLM frozen-tier transition feature on the elected master and publishes the result to the
 * health node via {@link UpdateHealthInfoCacheAction}. Runs on its own scheduler, independent of {@link DLMFrozenTransitionService}'s,
 * so that it keeps reporting even if the transition service's own scheduler has died.
 */
public class DLMFrozenTransitionHealthInfoPublisher extends AbstractDLMPeriodicMasterOnlyService {

    static final Setting<TimeValue> PUBLISH_INTERVAL_SETTING = Setting.timeSetting(
        "dlm.frozen_transitions.health.publish_interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    // Caps the number of individual index entries sent to the health node in each publish cycle.
    static final int MAX_INDICES_TO_PUBLISH = 100;

    private static final Logger logger = getLogger(DLMFrozenTransitionHealthInfoPublisher.class);

    private final ClusterService clusterService;
    private final Client client;
    private final DLMFrozenTransitionService transitionService;
    private final DLMFrozenTransitionExecutor transitionExecutor;
    private final DLMFrozenTransitionSettings transitionSettings;
    private final LongSupplier nowSupplier;

    public DLMFrozenTransitionHealthInfoPublisher(
        ClusterService clusterService,
        Client client,
        DLMFrozenTransitionService transitionService,
        DLMFrozenTransitionExecutor transitionExecutor,
        DLMFrozenTransitionSettings transitionSettings
    ) {
        this(
            clusterService,
            client,
            transitionService,
            transitionExecutor,
            transitionSettings,
            System::currentTimeMillis,
            PUBLISH_INTERVAL_SETTING.get(clusterService.getSettings()).millis()
        );
    }

    // visible for testing
    DLMFrozenTransitionHealthInfoPublisher(
        ClusterService clusterService,
        Client client,
        DLMFrozenTransitionService transitionService,
        DLMFrozenTransitionExecutor transitionExecutor,
        DLMFrozenTransitionSettings transitionSettings,
        LongSupplier nowSupplier,
        long initialDelayMillis
    ) {
        super(clusterService, PUBLISH_INTERVAL_SETTING.get(clusterService.getSettings()), initialDelayMillis);
        this.clusterService = clusterService;
        this.client = client;
        this.transitionService = transitionService;
        this.transitionExecutor = transitionExecutor;
        this.transitionSettings = transitionSettings;
        this.nowSupplier = nowSupplier;
    }

    @Override
    Runnable getScheduledTask() {
        return this::publishHealthInfo;
    }

    @Override
    String getSchedulerThreadName() {
        return "dlm-frozen-health-publisher";
    }

    // visible for testing
    void publishHealthInfo() {
        ClusterState state = clusterService.state();
        DiscoveryNode healthNode = HealthNode.findHealthNode(state);
        if (healthNode == null) {
            logger.trace(
                "unable to report DLM frozen transition health because there is no health node in the cluster. "
                    + "will retry on the next run"
            );
            return;
        }
        String healthNodeId = healthNode.getId();
        DlmFrozenTransitionsHealthInfo info = buildHealthInfo(state);
        client.execute(
            UpdateHealthInfoCacheAction.INSTANCE,
            new UpdateHealthInfoCacheAction.Request.Builder().nodeId(healthNodeId).dlmFrozenTransitionsHealthInfo(info).build(),
            ActionListener.wrap(
                resp -> logger.trace("published DLM frozen transition health info to health node [{}]", healthNodeId),
                e -> logger.debug(
                    () -> Strings.format("failed to publish DLM frozen transition health info to health node [%s]", healthNodeId),
                    e
                )
            )
        );
    }

    // visible for testing
    DlmFrozenTransitionsHealthInfo buildHealthInfo(ClusterState state) {
        boolean transitionsEnabled = transitionSettings.isTransitionEnabled();
        boolean serviceRunning = transitionService.isSchedulerThreadRunning();
        String defaultRepository = clusterService.getClusterSettings().get(RepositoriesService.DEFAULT_REPOSITORY_SETTING);
        boolean defaultRepositoryConfigured = Strings.hasText(defaultRepository);

        long now = nowSupplier.getAsLong();
        long thresholdMillis = transitionSettings.getHealthStuckThreshold().millis();

        OverdueIndices overdueIndices = new OverdueIndices();

        for (ProjectMetadata projectMetadata : state.metadata().projects().values()) {
            ProjectId projectId = projectMetadata.id();

            for (DataStream dataStream : projectMetadata.dataStreams().values()) {
                DataStreamLifecycle lifecycle = dataStream.getDataLifecycle();
                if (lifecycle == null || lifecycle.enabled() == false || lifecycle.frozenAfter() == null) {
                    continue;
                }
                // An index is overdue once it has been eligible (past frozen_after) for longer than the stuck threshold.
                TimeValue overdueAfter = TimeValue.timeValueMillis(lifecycle.frozenAfter().millis() + thresholdMillis);

                List<Index> overdueCandidates = dataStream.getIndicesOlderThan(
                    projectMetadata::index,
                    nowSupplier,
                    overdueAfter,
                    BACKING_INDICES
                ).stream().sorted(Comparator.comparing(Index::getName)).toList();

                for (Index index : overdueCandidates) {
                    IndexMetadata indexMetadata = projectMetadata.index(index);
                    if (indexMetadata == null || DataStreamLifecycleService.frozenTransitionCompleted(indexMetadata)) {
                        continue;
                    }
                    overdueIndices.add(projectId, index.getName(), transitionStateFor(projectId, indexMetadata));
                }
            }
        }

        return new DlmFrozenTransitionsHealthInfo(
            transitionsEnabled,
            serviceRunning,
            defaultRepositoryConfigured,
            overdueIndices.sample(),
            overdueIndices.totalCount(),
            now,
            getPollInterval().millis()
        );
    }

    private TransitionState transitionStateFor(ProjectId projectId, IndexMetadata indexMetadata) {
        if (DataStreamLifecycleService.indexMarkedForFrozen(indexMetadata) == false) {
            return TransitionState.UNMARKED;
        }

        String indexName = indexMetadata.getIndex().getName();
        FrozenTransitionInfoProvider.Status status = transitionExecutor.getTransitionStatus(projectId, indexName);
        return switch (status) {
            case NOT_STARTED -> TransitionState.MARKED;
            case QUEUED -> TransitionState.QUEUED;
            case RUNNING -> TransitionState.RUNNING;
        };
    }

    /**
     * Mutable accumulator for overdue indices. Tracks the total count independently of the capped sample so that
     * callers can distinguish "no overdue indices" from "overdue indices that didn't fit in the sample". The cap
     * ({@link #MAX_INDICES_TO_PUBLISH}) is applied across all projects combined, not per project.
     */
    private static final class OverdueIndices {
        private int totalCount;
        private int sampledCount;
        private final Map<ProjectId, Map<String, TransitionState>> sample = new HashMap<>();

        void add(ProjectId projectId, String indexName, TransitionState state) {
            totalCount++;
            if (sampledCount < MAX_INDICES_TO_PUBLISH) {
                sample.computeIfAbsent(projectId, ignored -> new HashMap<>()).put(indexName, state);
                sampledCount++;
            }
        }

        Map<ProjectId, Map<String, TransitionState>> sample() {
            return sample;
        }

        int totalCount() {
            return totalCount;
        }
    }
}
