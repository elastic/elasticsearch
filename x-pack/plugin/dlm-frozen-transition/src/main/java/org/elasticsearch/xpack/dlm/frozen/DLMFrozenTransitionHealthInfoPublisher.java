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
import org.elasticsearch.health.node.DlmFrozenTransitionIndexInfo;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo;
import org.elasticsearch.health.node.StalledIndices;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

    /**
     * The time at which this node most recently became master (i.e. the start of the current master's tenure over frozen transitions),
     * or {@code 0} if this node has never been master. Used as the reference point for as a refference for stall checks to ensure
     * master failover does not trigger false positives for "marked but not started" stalled indices.
     */
    private volatile long masterTenureStartMillis = 0;

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

    @Override
    void onStart() {
        // Record the start of this master's tenure so the "marked but not started" stall check does not fire before a
        // freshly-elected master has had a threshold's worth of time to attempt outstanding transitions.
        masterTenureStartMillis = nowSupplier.getAsLong();
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

        // Don't indicate if master has failed over and new scan for marked indices has not run unless it's not run for
        // more than threshold time
        boolean initialScanPending = transitionService.hasCompletedScanSinceStart() == false
            && now - masterTenureStartMillis <= thresholdMillis;

        int markedIndicesCount = 0;
        StalledBucket eligibleUnmarked = new StalledBucket();
        StalledBucket notStartedMarked = new StalledBucket();
        StalledBucket queuedMarked = new StalledBucket();

        for (ProjectMetadata projectMetadata : state.metadata().projects().values()) {
            ProjectId projectId = projectMetadata.id();
            // Count all marked indices regardless of the current service config
            // This is what drives the "transitions disabled but pending work" YELLOW signal.
            for (IndexMetadata indexMetadata : projectMetadata.indices().values()) {
                if (DataStreamLifecycleService.indexMarkedForFrozen(indexMetadata)) {
                    markedIndicesCount++;
                }
            }

            for (DataStream dataStream : projectMetadata.dataStreams().values()) {
                DataStreamLifecycle lifecycle = dataStream.getDataLifecycle();
                if (lifecycle == null || lifecycle.enabled() == false || lifecycle.frozenAfter() == null) {
                    continue;
                }
                TimeValue frozenAfter = lifecycle.frozenAfter();

                List<Index> eligibleIndices = dataStream.getIndicesOlderThan(
                    projectMetadata::index,
                    nowSupplier,
                    frozenAfter,
                    BACKING_INDICES
                ).stream().sorted(Comparator.comparing(Index::getName)).toList();

                for (Index index : eligibleIndices) {
                    IndexMetadata indexMetadata = projectMetadata.index(index);
                    if (indexMetadata == null || DataStreamLifecycleService.frozenTransitionCompleted(indexMetadata)) {
                        continue;
                    }
                    long eligibleSinceMillis = dataStream.getGenerationLifecycleDate(indexMetadata).millis() + frozenAfter.millis();
                    boolean marked = DataStreamLifecycleService.indexMarkedForFrozen(indexMetadata);
                    if (marked == false) {
                        if (now - eligibleSinceMillis > thresholdMillis) {
                            eligibleUnmarked.add(projectId, index.getName(), eligibleSinceMillis);
                        }
                    } else if (now - eligibleSinceMillis > thresholdMillis) {
                        switch (transitionExecutor.getTransitionStatus(projectId, index.getName())) {
                            case NOT_STARTED -> {
                                if (initialScanPending == false) {
                                    notStartedMarked.add(projectId, index.getName(), eligibleSinceMillis);
                                }
                            }
                            case QUEUED -> {
                                long stalledSinceMillis = Math.max(eligibleSinceMillis, masterTenureStartMillis);
                                if (now - stalledSinceMillis > thresholdMillis) {
                                    queuedMarked.add(projectId, index.getName(), stalledSinceMillis);
                                }
                            }
                            // A running transition is making progress, so it is by definition not stalled.
                            case RUNNING -> {
                            }
                        }
                    }
                }
            }
        }

        return new DlmFrozenTransitionsHealthInfo(
            transitionsEnabled,
            serviceRunning,
            defaultRepositoryConfigured,
            markedIndicesCount,
            eligibleUnmarked.build(),
            notStartedMarked.build(),
            queuedMarked.build(),
            now,
            getPollInterval().millis()
        );
    }

    /**
     * Mutable accumulator for one category of stalled indices. Tracks the total count independently of the capped
     * sample so that callers can distinguish "no stalled indices" from "stalled indices that didn't fit in the sample".
     *
     * <p>The cap is applied per-bucket: in the worst case up to {@link #MAX_INDICES_TO_PUBLISH} entries are
     * collected per stall category.
     */
    private static final class StalledBucket {
        private int totalCount;
        private final List<DlmFrozenTransitionIndexInfo> sample = new ArrayList<>();

        void add(ProjectId projectId, String indexName, long stalledSinceMillis) {
            totalCount++;
            if (sample.size() < MAX_INDICES_TO_PUBLISH) {
                sample.add(new DlmFrozenTransitionIndexInfo(projectId, indexName, stalledSinceMillis));
            }
        }

        StalledIndices build() {
            return new StalledIndices(totalCount, sample);
        }
    }
}
