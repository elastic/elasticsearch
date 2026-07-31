/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Optional;

/**
 * Observes writes to one index and feeds them to {@link DerivedMetricsService} when the index belongs to a data stream that configured
 * derived metrics.
 *
 * <p>Only operations originating on the primary are observed, so a document is counted exactly once no matter how many replicas it is
 * written to.
 *
 * <p>Resolving an index to its data stream configuration means walking cluster state, which is too expensive to do per document. The
 * resolution is therefore cached and only recomputed when the cluster state version changes.
 */
public class DerivedMetricsIndexingListener implements IndexingOperationListener {

    private static final Logger logger = LogManager.getLogger(DerivedMetricsIndexingListener.class);

    /**
     * The resolution of an index to the derived metrics it feeds. A resolution that is not {@code enabled} means the index does not feed
     * any, which is by far the most common case and is cached just as eagerly as a positive resolution.
     */
    private record Resolution(
        long clusterStateVersion,
        @Nullable ProjectId project,
        @Nullable String dataStream,
        @Nullable DataStreamDerivedMetrics config,
        @Nullable CompiledDerivedMetrics compiled
    ) {
        static Resolution none(long clusterStateVersion) {
            return new Resolution(clusterStateVersion, null, null, null, null);
        }

        boolean enabled() {
            return compiled != null;
        }
    }

    private final ClusterService clusterService;
    private final DerivedMetricsService service;
    private final Index index;

    private volatile Resolution cached;

    public DerivedMetricsIndexingListener(ClusterService clusterService, DerivedMetricsService service, Index index) {
        this.clusterService = clusterService;
        this.service = service;
        this.index = index;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index operation, Engine.IndexResult result) {
        record(operation, result.getResultType() == Engine.Result.Type.SUCCESS && result.getFailure() == null);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index operation, Exception ex) {
        record(operation, false);
    }

    private void record(Engine.Index operation, boolean succeeded) {
        if (operation.origin() != Engine.Operation.Origin.PRIMARY) {
            return;
        }
        Resolution resolution = resolve();
        if (resolution.enabled() == false) {
            return;
        }
        service.record(resolution.project(), resolution.dataStream(), resolution.compiled(), operation.parsedDoc(), succeeded);
    }

    private Resolution resolve() {
        ClusterState state = clusterService.state();
        long version = state.version();
        Resolution resolution = cached;
        if (resolution != null && resolution.clusterStateVersion() == version) {
            return resolution;
        }
        resolution = doResolve(state, version, resolution);
        cached = resolution;
        return resolution;
    }

    private Resolution doResolve(ClusterState state, long version, @Nullable Resolution previous) {
        Optional<ProjectMetadata> project = state.metadata().lookupProject(index);
        if (project.isEmpty()) {
            return Resolution.none(version);
        }
        IndexAbstraction abstraction = project.get().getIndicesLookup().get(index.getName());
        if (abstraction == null) {
            return Resolution.none(version);
        }
        DataStream dataStream = abstraction.getParentDataStream();
        if (dataStream == null || dataStream.isFailureStoreIndex(index.getName())) {
            return Resolution.none(version);
        }
        if (DerivedMetricsDestination.isDestination(dataStream.getName())) {
            // never derive metrics from the metrics we ourselves emit
            return Resolution.none(version);
        }
        DataStreamDerivedMetrics config = dataStream.getDataStreamOptions().derivedMetrics();
        if (config == null || config.enabled() == false) {
            return Resolution.none(version);
        }
        if (previous != null && config.equals(previous.config())) {
            // the configuration is unchanged, only the cluster state version moved on
            return new Resolution(version, previous.project(), previous.dataStream(), previous.config(), previous.compiled());
        }
        CompiledDerivedMetrics compiled = CompiledDerivedMetrics.compile(config);
        if (compiled.unsupportedMetrics().isEmpty() == false) {
            logger.warn(
                "derived metrics {} on data stream [{}] are configured but cannot be emitted",
                compiled.unsupportedMetrics(),
                dataStream.getName()
            );
        }
        return new Resolution(version, project.get().id(), dataStream.getName(), config, compiled);
    }
}
