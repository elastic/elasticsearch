/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationStatsService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

public class TransportGetAllocationStatsAction extends TransportMasterNodeReadAction<
    TransportGetAllocationStatsAction.Request,
    TransportGetAllocationStatsAction.Response> {

    public static final ActionType<TransportGetAllocationStatsAction.Response> TYPE = new ActionType<>("cluster:monitor/allocation/stats");

    private final AllocationStatsService allocationStatsService;
    private final DiskThresholdSettings diskThresholdSettings;
    private final FeatureService featureService;

    @Inject
    public TransportGetAllocationStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationStatsService allocationStatsService,
        FeatureService featureService
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            TransportGetAllocationStatsAction.Request::new,
            indexNameExpressionResolver,
            TransportGetAllocationStatsAction.Response::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.allocationStatsService = allocationStatsService;
        this.diskThresholdSettings = new DiskThresholdSettings(clusterService.getSettings(), clusterService.getClusterSettings());
        this.featureService = featureService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (clusterService.state().getMinTransportVersion().before(TransportVersions.V_8_14_0)) {
            // The action is not available before ALLOCATION_STATS
            listener.onResponse(new Response(Map.of(), null));
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        listener.onResponse(
            new Response(
                request.metrics().contains(Metric.ALLOCATIONS) ? allocationStatsService.stats() : Map.of(),
                request.metrics().contains(Metric.FS)
                    && featureService.clusterHasFeature(clusterService.state(), AllocationStatsFeatures.INCLUDE_DISK_THRESHOLD_SETTINGS)
                        ? diskThresholdSettings
                        : null
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final EnumSet<Metric> metrics;

        public Request(TimeValue masterNodeTimeout, TaskId parentTaskId, EnumSet<Metric> metrics) {
            super(masterNodeTimeout);
            setParentTask(parentTaskId);
            this.metrics = metrics;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.metrics = in.getTransportVersion().onOrAfter(TransportVersions.MASTER_NODE_METRICS)
                ? in.readEnumSet(Metric.class)
                : EnumSet.of(Metric.ALLOCATIONS, Metric.FS);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0);
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.MASTER_NODE_METRICS)) {
                out.writeEnumSet(metrics);
            }
        }

        public EnumSet<Metric> metrics() {
            return metrics;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {

        private final Map<String, NodeAllocationStats> nodeAllocationStats;
        @Nullable // for bwc
        private final DiskThresholdSettings diskThresholdSettings;

        public Response(Map<String, NodeAllocationStats> nodeAllocationStats, DiskThresholdSettings diskThresholdSettings) {
            this.nodeAllocationStats = nodeAllocationStats;
            this.diskThresholdSettings = diskThresholdSettings;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.nodeAllocationStats = in.readImmutableMap(StreamInput::readString, NodeAllocationStats::new);
            if (in.getTransportVersion().onOrAfter(TransportVersions.WATERMARK_THRESHOLDS_STATS)) {
                this.diskThresholdSettings = in.readOptionalWriteable(DiskThresholdSettings::readFrom);
            } else {
                this.diskThresholdSettings = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(nodeAllocationStats, StreamOutput::writeString, StreamOutput::writeWriteable);
            if (out.getTransportVersion().onOrAfter(TransportVersions.WATERMARK_THRESHOLDS_STATS)) {
                out.writeOptionalWriteable(diskThresholdSettings);
            } else {
                assert diskThresholdSettings == null;
            }
        }

        public Map<String, NodeAllocationStats> getNodeAllocationStats() {
            return nodeAllocationStats;
        }

        @Nullable // for bwc
        public DiskThresholdSettings getDiskThresholdSettings() {
            return diskThresholdSettings;
        }
    }
}
