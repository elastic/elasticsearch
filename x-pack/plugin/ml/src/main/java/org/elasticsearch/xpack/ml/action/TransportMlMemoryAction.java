/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction.Response.MlMemoryStats;
import org.elasticsearch.xpack.core.ml.action.TrainedModelCacheInfoAction;
import org.elasticsearch.xpack.core.ml.action.TrainedModelCacheInfoAction.Response.CacheInfo;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;
import static org.elasticsearch.xpack.ml.MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT;

public class TransportMlMemoryAction extends TransportMasterNodeAction<MlMemoryAction.Request, MlMemoryAction.Response> {

    private final Client client;
    private final MlMemoryTracker memoryTracker;

    @Inject
    public TransportMlMemoryAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        MlMemoryTracker memoryTracker
    ) {
        super(
            MlMemoryAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            MlMemoryAction.Request::new,
            indexNameExpressionResolver,
            MlMemoryAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.memoryTracker = memoryTracker;
    }

    @Override
    protected void masterOperation(
        Task task,
        MlMemoryAction.Request request,
        ClusterState state,
        ActionListener<MlMemoryAction.Response> listener
    ) throws Exception {

        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        // Resolve the node specification to some concrete nodes
        String[] nodeIds = state.nodes().resolveNodes(request.getNodeId());

        ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, task.getParentTaskId());

        ActionListener<NodesStatsResponse> nodeStatsListener = ActionListener.wrap(nodesStatsResponse -> {
            TrainedModelCacheInfoAction.Request trainedModelCacheInfoRequest = new TrainedModelCacheInfoAction.Request(
                nodesStatsResponse.getNodes().stream().map(NodeStats::getNode).toArray(DiscoveryNode[]::new)
            ).timeout(request.timeout());

            parentTaskClient.execute(
                TrainedModelCacheInfoAction.INSTANCE,
                trainedModelCacheInfoRequest,
                ActionListener.wrap(
                    trainedModelCacheInfoResponse -> handleResponses(
                        state,
                        clusterSettings,
                        nodesStatsResponse,
                        trainedModelCacheInfoResponse,
                        listener
                    ),
                    listener::onFailure
                )
            );
        }, listener::onFailure);

        // Next get node stats related to the OS and JVM
        ActionListener<Void> memoryTrackerRefreshListener = ActionListener.wrap(
            r -> parentTaskClient.admin()
                .cluster()
                .prepareNodesStats(nodeIds)
                .clear()
                .setOs(true)
                .setJvm(true)
                .setTimeout(request.timeout())
                .execute(nodeStatsListener),
            listener::onFailure
        );

        // If the memory tracker has never been refreshed, do that first
        if (memoryTracker.isEverRefreshed()) {
            memoryTrackerRefreshListener.onResponse(null);
        } else {
            memoryTracker.refresh(state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE), memoryTrackerRefreshListener);
        }
    }

    void handleResponses(
        ClusterState state,
        ClusterSettings clusterSettings,
        NodesStatsResponse nodesStatsResponse,
        TrainedModelCacheInfoAction.Response trainedModelCacheInfoResponse,
        ActionListener<MlMemoryAction.Response> listener
    ) {
        List<MlMemoryStats> nodeResponses = new ArrayList<>(nodesStatsResponse.getNodes().size());

        int maxOpenJobsPerNode = clusterSettings.get(MAX_OPEN_JOBS_PER_NODE);
        int maxMachineMemoryPercent = clusterSettings.get(MAX_MACHINE_MEMORY_PERCENT);
        boolean useAutoMachineMemoryPercent = clusterSettings.get(USE_AUTO_MACHINE_MEMORY_PERCENT);
        NodeLoadDetector nodeLoadDetector = new NodeLoadDetector(memoryTracker);
        Map<String, CacheInfo> cacheInfoByNode = trainedModelCacheInfoResponse.getNodesMap();
        List<FailedNodeException> failures = new ArrayList<>(nodesStatsResponse.failures());

        for (NodeStats nodeStats : nodesStatsResponse.getNodes()) {
            DiscoveryNode node = nodeStats.getNode();
            String nodeId = node.getId();
            // We only provide a response if both requests we issued to all nodes returned.
            // The loop is iterating successes of the node stats call with failures already
            // accumulated. This check adds failures of the trained model cache call that
            // happened on nodes where the node stats call succeeded.
            Optional<FailedNodeException> trainedModelCacheInfoFailure = trainedModelCacheInfoResponse.failures()
                .stream()
                .filter(e -> nodeId.equals(e.nodeId()))
                .findFirst();
            if (trainedModelCacheInfoFailure.isPresent()) {
                failures.add(trainedModelCacheInfoFailure.get());
                continue;
            }
            OsStats.Mem mem = nodeStats.getOs().getMem();
            ByteSizeValue mlMax;
            ByteSizeValue mlNativeCodeOverhead;
            ByteSizeValue mlAnomalyDetectors;
            ByteSizeValue mlDataFrameAnalytics;
            ByteSizeValue mlNativeInference;
            if (node.getRoles().contains(DiscoveryNodeRole.ML_ROLE)) {
                NodeLoad nodeLoad = nodeLoadDetector.detectNodeLoad(
                    state,
                    node,
                    maxOpenJobsPerNode,
                    maxMachineMemoryPercent,
                    useAutoMachineMemoryPercent
                );
                mlMax = ByteSizeValue.ofBytes(nodeLoad.getMaxMlMemory());
                mlNativeCodeOverhead = ByteSizeValue.ofBytes(nodeLoad.getAssignedNativeCodeOverheadMemory());
                mlAnomalyDetectors = ByteSizeValue.ofBytes(nodeLoad.getAssignedAnomalyDetectorMemory());
                mlDataFrameAnalytics = ByteSizeValue.ofBytes(nodeLoad.getAssignedDataFrameAnalyticsMemory());
                mlNativeInference = ByteSizeValue.ofBytes(nodeLoad.getAssignedNativeInferenceMemory());
            } else {
                mlMax = ByteSizeValue.ZERO;
                mlNativeCodeOverhead = ByteSizeValue.ZERO;
                mlAnomalyDetectors = ByteSizeValue.ZERO;
                mlDataFrameAnalytics = ByteSizeValue.ZERO;
                mlNativeInference = ByteSizeValue.ZERO;
            }
            ByteSizeValue jvmHeapMax = nodeStats.getJvm().getMem().getHeapMax();
            ByteSizeValue jvmInferenceMax;
            ByteSizeValue jvmInference;
            CacheInfo cacheInfoForNode = cacheInfoByNode.get(nodeId);
            if (cacheInfoForNode != null) {
                jvmInferenceMax = cacheInfoForNode.getJvmInferenceMax();
                jvmInference = cacheInfoForNode.getJvmInference();
            } else {
                jvmInferenceMax = ByteSizeValue.ZERO;
                jvmInference = ByteSizeValue.ZERO;
            }
            nodeResponses.add(
                new MlMemoryStats(
                    node,
                    mem.getTotal(),
                    mem.getAdjustedTotal(),
                    mlMax,
                    mlNativeCodeOverhead,
                    mlAnomalyDetectors,
                    mlDataFrameAnalytics,
                    mlNativeInference,
                    jvmHeapMax,
                    jvmInferenceMax,
                    jvmInference
                )
            );
        }

        listener.onResponse(new MlMemoryAction.Response(state.getClusterName(), nodeResponses, failures));
    }

    @Override
    protected ClusterBlockException checkBlock(MlMemoryAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
