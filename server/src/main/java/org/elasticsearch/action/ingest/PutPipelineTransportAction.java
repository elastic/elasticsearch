/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.IngestService.INGEST_ORIGIN;

public class PutPipelineTransportAction extends AcknowledgedTransportMasterNodeAction<PutPipelineRequest> {

    private final IngestService ingestService;
    private final OriginSettingClient client;
    private static final Logger logger = LogManager.getLogger(PutPipelineTransportAction.class);

    @Inject
    public PutPipelineTransportAction(ThreadPool threadPool, TransportService transportService,
        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
        IngestService ingestService, NodeClient client) {
        super(
            PutPipelineAction.NAME, transportService, ingestService.getClusterService(),
            threadPool, actionFilters, PutPipelineRequest::new, indexNameExpressionResolver, ThreadPool.Names.SAME
        );
        // This client is only used to perform an internal implementation detail,
        // so uses an internal origin context rather than the user context
        this.client = new OriginSettingClient(client, INGEST_ORIGIN);
        this.ingestService = ingestService;
    }

    @Override
    protected void masterOperation(Task task, PutPipelineRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
            throws Exception {
        if (state.getNodes().getMinNodeVersion().before(Version.V_8_0_0)) {
            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
            if (pipelineConfig.containsKey(Pipeline.META_KEY)) {
                throw new IllegalArgumentException("All nodes in the cluster must be at " + Version.V_8_0_0 + " or higher in order to" +
                    "create a pipeline with the " + Pipeline.META_KEY + " field. The minimum version currently in the cluster is " +
                    state.getNodes().getMinNodeVersion());
            }
        } else {
            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
            if (pipelineConfig.containsKey(Pipeline.META_KEY)) {
                logger.info("Found _meta entry");
            } else {
                logger.info("Did not find _meta entry");
            }
        }
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear()
            .addMetric(NodesInfoRequest.Metric.INGEST.metricName());
        client.admin().cluster().nodesInfo(nodesInfoRequest, ActionListener.wrap(nodeInfos -> {
            Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
            for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
                ingestInfos.put(nodeInfo.getNode(), nodeInfo.getInfo(IngestInfo.class));
            }
            ingestService.putPipeline(ingestInfos, request, listener);
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(PutPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
