/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.PipelineStore;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class PutPipelineTransportAction extends TransportMasterNodeAction<PutPipelineRequest, WritePipelineResponse> {

    private final PipelineStore pipelineStore;
    private final ClusterService clusterService;
    private final TransportNodesInfoAction nodesInfoAction;

    @Inject
    public PutPipelineTransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                      TransportService transportService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver, NodeService nodeService,
                                      TransportNodesInfoAction nodesInfoAction) {
        super(settings, PutPipelineAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, PutPipelineRequest::new);
        this.clusterService = clusterService;
        this.nodesInfoAction = nodesInfoAction;
        this.pipelineStore = nodeService.getIngestService().getPipelineStore();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected WritePipelineResponse newResponse() {
        return new WritePipelineResponse();
    }

    @Override
    protected void masterOperation(PutPipelineRequest request, ClusterState state, ActionListener<WritePipelineResponse> listener) throws Exception {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear();
        nodesInfoRequest.ingest(true);
        nodesInfoAction.execute(nodesInfoRequest, new ActionListener<NodesInfoResponse>() {
            @Override
            public void onResponse(NodesInfoResponse nodeInfos) {
                try {
                    Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
                    for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
                        ingestInfos.put(nodeInfo.getNode(), nodeInfo.getIngest());
                    }
                    pipelineStore.put(clusterService, ingestInfos, request, listener);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PutPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
