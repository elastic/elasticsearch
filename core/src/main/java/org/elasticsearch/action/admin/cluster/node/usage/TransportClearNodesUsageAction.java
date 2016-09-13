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

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.List;

public class TransportClearNodesUsageAction extends
        TransportNodesAction<ClearNodesUsageRequest, ClearNodesUsageResponse, TransportClearNodesUsageAction.ClearNodeUsageRequest, 
        ClearNodeUsageResponse> {

    private UsageService usageService;

    @Inject
    public TransportClearNodesUsageAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
            TransportService transportService, NodeService nodeService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, UsageService usageService) {
        super(settings, ClearNodesUsageAction.NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, ClearNodesUsageRequest::new, ClearNodeUsageRequest::new, ThreadPool.Names.MANAGEMENT,
                ClearNodeUsageResponse.class);
        this.usageService = usageService;
    }

    @Override
    protected ClearNodesUsageResponse newResponse(ClearNodesUsageRequest request, List<ClearNodeUsageResponse> responses,
            List<FailedNodeException> failures) {
        return new ClearNodesUsageResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ClearNodeUsageRequest newNodeRequest(String nodeId, ClearNodesUsageRequest request) {
        return new ClearNodeUsageRequest(nodeId, request);
    }

    @Override
    protected ClearNodeUsageResponse newNodeResponse() {
        return new ClearNodeUsageResponse();
    }

    @Override
    protected ClearNodeUsageResponse nodeOperation(ClearNodeUsageRequest nodeUsageRequest) {
        usageService.clear();
        return new ClearNodeUsageResponse();
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    public static class ClearNodeUsageRequest extends BaseNodeRequest {

        ClearNodesUsageRequest request;

        public ClearNodeUsageRequest() {
        }

        ClearNodeUsageRequest(String nodeId, ClearNodesUsageRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new ClearNodesUsageRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

}
