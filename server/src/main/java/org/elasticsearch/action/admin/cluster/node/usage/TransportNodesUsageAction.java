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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.List;

public class TransportNodesUsageAction
        extends TransportNodesAction<NodesUsageRequest, NodesUsageResponse, TransportNodesUsageAction.NodeUsageRequest, NodeUsage> {

    private UsageService usageService;

    @Inject
    public TransportNodesUsageAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                     ActionFilters actionFilters, UsageService usageService) {
        super(NodesUsageAction.NAME, threadPool, clusterService, transportService, actionFilters,
            NodesUsageRequest::new, NodeUsageRequest::new, ThreadPool.Names.MANAGEMENT, NodeUsage.class);
        this.usageService = usageService;
    }

    @Override
    protected NodesUsageResponse newResponse(NodesUsageRequest request, List<NodeUsage> responses, List<FailedNodeException> failures) {
        return new NodesUsageResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeUsageRequest newNodeRequest(NodesUsageRequest request) {
        return new NodeUsageRequest(request);
    }

    @Override
    protected NodeUsage newNodeResponse(StreamInput in) throws IOException {
        return new NodeUsage(in);
    }

    @Override
    protected NodeUsage nodeOperation(NodeUsageRequest nodeUsageRequest, Task task) {
        NodesUsageRequest request = nodeUsageRequest.request;
        return usageService.getUsageStats(clusterService.localNode(), request.restActions());
    }

    public static class NodeUsageRequest extends BaseNodeRequest {

        NodesUsageRequest request;

        public NodeUsageRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesUsageRequest(in);
        }

        NodeUsageRequest(NodesUsageRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
