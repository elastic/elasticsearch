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

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodesHotThreadsAction extends TransportNodesAction<NodesHotThreadsRequest,
                                                                         NodesHotThreadsResponse,
                                                                         TransportNodesHotThreadsAction.NodeRequest,
                                                                         NodeHotThreads> {

    @Inject
    public TransportNodesHotThreadsAction(ThreadPool threadPool, ClusterService clusterService,
                                          TransportService transportService, ActionFilters actionFilters) {
        super(NodesHotThreadsAction.NAME, threadPool, clusterService, transportService, actionFilters,
            NodesHotThreadsRequest::new, NodeRequest::new, ThreadPool.Names.GENERIC, NodeHotThreads.class);
    }

    @Override
    protected NodesHotThreadsResponse newResponse(NodesHotThreadsRequest request,
                                                  List<NodeHotThreads> responses, List<FailedNodeException> failures) {
        return new NodesHotThreadsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(NodesHotThreadsRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeHotThreads newNodeResponse(StreamInput in) throws IOException {
        return new NodeHotThreads(in);
    }

    @Override
    protected NodeHotThreads nodeOperation(NodeRequest request, Task task) {
        HotThreads hotThreads = new HotThreads()
                .busiestThreads(request.request.threads)
                .type(request.request.type)
                .interval(request.request.interval)
                .threadElementsSnapshotCount(request.request.snapshots)
                .ignoreIdleThreads(request.request.ignoreIdleThreads);
        try {
            return new NodeHotThreads(clusterService.localNode(), hotThreads.detect());
        } catch (Exception e) {
            throw new ElasticsearchException("failed to detect hot threads", e);
        }
    }

    public static class NodeRequest extends TransportRequest {

        NodesHotThreadsRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesHotThreadsRequest(in);
        }

        NodeRequest(NodesHotThreadsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
