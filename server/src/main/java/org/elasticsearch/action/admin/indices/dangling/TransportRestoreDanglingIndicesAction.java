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

package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.gateway.DanglingIndicesState;
import org.elasticsearch.gateway.LocalAllocateDangledIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class TransportRestoreDanglingIndicesAction extends HandledTransportAction<
    RestoreDanglingIndicesRequest,
    RestoreDanglingIndicesResponse> {

    private final TransportService transportService;
    private final LocalAllocateDangledIndices danglingIndexAllocator;

    @Inject
    public TransportRestoreDanglingIndicesAction(
        ActionFilters actionFilters,
        TransportService transportService,
        LocalAllocateDangledIndices danglingIndexAllocator
    ) {
        super(RestoreDanglingIndicesAction.NAME, transportService, actionFilters, RestoreDanglingIndicesRequest::new);
        this.transportService = transportService;
        this.danglingIndexAllocator = danglingIndexAllocator;
    }

    @Override
    protected void doExecute(Task task, RestoreDanglingIndicesRequest request, ActionListener<RestoreDanglingIndicesResponse> listener) {
        final Set<String> indexIds = new HashSet<>(Set.of(request.getIndexIds()));

        if (indexIds.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("No index UUIDs specified in request"));
        }

        final List<DanglingIndexInfo> danglingIndices = fetchDanglingIndices().actionGet().getDanglingIndices();

        final Set<String> availableIndices = danglingIndices.stream().map(DanglingIndexInfo::getIndexUUID).collect(Collectors.toSet());

        if (availableIndices.containsAll(indexIds) == false) {
            throw new IllegalArgumentException(
                "Dangling index list missing some specified UUIDs: [" + indexIds.removeAll(availableIndices) + "]"
            );
        }

        this.danglingIndexAllocator.allocateDangled(indexIds, listener);
    }

    private ActionFuture<ListDanglingIndicesResponse> fetchDanglingIndices() {
        final PlainActionFuture<ListDanglingIndicesResponse> future = PlainActionFuture.newFuture();

        this.transportService.sendRequest(
            this.transportService.getLocalNode(),
            ListDanglingIndicesAction.NAME,
            new ListDanglingIndicesRequest(),
            new TransportResponseHandler<ListDanglingIndicesResponse>() {
                @Override
                public void handleResponse(ListDanglingIndicesResponse response) {
                    future.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    future.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public ListDanglingIndicesResponse read(StreamInput in) throws IOException {
                    return new ListDanglingIndicesResponse(in);
                }
            }
        );

        return future;
    }

    // @Inject
    // public TransportRestoreDanglingIndicesAction(
    // ThreadPool threadPool,
    // ClusterService clusterService,
    // TransportService transportService,
    // ActionFilters actionFilters,
    // DanglingIndicesState danglingIndicesState,
    // LocalAllocateDangledIndices danglingIndexAllocator
    // ) {
    // super(
    // RestoreDanglingIndicesAction.NAME,
    // threadPool,
    // clusterService,
    // transportService,
    // actionFilters,
    // RestoreDanglingIndicesRequest::new,
    // NodeDanglingIndicesRequest::new,
    // ThreadPool.Names.MANAGEMENT,
    // NodeDanglingIndicesResponse.class
    // );
    // this.transportService = transportService;
    // this.danglingIndicesState = danglingIndicesState;
    // this.danglingIndexAllocator = danglingIndexAllocator;
    // }
    //
    // @Override
    // protected RestoreDanglingIndicesResponse newResponse(
    // RestoreDanglingIndicesRequest request,
    // List<NodeDanglingIndicesResponse> nodeDanglingIndicesResponses,
    // List<FailedNodeException> failures
    // ) {
    // if (failures.isEmpty()) {
    // // TODO: Do some kind of restore operation
    // this.danglingIndexAllocator.allocateDangled();
    // }
    //
    // return new RestoreDanglingIndicesResponse(clusterService.getClusterName(), nodeDanglingIndicesResponses, failures);
    // }
    //
    // @Override
    // protected NodeDanglingIndicesRequest newNodeRequest(RestoreDanglingIndicesRequest request) {
    // return new NodeDanglingIndicesRequest();
    // }
    //
    // @Override
    // protected NodeDanglingIndicesResponse newNodeResponse(StreamInput in) throws IOException {
    // return new NodeDanglingIndicesResponse(in);
    // }
    //
    // @Override
    // protected NodeDanglingIndicesResponse nodeOperation(NodeDanglingIndicesRequest request, Task task) {
    // final List<DanglingIndexInfo> indexInfo = new ArrayList<>();
    // final DiscoveryNode localNode = transportService.getLocalNode();
    //
    // for (IndexMetaData metaData : danglingIndicesState.getDanglingIndices().values()) {
    // DanglingIndexInfo info = new DanglingIndexInfo(localNode.getId(), metaData.getIndex().getName(), metaData.getIndexUUID());
    // indexInfo.add(info);
    // }
    //
    // return new NodeDanglingIndicesResponse(localNode, indexInfo);
    // }
}
