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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.gateway.LocalAllocateDangledIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.util.CollectionUtils.filter;
import static org.elasticsearch.common.util.CollectionUtils.map;

public class TransportRestoreDanglingIndexAction extends HandledTransportAction<RestoreDanglingIndexRequest, RestoreDanglingIndexResponse> {

    private final TransportService transportService;
    private final LocalAllocateDangledIndices danglingIndexAllocator;

    @Inject
    public TransportRestoreDanglingIndexAction(
        ActionFilters actionFilters,
        TransportService transportService,
        LocalAllocateDangledIndices danglingIndexAllocator
    ) {
        super(RestoreDanglingIndexAction.NAME, transportService, actionFilters, RestoreDanglingIndexRequest::new);
        this.transportService = transportService;
        this.danglingIndexAllocator = danglingIndexAllocator;
    }

    @Override
    protected void doExecute(Task task, RestoreDanglingIndexRequest request, ActionListener<RestoreDanglingIndexResponse> listener) {
        IndexMetaData indexMetaDataToRestore;
        try {
            indexMetaDataToRestore = getIndexMetaDataToRestore(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        this.danglingIndexAllocator.allocateDangled(List.of(indexMetaDataToRestore), new ActionListener<>() {
            @Override
            public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse allocateDangledResponse) {
                listener.onResponse(new RestoreDanglingIndexResponse());
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private IndexMetaData getIndexMetaDataToRestore(RestoreDanglingIndexRequest request) {
        String indexUuid = request.getIndexUuid();
        String nodeId = request.getNodeId();

        if (indexUuid == null || indexUuid.isEmpty()) {
            throw new IllegalArgumentException("No index UUID specified in request");
        }

        final List<IndexMetaData> allMetaData = new ArrayList<>();
        for (NodeDanglingIndicesResponse response : fetchDanglingIndices().actionGet().getNodes()) {
            allMetaData.addAll(response.getDanglingIndices());
        }

        List<IndexMetaData> matchingMetaData = new ArrayList<>();

        for (NodeDanglingIndicesResponse response : fetchDanglingIndices().actionGet().getNodes()) {
            for (IndexMetaData danglingIndex : response.getDanglingIndices()) {
                if (danglingIndex.getIndexUUID().equals(indexUuid) && (nodeId == null || response.getNode().getId().equals(nodeId))) {
                    matchingMetaData.add(danglingIndex);
                }
            }
        }

        if (matchingMetaData.isEmpty()) {
            throw new IllegalArgumentException("No dangling index found for UUID [" + indexUuid + "]");
        }

        if (matchingMetaData.size() > 1) {
            throw new IllegalArgumentException("Multiple nodes contain dangling index [" + indexUuid + "]. "
                + "Specify a node ID to import a specific dangling index.");
        }

        return matchingMetaData.get(0);
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
}
