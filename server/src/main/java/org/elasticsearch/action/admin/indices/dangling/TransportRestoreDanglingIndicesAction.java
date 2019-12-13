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
        final Set<String> specifiedIndexIdsToRestore = Set.of(request.getIndexIds());

        List<IndexMetaData> indexMetaDataToRestore;
        try {
            indexMetaDataToRestore = getIndexMetaDataToRestore(specifiedIndexIdsToRestore);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        this.danglingIndexAllocator.allocateDangled(indexMetaDataToRestore, new ActionListener<>() {
            @Override
            public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse allocateDangledResponse) {
                listener.onResponse(new RestoreDanglingIndicesResponse());
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private List<IndexMetaData> getIndexMetaDataToRestore(Set<String> specifiedIndexIdsToRestore) {
        if (specifiedIndexIdsToRestore.isEmpty()) {
            throw new IllegalArgumentException("No index UUIDs specified in request");
        }

        final List<IndexMetaData> allMetaData = new ArrayList<>();
        for (NodeDanglingIndicesResponse response : fetchDanglingIndices().actionGet().getNodes()) {
            allMetaData.addAll(response.getDanglingIndices());
        }

        final List<String> allMetaDataIds = map(allMetaData, IndexMetaData::getIndexUUID);

        final List<String> unknownIndexIds = new ArrayList<>(specifiedIndexIdsToRestore);
        unknownIndexIds.removeAll(allMetaDataIds);

        if (unknownIndexIds.isEmpty() == false) {
            throw new IllegalArgumentException("Dangling index list missing some specified UUIDs: " + unknownIndexIds);
        }

        return filter(allMetaData, each -> specifiedIndexIdsToRestore.contains(each.getIndexUUID()));
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
