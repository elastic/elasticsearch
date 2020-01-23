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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
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

/**
 * Implements the import of a dangling index. When handling a {@link ImportDanglingIndexAction},
 * this class first checks that such a dangling index exists. It then calls {@link LocalAllocateDangledIndices}
 * to perform the actual allocation.
 */
public class TransportImportDanglingIndexAction extends HandledTransportAction<ImportDanglingIndexRequest, ImportDanglingIndexResponse> {
    private static final Logger logger = LogManager.getLogger(TransportImportDanglingIndexAction.class);

    private final TransportService transportService;
    private final LocalAllocateDangledIndices danglingIndexAllocator;

    @Inject
    public TransportImportDanglingIndexAction(
        ActionFilters actionFilters,
        TransportService transportService,
        LocalAllocateDangledIndices danglingIndexAllocator
    ) {
        super(ImportDanglingIndexAction.NAME, transportService, actionFilters, ImportDanglingIndexRequest::new);
        this.transportService = transportService;
        this.danglingIndexAllocator = danglingIndexAllocator;
    }

    @Override
    protected void doExecute(
        Task task,
        ImportDanglingIndexRequest importRequest,
        ActionListener<ImportDanglingIndexResponse> importListener
    ) {
        fetchDanglingIndices(importRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndexMetaData indexMetaDataToImport) {
                // This flag is checked at this point so that we always check that the supplied index UUID
                // does correspond to a dangling index.
                if (importRequest.isAcceptDataLoss() == false) {
                    throw new IllegalArgumentException("accept_data_loss must be set to true");
                }

                danglingIndexAllocator.allocateDangled(List.of(indexMetaDataToImport), new ActionListener<>() {
                    @Override
                    public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse allocateDangledResponse) {
                        importListener.onResponse(new ImportDanglingIndexResponse());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("Failed to import dangling index [" + indexMetaDataToImport.getIndexUUID() + "]", e);
                        importListener.onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Failed to list dangling indices", e);
                importListener.onFailure(e);
            }
        });
    }

    private void fetchDanglingIndices(ImportDanglingIndexRequest request, ActionListener<IndexMetaData> listener) {
        this.transportService.sendRequest(
            this.transportService.getLocalNode(),
            ListDanglingIndicesAction.NAME,
            new ListDanglingIndicesRequest(),
            new TransportResponseHandler<ListDanglingIndicesResponse>() {

                @Override
                public void handleResponse(ListDanglingIndicesResponse response) {
                    String indexUUID = request.getIndexUUID();
                    String nodeId = request.getNodeId();

                    List<IndexMetaData> matchingMetaData = new ArrayList<>();

                    for (NodeDanglingIndicesResponse nodeResponse : response.getNodes()) {
                        for (IndexMetaData danglingIndex : nodeResponse.getDanglingIndices()) {
                            if (danglingIndex.getIndexUUID().equals(indexUUID)
                                && (nodeId == null || nodeResponse.getNode().getId().equals(nodeId))) {
                                matchingMetaData.add(danglingIndex);
                            }
                        }
                    }

                    if (matchingMetaData.isEmpty()) {
                        listener.onFailure(new IllegalArgumentException("No dangling index found for UUID [" + indexUUID + "]"));
                    }

                    if (matchingMetaData.size() > 1) {
                        listener.onFailure(
                            new IllegalArgumentException(
                                "Multiple nodes contain dangling index ["
                                    + indexUUID
                                    + "]. Specify a node ID to import a specific dangling index."
                            )
                        );
                    }

                    listener.onResponse(matchingMetaData.get(0));
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
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
    }
}
