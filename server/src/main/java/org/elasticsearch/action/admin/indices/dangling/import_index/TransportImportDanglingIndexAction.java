/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.import_index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.dangling.find.FindDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.find.FindDanglingIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.find.NodeFindDanglingIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.gateway.LocalAllocateDangledIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the import of a dangling index. When handling a {@link ImportDanglingIndexAction},
 * this class first checks that such a dangling index exists. It then calls {@link LocalAllocateDangledIndices}
 * to perform the actual allocation.
 */
public class TransportImportDanglingIndexAction extends HandledTransportAction<ImportDanglingIndexRequest, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportImportDanglingIndexAction.class);

    private final LocalAllocateDangledIndices danglingIndexAllocator;
    private final NodeClient nodeClient;

    @Inject
    public TransportImportDanglingIndexAction(
        ActionFilters actionFilters,
        TransportService transportService,
        LocalAllocateDangledIndices danglingIndexAllocator,
        NodeClient nodeClient
    ) {
        super(ImportDanglingIndexAction.NAME, transportService, actionFilters, ImportDanglingIndexRequest::new);
        this.danglingIndexAllocator = danglingIndexAllocator;
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doExecute(
        Task task,
        ImportDanglingIndexRequest importRequest,
        ActionListener<AcknowledgedResponse> importListener
    ) {
        findDanglingIndex(importRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndexMetadata indexMetaDataToImport) {
                // This flag is checked at this point so that we always check that the supplied index UUID
                // does correspond to a dangling index.
                if (importRequest.isAcceptDataLoss() == false) {
                    importListener.onFailure(new IllegalArgumentException("accept_data_loss must be set to true"));
                    return;
                }

                String indexName = indexMetaDataToImport.getIndex().getName();
                String indexUUID = indexMetaDataToImport.getIndexUUID();

                danglingIndexAllocator.allocateDangled(List.of(indexMetaDataToImport), new ActionListener<>() {
                    @Override
                    public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse allocateDangledResponse) {
                        importListener.onResponse(AcknowledgedResponse.TRUE);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("Failed to import dangling index [" + indexName + "] [" + indexUUID + "]", e);
                        importListener.onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Failed to find dangling index [" + importRequest.getIndexUUID() + "]", e);
                importListener.onFailure(e);
            }
        });
    }

    private void findDanglingIndex(ImportDanglingIndexRequest request, ActionListener<IndexMetadata> listener) {
        final String indexUUID = request.getIndexUUID();
        this.nodeClient.execute(FindDanglingIndexAction.INSTANCE, new FindDanglingIndexRequest(indexUUID),
            listener.delegateFailure((l, response) -> {
                if (response.hasFailures()) {
                    final String nodeIds = response.failures().stream().map(FailedNodeException::nodeId).collect(Collectors.joining(","));
                    ElasticsearchException e = new ElasticsearchException("Failed to query nodes [" + nodeIds + "]");

                    for (FailedNodeException failure : response.failures()) {
                        logger.error("Failed to query node [" + failure.nodeId() + "]", failure);
                        e.addSuppressed(failure);
                    }

                    l.onFailure(e);
                    return;
                }

                final List<IndexMetadata> metaDataSortedByVersion = new ArrayList<>();
                for (NodeFindDanglingIndexResponse each : response.getNodes()) {
                    metaDataSortedByVersion.addAll(each.getDanglingIndexInfo());
                }
                metaDataSortedByVersion.sort(Comparator.comparingLong(IndexMetadata::getVersion));

                if (metaDataSortedByVersion.isEmpty()) {
                    l.onFailure(new IllegalArgumentException("No dangling index found for UUID [" + indexUUID + "]"));
                    return;
                }

                logger.debug(
                    "Metadata versions {} found for index UUID [{}], selecting the highest",
                    metaDataSortedByVersion.stream().map(IndexMetadata::getVersion).collect(Collectors.toList()),
                    indexUUID
                );

                l.onResponse(metaDataSortedByVersion.get(metaDataSortedByVersion.size() - 1));
        }));
    }
}
