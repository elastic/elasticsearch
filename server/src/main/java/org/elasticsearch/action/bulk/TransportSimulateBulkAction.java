/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.SimulateIngestService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;

public class TransportSimulateBulkAction extends TransportBulkAction {
    @Inject
    public TransportSimulateBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices
    ) {
        super(
            SimulateBulkAction.NAME,
            threadPool,
            transportService,
            clusterService,
            ingestService,
            client,
            actionFilters,
            indexNameExpressionResolver,
            indexingPressure,
            systemIndices,
            System::nanoTime
        );
    }

    @Override
    protected void indexData(
        Task task,
        BulkRequest bulkRequest,
        String executorName,
        ActionListener<BulkResponse> listener,
        AtomicArray<BulkItemResponse> responses,
        Set<String> autoCreateIndices,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated,
        long startTime
    ) {
        super.indexData(task, bulkRequest, executorName, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                BulkItemResponse[] originalResponses = response.getItems();
                for (int i = 0; i < bulkRequest.requests.size(); i++) {
                    DocWriteRequest<?> request = bulkRequest.requests.get(i);
                    BulkItemResponse originalResponse = originalResponses[i];
                    Exception exception = originalResponse.isFailed() ? originalResponse.getFailure().getCause() : null;
                    BulkItemResponse updatedResponse = BulkItemResponse.success(
                        originalResponse.getItemId(),
                        originalResponse.getOpType(),
                        new SimulateIndexResponse(
                            request.index(),
                            ((IndexRequest) request).source(),
                            ((IndexRequest) request).getContentType(),
                            ((IndexRequest) request).getPipelines(),
                            exception
                        )
                    );
                    responses.set(i, updatedResponse);
                }
                listener.onResponse(
                    new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTime))
                );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }, responses, autoCreateIndices, indicesThatCannotBeCreated, startTime);
    }

    @Override
    protected IngestService getIngestService(BulkRequest request) {
        IngestService rawIngestService = super.getIngestService(request);
        return new SimulateIngestService(rawIngestService, request);
    }

    @Override
    void createIndex(String index, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
        listener.onResponse(null);
    }
}
