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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.features.FeatureService;
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
        FeatureService featureService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices
    ) {
        super(
            SimulateBulkAction.INSTANCE,
            SimulateBulkRequest::new,
            threadPool,
            transportService,
            clusterService,
            ingestService,
            featureService,
            client,
            actionFilters,
            indexNameExpressionResolver,
            indexingPressure,
            systemIndices,
            System::nanoTime
        );
    }

    /*
     * This overrides indexData in TransportBulkAction in order to _not_ actually create any indices or index any data. Instead, each
     * request gets a corresponding CREATE response, using information from the request.
     */
    @Override
    protected void createMissingIndicesAndIndexData(
        Task task,
        BulkRequest bulkRequest,
        String executorName,
        ActionListener<BulkResponse> listener,
        Map<String, Boolean> indicesToAutoCreate,
        Set<String> dataStreamsToRollover,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated,
        long startTime
    ) {
        assert bulkRequest instanceof SimulateBulkRequest;
        super.createMissingIndicesAndIndexData(task, bulkRequest, executorName, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                BulkItemResponse[] originalResponses = bulkResponse.getItems();
                final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());
                for (int i = 0; i < bulkRequest.requests.size(); i++) {
                    DocWriteRequest<?> request = bulkRequest.requests.get(i);
                    BulkItemResponse originalResponse = originalResponses[i];
                    Exception exception = originalResponse.isFailed() ? originalResponse.getFailure().getCause() : null;
                    assert request != null : "A request was unexpectedly set to null. Simulate action never set requests to null";
                    // This action is only every called with IndexRequests:
                    assert request instanceof IndexRequest : "expected IndexRequest but got " + request.getClass();
                    BulkItemResponse updatedResponse = BulkItemResponse.success(
                        originalResponse.getItemId(),
                        originalResponse.getOpType(),
                        new SimulateIndexResponse(
                            request.id(),
                            request.index(),
                            request.version(),
                            ((IndexRequest) request).source(),
                            ((IndexRequest) request).getContentType(),
                            ((IndexRequest) request).getExecutedPipelines(),
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
        }, indicesToAutoCreate, dataStreamsToRollover, indicesThatCannotBeCreated, startTime);
    }

    /*
     * This overrides TransportSimulateBulkAction's getIngestService to allow us to provide an IngestService that handles pipeline
     * substitutions defined in the request.
     */
    @Override
    protected IngestService getIngestService(BulkRequest request) {
        IngestService rawIngestService = super.getIngestService(request);
        return new SimulateIngestService(rawIngestService, request);
    }
}
