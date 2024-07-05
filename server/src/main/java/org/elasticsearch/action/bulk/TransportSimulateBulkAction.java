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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.SimulateIngestService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/**
 * This action simulates bulk indexing data. Pipelines are executed for all indices that the request routes to, but no data is actually
 * indexed and no state is changed. Unlike TransportBulkAction, this does not push the work out to the nodes where the shards live (since
 * shards are not actually modified).
 */
public class TransportSimulateBulkAction extends TransportAbstractBulkAction {

    @Inject
    public TransportSimulateBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices
    ) {
        super(
            SimulateBulkAction.INSTANCE,
            transportService,
            actionFilters,
            SimulateBulkRequest::new,
            threadPool,
            clusterService,
            ingestService,
            indexingPressure,
            systemIndices,
            System::nanoTime
        );
    }

    @Override
    protected void doInternalExecute(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> listener,
        long relativeStartTime
    ) {
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            DocWriteRequest<?> docRequest = bulkRequest.requests.get(i);
            assert docRequest instanceof IndexRequest : "TransportSimulateBulkAction should only ever be called with IndexRequests";
            IndexRequest request = (IndexRequest) docRequest;

            responses.set(
                i,
                BulkItemResponse.success(
                    0,
                    DocWriteRequest.OpType.CREATE,
                    new SimulateIndexResponse(
                        request.id(),
                        request.index(),
                        request.version(),
                        request.source(),
                        request.getContentType(),
                        request.getExecutedPipelines(),
                        null
                    )
                )
            );
        }
        listener.onResponse(
            new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(relativeStartTime))
        );
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

    @Override
    protected boolean shouldStoreFailure(String indexName, Metadata metadata, long time) {
        // A simulate bulk request should not change any persistent state in the system, so we never write to the failure store
        return false;
    }
}
