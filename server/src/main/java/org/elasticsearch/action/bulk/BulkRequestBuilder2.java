/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder2;
import org.elasticsearch.action.update.UpdateRequestBuilder2;
import org.elasticsearch.index.reindex.ReindexRequestBuilder2;

import java.util.ArrayList;
import java.util.List;

public class BulkRequestBuilder2 implements RequestBuilder<BulkRequest> {
    private final List<RequestBuilder<?>> requestBuilders = new ArrayList<>();

    /**
     * This creates a new IndexRequestBuilder2, and adds it to the list of requests that will be part of this BulkRequest when #buildRequest
     * is called.
     * @param index The name of the index for the index request
     * @return The new index request builder
     */
    public IndexRequestBuilder2 indexRequestBuilder(String index) {
        IndexRequestBuilder2 indexRequestBuilder = new IndexRequestBuilder2(index);
        requestBuilders.add(indexRequestBuilder);
        return indexRequestBuilder;
    }

    /**
     * This creates a new UpdateRequestBuilder2, and adds it to the list of requests that will be part of this BulkRequest when
     * #buildRequest is called.
     * @param index The name of the index for the update request
     * @param id The id of the document to be updated by the update request
     * @return The new update request builder
     */
    public UpdateRequestBuilder2 updateRequestBuilder(String index, String id) {
        UpdateRequestBuilder2 updateRequestBuilder = new UpdateRequestBuilder2(index, id);
        requestBuilders.add(updateRequestBuilder);
        return updateRequestBuilder;
    }

    /**
     * This creates a new ReindexRequestBuilder2, and adds it to the list of requests that will be part of this BulkRequest when
     * #buildRequest is called.
     * @return The new reindex request builder
     */
    public ReindexRequestBuilder2 reindexRequestBuilder() {
        ReindexRequestBuilder2 reindexRequestBuilder = new ReindexRequestBuilder2();
        requestBuilders.add(reindexRequestBuilder);
        return reindexRequestBuilder;
    }

    @Override
    public BulkRequest buildRequest() {
        BulkRequest bulkRequest = new BulkRequest();
        try {
            for (RequestBuilder<?> requestBuilder : requestBuilders) {
                ActionRequest request = requestBuilder.buildRequest();
                try {
                    bulkRequest.add((DocWriteRequest<?>) request);
                } finally {
                    request.decRef();
                }
            }
            return bulkRequest;
        } catch (Exception e) {
            bulkRequest.decRef();
            throw e;
        }
    }
}
