/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.AtomicBoolean;

public class RequestMemory {

    public static RequestMemory NO_OP = new RequestMemory();

    private final Releasable networkMemory;
    private final AtomicBoolean needToReleaseNetworkMemory;

    private RequestMemory() {
        this.networkMemory = Releasable.NO_OP;
        this.needToReleaseNetworkMemory = new AtomicBoolean(false);
    }

    public RequestMemory(Releasable networkMemory) {
        this.networkMemory = networkMemory;
        this.needToReleaseNetworkMemory = new AtomicBoolean(true);
    }

    public boolean needToReleaseNetworkMemory() {
        return needToReleaseNetworkMemory.get();
    }

    public void releaseNetworkMemory() {
        if (needToReleaseNetworkMemory.compareAndSet(true, false)) {
            networkMemory.close();
        }
    }

    private static void updateRequests(BytesReference bytesReference, BulkShardRequest bulkShardRequest) {
        int offset = 0;
        for (BulkItemRequest item : bulkShardRequest.items()) {
            DocWriteRequest<?> request = item.request();
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                int length = indexRequest.source().length();
                indexRequest.source(bytesReference.slice(offset, length), indexRequest.getContentType());
                offset += length;
            } else if (request instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) request;
                if (updateRequest.upsertRequest() != null) {
                    IndexRequest indexRequest = updateRequest.upsertRequest();
                    int length = indexRequest.source().length();
                    indexRequest.source(bytesReference.slice(offset, length), indexRequest.getContentType());
                    offset += length;
                }
                if (updateRequest.doc() != null) {
                    IndexRequest indexRequest = updateRequest.doc();
                    int length = indexRequest.source().length();
                    indexRequest.source(bytesReference.slice(offset, length), indexRequest.getContentType());
                    offset += length;
                }
            }
        }
    }
}
