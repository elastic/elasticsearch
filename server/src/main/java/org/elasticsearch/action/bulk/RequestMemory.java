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
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RequestMemory {

    public static RequestMemory NO_OP = new RequestMemory();

    private final Releasable networkMemory;
    private final AtomicBoolean needToReleaseNetworkMemory;

    private RequestMemory() {
        this.networkMemory = Releasable.NO_OP;
        this.needToReleaseNetworkMemory = new AtomicBoolean(false);
    }

    public RequestMemory(Releasable coordinatingMemory) {
        this.networkMemory = coordinatingMemory;
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

    public static ReleasableBytesReference copyBytesToNewReference(
        RecyclerBytesStreamOutput streamOutput,
        BulkShardRequest bulkShardRequest
    ) {
        ReleasableBytesReference bytesReference = null;
        boolean success = false;
        try {
            for (BulkItemRequest item : bulkShardRequest.items()) {
                DocWriteRequest<?> request = item.request();
                if (request instanceof IndexRequest) {
                    ((IndexRequest) request).source().writeTo(streamOutput);
                } else if (request instanceof UpdateRequest) {
                    UpdateRequest updateRequest = (UpdateRequest) request;
                    if (updateRequest.upsertRequest() != null) {
                        updateRequest.upsertRequest().source().writeTo(streamOutput);
                    }
                    if (updateRequest.doc() != null) {
                        updateRequest.doc().source().writeTo(streamOutput);
                    }
                }
            }
            bytesReference = streamOutput.retainBytesAndTruncateStream();
            updateRequests(bytesReference, bulkShardRequest);
            success = true;
            return bytesReference;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                streamOutput.retainBytesAndTruncateStream().close();
                Releasables.close(bytesReference);
            }

        }
    }

    private static void updateRequests(BytesReference bytesReference, BulkShardRequest bulkShardRequest) {
        int offset = 0;
        for (BulkItemRequest item : bulkShardRequest.items()) {
            DocWriteRequest<?> request = item.request();
            int length = 0;
            if (request instanceof IndexRequest) {
                length = ((IndexRequest) request).source().length();
                ((IndexRequest) request).source(bytesReference.slice(offset, length));
            } else if (request instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) request;
                if (updateRequest.upsertRequest() != null) {
                    length = updateRequest.upsertRequest().source().length();
                    updateRequest.upsertRequest().source(bytesReference.slice(offset, length));
                }
                if (updateRequest.doc() != null) {
                    length = updateRequest.doc().source().length();
                    updateRequest.doc().source(bytesReference.slice(offset, length));
                }
            }
            offset += length;
        }
    }
}
