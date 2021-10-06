/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

public class BulkItemRequest implements Writeable, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class);

    private int id;
    private DocWriteRequest<?> request;
    private volatile BulkItemResponse primaryResponse;

    BulkItemRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        id = in.readVInt();
        request = DocWriteRequest.readDocumentRequest(shardId, in);
        if (in.readBoolean()) {
            if (shardId == null) {
                primaryResponse = new BulkItemResponse(in);
            } else {
                primaryResponse = new BulkItemResponse(shardId, in);
            }
        }
    }

    // NOTE: public for testing only
    public BulkItemRequest(int id, DocWriteRequest<?> request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public DocWriteRequest<?> request() {
        return request;
    }

    public String index() {
        assert request.indices().length == 1;
        return request.indices()[0];
    }

    BulkItemResponse getPrimaryResponse() {
        return primaryResponse;
    }

    void setPrimaryResponse(BulkItemResponse primaryResponse) {
        this.primaryResponse = primaryResponse;
    }

    /**
     * Abort this request, and store a {@link org.elasticsearch.action.bulk.BulkItemResponse.Failure} response.
     *
     * @param index The concrete index that was resolved for this request
     * @param cause The cause of the rejection (may not be null)
     * @throws IllegalStateException If a response already exists for this request
     */
    public void abort(String index, Exception cause) {
        if (primaryResponse == null) {
            final BulkItemResponse.Failure failure = new BulkItemResponse.Failure(index, request.id(), Objects.requireNonNull(cause), true);
            setPrimaryResponse(BulkItemResponse.failure(id, request.opType(), failure));
        } else {
            assert primaryResponse.isFailed() && primaryResponse.getFailure().isAborted()
                    : "response [" + Strings.toString(primaryResponse) + "]; cause [" + cause + "]";
            if (primaryResponse.isFailed() && primaryResponse.getFailure().isAborted()) {
                primaryResponse.getFailure().getCause().addSuppressed(cause);
            } else {
                throw new IllegalStateException(
                        "aborting item that with response [" + primaryResponse + "] that was previously processed", cause);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        DocWriteRequest.writeDocumentRequest(out, request);
        out.writeOptionalWriteable(primaryResponse);
    }

    public void writeThin(StreamOutput out) throws IOException {
        out.writeVInt(id);
        DocWriteRequest.writeDocumentRequestThin(out, request);
        out.writeOptionalWriteable(primaryResponse == null ? null : primaryResponse::writeThin);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + request.ramBytesUsed();
    }
}
