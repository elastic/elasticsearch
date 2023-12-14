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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.LeakTracker;

import java.io.IOException;
import java.util.Objects;

public class BulkItemRequest implements Writeable, Accountable, RefCounted, Releasable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class);

    private final int id;
    private final DocWriteRequest<?> request;
    private volatile BulkItemResponse primaryResponse;
    private final RefCounted refCounted;

    BulkItemRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        id = in.readVInt();
        request = DocWriteRequest.readDocumentRequest(shardId, in);
        this.refCounted = LeakTracker.wrap(new BulkItemRequestRefCounted());
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
        this.refCounted = LeakTracker.wrap(new BulkItemRequestRefCounted());
        if (this.request instanceof RefCounted refCountedRequest) {
            refCountedRequest.incRef();
        }
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

    // public for tests
    public BulkItemResponse getPrimaryResponse() {
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
                    "aborting item that with response [" + primaryResponse + "] that was previously processed",
                    cause
                );
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

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        assert refCounted.hasReferences() : "Attempt to decRef BulkItemRequest that is already closed";
        boolean droppedToZero = refCounted.decRef();
        if (droppedToZero && this.request instanceof RefCounted refCountedRequest) {
            refCountedRequest.decRef();
        }
        return droppedToZero;
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }

    @Override
    public void close() {
        boolean closed = decRef();
        assert closed : "Attempt to close BulkItemRequest but it still has references";
    }

    private static class BulkItemRequestRefCounted extends AbstractRefCounted {
        @Override
        protected void closeInternal() {
            // nothing to close
        }
    }

}
