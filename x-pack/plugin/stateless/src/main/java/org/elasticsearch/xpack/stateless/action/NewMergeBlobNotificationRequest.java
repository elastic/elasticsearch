/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.unpromotable.BroadcastUnpromotableRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.stateless.commits.Blob;
import org.elasticsearch.xpack.stateless.commits.MergeBlob;
import org.elasticsearch.xpack.stateless.commits.TimestampFieldValueRange;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/// A request sent from an indexing shard to all its search shards, including relocation targets, once a large merged segment is uploaded
/// as a [MergeBlob] and before the merge is published, so that the search shards can prefetch the merge blob.
///
/// Merges which are small are uploaded within a BCC, don't get a merge blob, and do not send this request.
public class NewMergeBlobNotificationRequest extends BroadcastUnpromotableRequest {

    private final MergeBlob mergeBlob;
    private final long mergeBlobLengthBytes;
    /// Merged segment timestamp range, if it has one, for informing warming ratio and cache entry timestamp values used for cache pinning.
    private @Nullable final TimestampFieldValueRange timestampFieldValueRange;

    public NewMergeBlobNotificationRequest(
        final IndexShardRoutingTable indexShardRoutingTable,
        final MergeBlob mergeBlob,
        final long mergeBlobLengthBytes,
        @Nullable final TimestampFieldValueRange timestampFieldValueRange
    ) {
        super(indexShardRoutingTable, false); // failShardOnError=false. Prefetch is best-effort.
        this.mergeBlob = Objects.requireNonNull(mergeBlob);
        this.mergeBlobLengthBytes = mergeBlobLengthBytes;
        this.timestampFieldValueRange = timestampFieldValueRange;
    }

    public NewMergeBlobNotificationRequest(final StreamInput in) throws IOException {
        super(in);
        mergeBlob = Blob.readFrom(in, MergeBlob.class);
        mergeBlobLengthBytes = in.readVLong();
        timestampFieldValueRange = in.readOptionalWriteable(TimestampFieldValueRange::new);
    }

    public MergeBlob getMergeBlob() {
        return mergeBlob;
    }

    public long getMergeBlobLengthBytes() {
        return mergeBlobLengthBytes;
    }

    @Nullable
    public TimestampFieldValueRange getTimestampFieldValueRange() {
        return timestampFieldValueRange;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (mergeBlobLengthBytes <= 0L) {
            validationException = addValidationError(
                "merge blob length [" + mergeBlobLengthBytes + "] must be positive",
                validationException
            );
        }
        return validationException;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        mergeBlob.writeTo(out);
        out.writeVLong(mergeBlobLengthBytes);
        out.writeOptionalWriteable(timestampFieldValueRange);
    }

    @Override
    public String toString() {
        return "NewMergeBlobNotificationRequest{"
            + "shardId="
            + shardId
            + ", mergeBlob="
            + mergeBlob
            + ", mergeBlobLengthBytes="
            + mergeBlobLengthBytes
            + ", timestampFieldValueRange="
            + timestampFieldValueRange
            + '}';
    }
}
