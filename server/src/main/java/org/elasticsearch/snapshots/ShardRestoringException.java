/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Indicates that a shard is currently being restored from a snapshot and is therefore temporarily
 * unavailable for reads and writes. This is a transient condition: the shard will become available
 * once the restore completes. Callers should retry the request after a short delay.
 *
 * <p>This exception is produced only when the failing {@link ShardId} can be positively correlated
 * to an in-progress, API-level snapshot restore via
 * {@link RestoreService#isRestoringShardFromSnapshot}. Unrelated shard-unavailable failures retain
 * their existing error types and HTTP statuses.
 *
 * <p>The body carries the shard identity ({@code es.index}, {@code es.index_uuid}, {@code es.shard}
 * via {@link #setShard}) plus the restore UUID ({@code es.restore_uuid}), which can be
 * cross-referenced against the output of {@code GET _recovery} and {@code GET _cluster/state}.
 */
public final class ShardRestoringException extends ElasticsearchException {

    /**
     * The transport version from which this exception is serialised as its own type.
     * Older nodes receive a {@link org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper}
     * that preserves the exception name, HTTP 409 status, and all metadata keys.
     */
    public static final TransportVersion SHARD_RESTORING_EXCEPTION_VERSION = TransportVersion.fromName("shard_restoring_exception");

    private static final String RESTORE_UUID_KEY = "es.restore_uuid";

    /**
     * Creates a {@code ShardRestoringException} for the given shard and active restore.
     *
     * @param shardId     the shard that is currently being restored
     * @param restoreUuid the restore UUID ({@code RestoreInProgress.Entry.uuid()}),
     *                    which cross-references {@code GET _recovery}'s {@code restoreUUID} field
     */
    @SuppressWarnings("this-escape")
    public ShardRestoringException(ShardId shardId, String restoreUuid) {
        super("shard [" + shardId + "] is currently being restored from a snapshot and is temporarily unavailable");
        setShard(shardId);
        addMetadata(RESTORE_UUID_KEY, restoreUuid);
    }

    public ShardRestoringException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

    /**
     * Returns the restore UUID carried in this exception, or {@code null} if the exception
     * arrived from a node running a version that does not serialise metadata (extremely unlikely
     * in practice; included for completeness).
     */
    public String restoreUuid() {
        var values = getMetadata(RESTORE_UUID_KEY);
        return values == null ? null : values.getFirst();
    }

    @Override
    public Throwable fillInStackTrace() {
        return this; // transient condition, not a bug — no stack trace needed
    }
}
