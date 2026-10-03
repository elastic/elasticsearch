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
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

/**
 * Indicates that a shard is currently being restored from a snapshot and is therefore temporarily
 * unavailable for reads and writes. This is a transient though potentially long-running condition:
 * the shard will become available once the restore completes.
 *
 * <p>This exception is for data recovery in serverless, where users restore their data from a
 * backup. It is not used for operational backup (OBS) restores, which are started by operators.
 * Because it is surfaced to end users, it deliberately omits shard-level details such as the shard
 * number and index UUID. The body carries only the index name ({@code es.index}) and the recovery
 * ID ({@code es.recovery_id}), which is the restore UUID that the recovery APIs report as
 * {@code recovery_id}.
 */
public final class ShardRestoringException extends ElasticsearchException {

    /**
     * The transport version from which this exception is serialised as its own type.
     * Older nodes receive a {@link org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper}
     * that preserves the exception name, HTTP 403 status, and all metadata keys.
     */
    public static final TransportVersion SHARD_RESTORING_EXCEPTION_VERSION = TransportVersion.fromName("shard_restoring_exception");

    private static final String INDEX_KEY = "es.index";
    private static final String RECOVERY_ID_KEY = "es.recovery_id";

    /**
     * Creates a {@code ShardRestoringException} for the given index and active restore.
     *
     * @param indexName  the name of the index with a shard that is currently being restored
     * @param recoveryId the non-null restore UUID ({@code RestoreInProgress.Entry.uuid()}), reported to users as the recovery ID
     */
    public ShardRestoringException(String indexName, String recoveryId) {
        super(
            "index [" + Objects.requireNonNull(indexName) + "] is currently being restored from a snapshot and is temporarily unavailable"
        );
        // Add only the index name: setIndex(String) would also add a placeholder es.index_uuid of "_na_".
        addMetadata(INDEX_KEY, indexName);
        addMetadata(RECOVERY_ID_KEY, Objects.requireNonNull(recoveryId));
    }

    public ShardRestoringException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.FORBIDDEN;
    }

    /**
     * Returns the recovery ID (the restore UUID) carried in this exception, or {@code null} if it is absent.
     */
    public String recoveryId() {
        var values = getMetadata(RECOVERY_ID_KEY);
        return values == null ? null : values.getFirst();
    }

    @Override
    public Throwable fillInStackTrace() {
        return this; // transient condition, not a bug — no stack trace needed
    }
}
