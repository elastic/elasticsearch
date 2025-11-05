/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.Objects;

/**
 * Base exception class indicating that a failure occurred while performing an operation on a shard.
 * This exception is used throughout Elasticsearch to represent failures during shard-level operations
 * such as searching, indexing, or other shard-specific tasks.
 *
 * <p>Shard operation failures are common in distributed systems where operations may fail on individual
 * shards while succeeding on others. This exception captures the context of the failure including the
 * index, shard ID, reason, status, and underlying cause.
 *
 * <p>This is an abstract class that should be subclassed for specific types of shard failures.
 * It implements both {@link Writeable} for serialization across nodes and {@link ToXContentObject}
 * for JSON/XML representation.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Creating a custom shard operation failure
 * public class SearchShardFailure extends ShardOperationFailedException {
 *     public SearchShardFailure(String index, int shardId, String reason,
 *                               RestStatus status, Throwable cause) {
 *         super(index, shardId, reason, status, cause);
 *     }
 *     // Implement serialization methods...
 * }
 *
 * // Handling shard failures
 * try {
 *     performShardOperation(shardId);
 * } catch (Exception e) {
 *     ShardOperationFailedException failure = new SearchShardFailure(
 *         indexName, shardId, e.getMessage(), RestStatus.INTERNAL_SERVER_ERROR, e
 *     );
 *     collectFailure(failure);
 * }
 * }</pre>
 */
public abstract class ShardOperationFailedException extends Exception implements Writeable, ToXContentObject {

    /**
     * The name of the index where the operation failed.
     */
    protected String index;

    /**
     * The ID of the shard where the operation failed, or -1 if unknown.
     */
    protected int shardId = -1;

    /**
     * The reason describing why the operation failed.
     */
    protected String reason;

    /**
     * The HTTP status code representing the type of failure.
     */
    protected RestStatus status;

    /**
     * The underlying cause of the failure.
     */
    protected Throwable cause;

    /**
     * Default constructor for subclasses and deserialization.
     */
    protected ShardOperationFailedException() {

    }

    /**
     * Constructs a new shard operation failed exception with full details.
     *
     * @param index the name of the index, or {@code null} if it can't be determined
     * @param shardId the ID of the shard where the failure occurred
     * @param reason the reason for the failure (must not be null)
     * @param status the REST status representing the failure (must not be null)
     * @param cause the underlying cause of the failure (must not be null)
     * @throws NullPointerException if reason, status, or cause is null
     */
    protected ShardOperationFailedException(@Nullable String index, int shardId, String reason, RestStatus status, Throwable cause) {
        this.index = index;
        this.shardId = shardId;
        this.reason = Objects.requireNonNull(reason, "reason cannot be null");
        this.status = Objects.requireNonNull(status, "status cannot be null");
        this.cause = Objects.requireNonNull(cause, "cause cannot be null");
    }

    /**
     * Returns the name of the index where the operation failed.
     *
     * @return the index name, or {@code null} if it cannot be determined
     */
    @Nullable
    public final String index() {
        return index;
    }

    /**
     * Returns the ID of the shard where the operation failed.
     *
     * @return the shard ID, or {@code -1} if it cannot be determined
     */
    public final int shardId() {
        return shardId;
    }

    /**
     * Returns the reason describing why the operation failed.
     *
     * @return the failure reason
     */
    public final String reason() {
        return reason;
    }

    /**
     * Returns the REST status code representing the type of failure.
     *
     * @return the REST status
     */
    public final RestStatus status() {
        return status;
    }

    /**
     * Returns the underlying cause of this failure.
     *
     * @return the cause throwable
     */
    public final Throwable getCause() {
        return cause;
    }
}
