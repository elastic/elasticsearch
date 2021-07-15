/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.rest.RestStatus;

import java.util.Objects;

/**
 * An exception indicating that a failure occurred performing an operation on the shard.
 *
 */
public abstract class ShardOperationFailedException extends Exception implements Writeable, ToXContentObject {

    protected String index;
    protected int shardId = -1;
    protected String reason;
    protected RestStatus status;
    protected Throwable cause;

    protected ShardOperationFailedException() {

    }

    protected ShardOperationFailedException(@Nullable String index, int shardId, String reason, RestStatus status, Throwable cause) {
        this.index = index;
        this.shardId = shardId;
        this.reason = Objects.requireNonNull(reason, "reason cannot be null");
        this.status = Objects.requireNonNull(status, "status cannot be null");
        this.cause = Objects.requireNonNull(cause, "cause cannot be null");
    }

    /**
     * The index the operation failed on. Might return {@code null} if it can't be derived.
     */
    @Nullable
    public final String index() {
        return index;
    }

    /**
     * The index the operation failed on. Might return {@code -1} if it can't be derived.
     */
    public final int shardId() {
        return shardId;
    }

    /**
     * The reason of the failure.
     */
    public final String reason() {
        return reason;
    }

    /**
     * The status of the failure.
     */
    public final RestStatus status() {
        return status;
    }

    /**
     * The cause of this failure
     */
    public final Throwable getCause() {
        return cause;
    }
}
