/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Requests that are run on a particular replica, first on the primary and then on the replicas like {@link IndexRequest} or
 * {@link TransportShardRefreshAction}.
 */
public abstract class ReplicationRequest<Request extends ReplicationRequest<Request>> extends ActionRequest
        implements IndicesRequest {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    /**
     * Target shard the request should execute on. In case of index and delete requests,
     * shard id gets resolved by the transport action before performing request operation
     * and at request creation time for shard-level bulk, refresh and flush requests.
     */
    protected final ShardId shardId;

    protected TimeValue timeout;
    protected String index;

    /**
     * The number of shard copies that must be active before proceeding with the replication action.
     */
    protected ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private long routedBasedOnClusterVersion = 0;

    public ReplicationRequest(StreamInput in) throws IOException {
        this(null, in);
    }

    public ReplicationRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(in);
        final boolean thinRead = shardId != null;
        if (thinRead) {
            this.shardId = shardId;
        } else {
            this.shardId = in.readOptionalWriteable(ShardId::new);
       }
        waitForActiveShards = ActiveShardCount.readFrom(in);
        timeout = in.readTimeValue();
        if (thinRead) {
            if (in.readBoolean()) {
                index = in.readString();
            } else {
                index = shardId.getIndexName();
            }
        } else {
            index = in.readString();
        }
        routedBasedOnClusterVersion = in.readVLong();
    }

    /**
     * Creates a new request with resolved shard id
     */
    public ReplicationRequest(@Nullable ShardId shardId) {
        this.index = shardId == null ? null : shardId.getIndexName();
        this.shardId = shardId;
        this.timeout = DEFAULT_TIMEOUT;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final Request timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    @SuppressWarnings("unchecked")
    public final Request index(String index) {
        this.index = index;
        return (Request) this;
    }

    @Override
    public String[] indices() {
        return new String[]{index};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    /**
     * @return the shardId of the shard where this operation should be executed on.
     * can be null if the shardID has not yet been resolved
     */
    @Nullable
    public ShardId shardId() {
        return shardId;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the replication
     * operation. Defaults to {@link ActiveShardCount#DEFAULT}, which requires one shard copy
     * (the primary) to be active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active. Otherwise, use
     * {@link ActiveShardCount#from(int)} to set this value to any non-negative integer, up to the
     * total number of shard copies (number of replicas + 1).
     */
    @SuppressWarnings("unchecked")
    public final Request waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return (Request) this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public final Request waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * Sets the minimum version of the cluster state that is required on the next node before we redirect to another primary.
     * Used to prevent redirect loops, see also {@link TransportReplicationAction.ReroutePhase#doRun()}
     */
    @SuppressWarnings("unchecked")
    protected Request routedBasedOnClusterVersion(long routedBasedOnClusterVersion) {
        this.routedBasedOnClusterVersion = routedBasedOnClusterVersion;
        return (Request) this;
    }

    long routedBasedOnClusterVersion() {
        return routedBasedOnClusterVersion;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(shardId);
        waitForActiveShards.writeTo(out);
        out.writeTimeValue(timeout);
        out.writeString(index);
        out.writeVLong(routedBasedOnClusterVersion);
    }

    /**
     * Thin serialization that does not write {@link #shardId} and will only write {@link #index} if it is different from the index name in
     * {@link #shardId}.
     */
    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeTimeValue(timeout);
        if (shardId != null && index.equals(shardId.getIndexName())) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(index);
        }
        out.writeVLong(routedBasedOnClusterVersion);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new ReplicationTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public abstract String toString(); // force a proper to string to ease debugging

    @Override
    public String getDescription() {
        return toString();
    }

    /**
     * This method is called before this replication request is retried
     * the first time.
     */
    public void onRetry() {
        // nothing by default
    }
}
