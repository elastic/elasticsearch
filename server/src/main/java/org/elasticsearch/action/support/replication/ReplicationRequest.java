/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Requests that are run on a particular replica, first on the primary and then on the replicas like {@link IndexRequest} or
 * {@link TransportShardRefreshAction}.
 */
public abstract class ReplicationRequest<Request extends ReplicationRequest<Request>> extends LegacyActionRequest
    implements
        IndicesRequest {

    // superseded
    private static final TransportVersion INDEX_RESHARD_SHARDCOUNT_SUMMARY = TransportVersion.fromName("index_reshard_shardcount_summary");
    // bumped to use VInt instead of Int
    private static final TransportVersion INDEX_RESHARD_SHARDCOUNT_SMALL = TransportVersion.fromName("index_reshard_shardcount_small");

    public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueMinutes(1);

    /**
     * Target shard the request should execute on. In case of index and delete requests,
     * shard id gets resolved by the transport action before performing request operation
     * and at request creation time for shard-level bulk, refresh and flush requests.
     */
    protected final ShardId shardId;

    protected TimeValue timeout;
    protected String index;

    /**
     * The reshardSplitShardCountSummary has been added to accommodate the Resharding feature.
     * This is populated when the coordinator is deciding which shards a request applies to.
     * For example, {@link org.elasticsearch.action.bulk.BulkOperation} splits
     * an incoming bulk request into shard level {@link org.elasticsearch.action.bulk.BulkShardRequest}
     * based on its cluster state view of the number of shards that are ready for indexing.
     * The purpose of this metadata is to reconcile the cluster state visible at the coordinating
     * node with that visible at the source shard node. (w.r.t resharding).
     * When an index is being split, there is a point in time when the newly created shard (target shard)
     * takes over its portion of the document space from the original shard (source shard).
     * Although the handoff is atomic at the original (source shard) and new shards (target shard),
     * there is a window of time between the coordinating node creating a shard request and the shard receiving and processing it.
     * This field is used by the original shard (source shard) when it processes the request to detect whether
     * the coordinator's view of the new shard's state when it created the request matches the shard's current state,
     * or whether the request must be reprocessed taking into account the current shard states.
     *
     * Note that we are able to get away with a single number, instead of an array of target shard states,
     * because we only allow splits in increments of 2x.
     *
     * Example 1:
     * Suppose we are resharding an index from 2 -> 4 shards. While splitting a bulk request, the coordinator observes
     * that target shards are not ready for indexing. So requests that are meant for shard 0 and 2 are bundled together,
     * sent to shard 0 with “reshardSplitShardCountSummary” 2 in the request.
     * Requests that are meant for shard 1 and 3 are bundled together,
     * sent to shard 1 with “reshardSplitShardCountSummary” 2 in the request.
     *
     * Example 2:
     * Suppose we are resharding an index from 4 -> 8 shards. While splitting a bulk request, the coordinator observes
     * that source shard 0 has completed HANDOFF but source shards 1, 2, 3 have not completed handoff.
     * So, the shard-bulk-request it sends to shard 0 and 4 has the "reshardSplitShardCountSummary" 8,
     * while the shard-bulk-request it sends to shard 1,2,3 has the "reshardSplitShardCountSummary" 4.
     * Note that in this case no shard-bulk-request is sent to shards 5, 6, 7 and the requests that were meant for these target shards
     * are bundled together with and sent to their source shards.
     *
     * A value of 0 indicates an INVALID reshardSplitShardCountSummary. Hence, a request with INVALID reshardSplitShardCountSummary
     * will be treated as a Summary mismatch on the source shard node.
     */
    protected final int reshardSplitShardCountSummary;

    /**
     * The number of shard copies that must be active before proceeding with the replication action.
     */
    protected ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private long routedBasedOnClusterVersion = 0;

    public ReplicationRequest(StreamInput in) throws IOException {
        this(null, in);
    }

    public ReplicationRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        this(shardId, 0, in);
    }

    public ReplicationRequest(@Nullable ShardId shardId, int reshardSplitShardCountSummary, StreamInput in) throws IOException {
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
        if (thinRead) {
            this.reshardSplitShardCountSummary = reshardSplitShardCountSummary;
        } else {
            if (in.getTransportVersion().supports(INDEX_RESHARD_SHARDCOUNT_SMALL)) {
                this.reshardSplitShardCountSummary = in.readVInt();
            } else if (in.getTransportVersion().supports(INDEX_RESHARD_SHARDCOUNT_SUMMARY)) {
                this.reshardSplitShardCountSummary = in.readInt();
            } else {
                this.reshardSplitShardCountSummary = 0;
            }
        }
    }

    /**
     * Creates a new request with resolved shard id
     */
    public ReplicationRequest(@Nullable ShardId shardId) {
        this(shardId, 0);
    }

    /**
     * Creates a new request with resolved shard id and reshardSplitShardCountSummary
     */
    public ReplicationRequest(@Nullable ShardId shardId, int reshardSplitShardCountSummary) {
        this.index = shardId == null ? null : shardId.getIndexName();
        this.shardId = shardId;
        this.timeout = DEFAULT_TIMEOUT;
        this.reshardSplitShardCountSummary = reshardSplitShardCountSummary;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
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
        return new String[] { index };
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
     * @return The effective shard count as seen by the coordinator when creating this request.
     * can be 0 if this has not yet been resolved.
     */
    public int reshardSplitShardCountSummary() {
        return reshardSplitShardCountSummary;
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
        if (out.getTransportVersion().supports(INDEX_RESHARD_SHARDCOUNT_SMALL)) {
            out.writeVInt(reshardSplitShardCountSummary);
        } else if (out.getTransportVersion().supports(INDEX_RESHARD_SHARDCOUNT_SUMMARY)) {
            out.writeInt(reshardSplitShardCountSummary);
        }
    }

    /**
     * Thin serialization that does not write {@link #shardId} and will only write {@link #index} if it is different from the index name in
     * {@link #shardId}. Since we do not write {@link #shardId}, we also do not write {@link #reshardSplitShardCountSummary}.
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
        return new ReplicationTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return ReplicationRequest.this.getDescription();
            }
        };
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
