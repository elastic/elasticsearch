/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

/**
 * Request for {@link IndicesShardStoresAction}
 */
public class IndicesShardStoresRequest extends MasterNodeReadRequest<IndicesShardStoresRequest> implements IndicesRequest.Replaceable {

    static final int DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS = 100;

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpand();
    private EnumSet<ClusterHealthStatus> statuses = EnumSet.of(ClusterHealthStatus.YELLOW, ClusterHealthStatus.RED);
    private int maxConcurrentShardRequests = DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS;

    /**
     * Create a request for shard stores info for <code>indices</code>
     */
    public IndicesShardStoresRequest(String... indices) {
        this.indices = indices;
    }

    public IndicesShardStoresRequest() {}

    public IndicesShardStoresRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        int nStatus = in.readVInt();
        statuses = EnumSet.noneOf(ClusterHealthStatus.class);
        for (int i = 0; i < nStatus; i++) {
            statuses.add(ClusterHealthStatus.readFrom(in));
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            maxConcurrentShardRequests = in.readVInt();
        } else {
            // earlier versions had unlimited concurrency
            maxConcurrentShardRequests = Integer.MAX_VALUE;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        out.writeCollection(statuses, (o, v) -> o.writeByte(v.value()));
        indicesOptions.writeIndicesOptions(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeVInt(maxConcurrentShardRequests);
        } else if (maxConcurrentShardRequests != DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS) {
            throw new IllegalArgumentException(
                "support for maxConcurrentShardRequests=["
                    + maxConcurrentShardRequests
                    + "] was added in version [8.8.0], cannot send this request using transport version ["
                    + out.getTransportVersion()
                    + "]"
            );
        } // else just drop the value and use the default behaviour
    }

    /**
     * Set statuses to filter shards to get stores info on.
     * see {@link ClusterHealthStatus} for details.
     * Defaults to "yellow" and "red" status
     * @param shardStatuses acceptable values are "green", "yellow", "red" and "all"
     */
    public IndicesShardStoresRequest shardStatuses(String... shardStatuses) {
        statuses = EnumSet.noneOf(ClusterHealthStatus.class);
        for (String statusString : shardStatuses) {
            if ("all".equalsIgnoreCase(statusString)) {
                statuses = EnumSet.allOf(ClusterHealthStatus.class);
                return this;
            }
            statuses.add(ClusterHealthStatus.fromString(statusString));
        }
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * By default, expands wildcards to both open and closed indices
     */
    public IndicesShardStoresRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Sets the indices for the shard stores request
     */
    @Override
    public IndicesShardStoresRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Returns the shard criteria to get store information on
     */
    public EnumSet<ClusterHealthStatus> shardStatuses() {
        return statuses;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public void maxConcurrentShardRequests(int maxConcurrentShardRequests) {
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
    }

    public int maxConcurrentShardRequests() {
        return maxConcurrentShardRequests;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}
