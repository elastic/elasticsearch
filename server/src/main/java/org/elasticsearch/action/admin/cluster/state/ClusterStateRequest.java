/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ClusterStateRequest extends MasterNodeReadRequest<ClusterStateRequest> implements IndicesRequest.Replaceable {

    public static final TimeValue DEFAULT_WAIT_FOR_NODE_TIMEOUT = TimeValue.timeValueMinutes(1);

    private boolean routingTable = true;
    private boolean nodes = true;
    private boolean metadata = true;
    private boolean blocks = true;
    private boolean customs = true;
    private Long waitForMetadataVersion;
    private TimeValue waitForTimeout = DEFAULT_WAIT_FOR_NODE_TIMEOUT;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();

    public ClusterStateRequest() {}

    public ClusterStateRequest(StreamInput in) throws IOException {
        super(in);
        routingTable = in.readBoolean();
        nodes = in.readBoolean();
        metadata = in.readBoolean();
        blocks = in.readBoolean();
        customs = in.readBoolean();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        waitForTimeout = in.readTimeValue();
        waitForMetadataVersion = in.readOptionalLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(routingTable);
        out.writeBoolean(nodes);
        out.writeBoolean(metadata);
        out.writeBoolean(blocks);
        out.writeBoolean(customs);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeTimeValue(waitForTimeout);
        out.writeOptionalLong(waitForMetadataVersion);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ClusterStateRequest all() {
        routingTable = true;
        nodes = true;
        metadata = true;
        blocks = true;
        customs = true;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public ClusterStateRequest clear() {
        routingTable = false;
        nodes = false;
        metadata = false;
        blocks = false;
        customs = false;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public boolean routingTable() {
        return routingTable;
    }

    public ClusterStateRequest routingTable(boolean routingTable) {
        this.routingTable = routingTable;
        return this;
    }

    public boolean nodes() {
        return nodes;
    }

    public ClusterStateRequest nodes(boolean nodes) {
        this.nodes = nodes;
        return this;
    }

    public boolean metadata() {
        return metadata;
    }

    public ClusterStateRequest metadata(boolean metadata) {
        this.metadata = metadata;
        return this;
    }

    public boolean blocks() {
        return blocks;
    }

    public ClusterStateRequest blocks(boolean blocks) {
        this.blocks = blocks;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ClusterStateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    public final ClusterStateRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public ClusterStateRequest customs(boolean customs) {
        this.customs = customs;
        return this;
    }

    public boolean customs() {
        return customs;
    }

    public TimeValue waitForTimeout() {
        return waitForTimeout;
    }

    public ClusterStateRequest waitForTimeout(TimeValue waitForTimeout) {
        this.waitForTimeout = waitForTimeout;
        return this;
    }

    public Long waitForMetadataVersion() {
        return waitForMetadataVersion;
    }

    public ClusterStateRequest waitForMetadataVersion(long waitForMetadataVersion) {
        if (waitForMetadataVersion < 1) {
            throw new IllegalArgumentException(
                "provided waitForMetadataVersion should be >= 1, but instead is [" + waitForMetadataVersion + "]"
            );
        }
        this.waitForMetadataVersion = waitForMetadataVersion;
        return this;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return true;
            }
        };
    }

    @Override
    public String getDescription() {
        final StringBuilder stringBuilder = new StringBuilder("cluster state [");
        if (routingTable) {
            stringBuilder.append("routing table, ");
        }
        if (nodes) {
            stringBuilder.append("nodes, ");
        }
        if (metadata) {
            stringBuilder.append("metadata, ");
        }
        if (blocks) {
            stringBuilder.append("blocks, ");
        }
        if (customs) {
            stringBuilder.append("customs, ");
        }
        if (local) {
            stringBuilder.append("local, ");
        }
        if (waitForMetadataVersion != null) {
            stringBuilder.append("wait for metadata version [")
                .append(waitForMetadataVersion)
                .append("] with timeout [")
                .append(waitForTimeout)
                .append("], ");
        }
        if (indices.length > 0) {
            stringBuilder.append("indices ").append(Arrays.toString(indices)).append(", ");
        }
        stringBuilder.append("master timeout [").append(masterNodeTimeout).append("]]");
        return stringBuilder.toString();
    }

}
