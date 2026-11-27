/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.timeseries;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;
import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY;

final class TsidPrefixHistogramNodeRequest extends AbstractTransportRequest
    implements
        IndicesRequest.Replaceable,
        IndicesRequest.RemoteClusterShardRequest {
    private String[] indices;
    private final IndicesOptions indicesOptions;
    private List<ShardId> shardIds;
    final List<String> metricFields;
    final long minTimestamp;
    final long maxTimestamp;
    final long bucketMillis;

    TsidPrefixHistogramNodeRequest(TsidPrefixHistogramRequest request, List<ShardId> shardIds) {
        this.indices = request.originalIndices.indices();
        this.indicesOptions = request.originalIndices.indicesOptions();
        this.shardIds = List.copyOf(shardIds);
        this.metricFields = request.metricFields;
        this.minTimestamp = request.minTimestamp;
        this.maxTimestamp = request.maxTimestamp;
        this.bucketMillis = request.bucketMillis;
    }

    TsidPrefixHistogramNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.shardIds = in.readCollectionAsImmutableList(ShardId::new);
        this.metricFields = in.readStringCollectionAsList();
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
        this.bucketMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeCollection(shardIds);
        out.writeStringCollection(metricFields);
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeVLong(bucketMillis);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public List<ShardId> shards() {
        return shardIds;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        if (Arrays.equals(NO_INDICES_OR_ALIASES_ARRAY, indices) || Arrays.asList(indices).contains(NO_INDEX_PLACEHOLDER)) {
            this.shardIds = List.of();
        }
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public CancellableTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "shards="
                    + shardIds
                    + ", metric_fields="
                    + metricFields
                    + ", time_range=["
                    + minTimestamp
                    + ", "
                    + maxTimestamp
                    + "], bucket_millis="
                    + bucketMillis;
            }
        };
    }
}
