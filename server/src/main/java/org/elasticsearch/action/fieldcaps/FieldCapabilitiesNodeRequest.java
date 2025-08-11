/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class FieldCapabilitiesNodeRequest extends ActionRequest implements IndicesRequest {

    private final List<ShardId> shardIds;
    private final String[] fields;
    private final OriginalIndices originalIndices;
    private final QueryBuilder indexFilter;
    private final long nowInMillis;
    private final Map<String, Object> runtimeFields;

    FieldCapabilitiesNodeRequest(StreamInput in) throws IOException {
        super(in);
        shardIds = in.readList(ShardId::new);
        fields = in.readStringArray();
        originalIndices = OriginalIndices.readOriginalIndices(in);
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nowInMillis = in.readLong();
        runtimeFields = in.readMap();
    }

    FieldCapabilitiesNodeRequest(
        List<ShardId> shardIds,
        String[] fields,
        OriginalIndices originalIndices,
        QueryBuilder indexFilter,
        long nowInMillis,
        Map<String, Object> runtimeFields
    ) {
        this.shardIds = Objects.requireNonNull(shardIds);
        this.fields = fields;
        this.originalIndices = originalIndices;
        this.indexFilter = indexFilter;
        this.nowInMillis = nowInMillis;
        this.runtimeFields = runtimeFields;
    }

    public String[] fields() {
        return fields;
    }

    public OriginalIndices originalIndices() {
        return originalIndices;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    public Map<String, Object> runtimeFields() {
        return runtimeFields;
    }

    public List<ShardId> shardIds() {
        return shardIds;
    }

    public long nowInMillis() {
        return nowInMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(shardIds);
        out.writeStringArray(fields);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeOptionalNamedWriteable(indexFilter);
        out.writeLong(nowInMillis);
        out.writeMap(runtimeFields);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String getDescription() {
        final StringBuilder stringBuilder = new StringBuilder("shards[");
        Strings.collectionToDelimitedStringWithLimit(shardIds, ",", "", "", 1024, stringBuilder);
        stringBuilder.append("], fields[");
        Strings.collectionToDelimitedStringWithLimit(Arrays.asList(fields), ",", "", "", 1024, stringBuilder);
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return FieldCapabilitiesNodeRequest.this.getDescription();
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesNodeRequest that = (FieldCapabilitiesNodeRequest) o;
        return nowInMillis == that.nowInMillis
            && shardIds.equals(that.shardIds)
            && Arrays.equals(fields, that.fields)
            && Objects.equals(originalIndices, that.originalIndices)
            && Objects.equals(indexFilter, that.indexFilter)
            && Objects.equals(runtimeFields, that.runtimeFields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(originalIndices, indexFilter, nowInMillis, runtimeFields);
        result = 31 * result + shardIds.hashCode();
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
