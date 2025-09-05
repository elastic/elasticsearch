/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
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

class FieldCapabilitiesNodeRequest extends LegacyActionRequest implements IndicesRequest {

    private final List<ShardId> shardIds;
    private final String[] fields;
    private final String[] filters;
    private final String[] allowedTypes;
    private final OriginalIndices originalIndices;
    private final QueryBuilder indexFilter;
    private final long nowInMillis;
    private final Map<String, Object> runtimeFields;
    private final boolean includeEmptyFields;

    FieldCapabilitiesNodeRequest(StreamInput in) throws IOException {
        super(in);
        shardIds = in.readCollectionAsList(ShardId::new);
        fields = in.readStringArray();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            filters = in.readStringArray();
            allowedTypes = in.readStringArray();
        } else {
            filters = Strings.EMPTY_ARRAY;
            allowedTypes = Strings.EMPTY_ARRAY;
        }
        originalIndices = OriginalIndices.readOriginalIndices(in);
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nowInMillis = in.readLong();
        runtimeFields = in.readGenericMap();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            includeEmptyFields = in.readBoolean();
        } else {
            includeEmptyFields = true;
        }
    }

    FieldCapabilitiesNodeRequest(
        List<ShardId> shardIds,
        String[] fields,
        String[] filters,
        String[] allowedTypes,
        OriginalIndices originalIndices,
        QueryBuilder indexFilter,
        long nowInMillis,
        Map<String, Object> runtimeFields,
        boolean includeEmptyFields
    ) {
        this.shardIds = Objects.requireNonNull(shardIds);
        this.fields = fields;
        this.filters = filters;
        this.allowedTypes = allowedTypes;
        this.originalIndices = originalIndices;
        this.indexFilter = indexFilter;
        this.nowInMillis = nowInMillis;
        this.runtimeFields = runtimeFields;
        this.includeEmptyFields = includeEmptyFields;
    }

    public String[] fields() {
        return fields;
    }

    public String[] filters() {
        return filters;
    }

    public String[] allowedTypes() {
        return allowedTypes;
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

    public boolean includeEmptyFields() {
        return includeEmptyFields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(shardIds);
        out.writeStringArray(fields);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            out.writeStringArray(filters);
            out.writeStringArray(allowedTypes);
        }
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeOptionalNamedWriteable(indexFilter);
        out.writeLong(nowInMillis);
        out.writeGenericMap(runtimeFields);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeBoolean(includeEmptyFields);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String getDescription() {
        final StringBuilder stringBuilder = new StringBuilder("shards[");
        Strings.collectionToDelimitedStringWithLimit(shardIds, ",", 1024, stringBuilder);
        return completeDescription(stringBuilder, fields, filters, allowedTypes, includeEmptyFields);
    }

    static String completeDescription(
        StringBuilder stringBuilder,
        String[] fields,
        String[] filters,
        String[] allowedTypes,
        boolean includeEmptyFields
    ) {
        stringBuilder.append("], fields[");
        Strings.collectionToDelimitedStringWithLimit(Arrays.asList(fields), ",", 1024, stringBuilder);
        stringBuilder.append("], filters[");
        Strings.collectionToDelimitedString(Arrays.asList(filters), ",", stringBuilder);
        stringBuilder.append("], types[");
        Strings.collectionToDelimitedString(Arrays.asList(allowedTypes), ",", stringBuilder);
        stringBuilder.append("], includeEmptyFields[");
        stringBuilder.append(includeEmptyFields);
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
            && Arrays.equals(filters, that.filters)
            && Arrays.equals(allowedTypes, that.allowedTypes)
            && Objects.equals(originalIndices, that.originalIndices)
            && Objects.equals(indexFilter, that.indexFilter)
            && Objects.equals(runtimeFields, that.runtimeFields)
            && includeEmptyFields == that.includeEmptyFields;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(originalIndices, indexFilter, nowInMillis, runtimeFields, includeEmptyFields);
        result = 31 * result + shardIds.hashCode();
        result = 31 * result + Arrays.hashCode(fields);
        result = 31 * result + Arrays.hashCode(filters);
        result = 31 * result + Arrays.hashCode(allowedTypes);
        return result;
    }
}
