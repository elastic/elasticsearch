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
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class FieldCapabilitiesRequest extends ActionRequest implements IndicesRequest.Replaceable, ToXContentObject {
    public static final String NAME = "field_caps_request";

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String[] fields = Strings.EMPTY_ARRAY;
    private boolean includeUnmapped = false;
    // pkg private API mainly for cross cluster search to signal that we do multiple reductions ie. the results should not be merged
    private boolean mergeResults = true;
    private QueryBuilder indexFilter;
    private Map<String, Object> runtimeFields = Collections.emptyMap();
    private Long nowInMillis;

    public FieldCapabilitiesRequest(StreamInput in) throws IOException {
        super(in);
        fields = in.readStringArray();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        mergeResults = in.readBoolean();
        includeUnmapped = in.readBoolean();
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nowInMillis = in.readOptionalLong();
        runtimeFields = in.readMap();
    }

    public FieldCapabilitiesRequest() {
    }

    /**
     * Returns <code>true</code> iff the results should be merged.
     * <p>
     * Note that when using the high-level REST client, results are always merged (this flag is always considered 'true').
     */
    boolean isMergeResults() {
        return mergeResults;
    }

    /**
     * If set to <code>true</code> the response will contain only a merged view of the per index field capabilities.
     * Otherwise only unmerged per index field capabilities are returned.
     * <p>
     * Note that when using the high-level REST client, results are always merged (this flag is always considered 'true').
     */
    void setMergeResults(boolean mergeResults) {
        this.mergeResults = mergeResults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(fields);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeBoolean(mergeResults);
        out.writeBoolean(includeUnmapped);
        out.writeOptionalNamedWriteable(indexFilter);
        out.writeOptionalLong(nowInMillis);
        out.writeMap(runtimeFields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (indexFilter != null) {
            builder.field("index_filter", indexFilter);
        }
        if (runtimeFields.isEmpty() == false) {
            builder.field("runtime_mappings", runtimeFields);
        }
        builder.endObject();
        return builder;
    }

    /**
     * The list of field names to retrieve
     */
    public FieldCapabilitiesRequest fields(String... fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("specified fields can't be null or empty");
        }
        Set<String> fieldSet = new HashSet<>(Arrays.asList(fields));
        this.fields = fieldSet.toArray(new String[0]);
        return this;
    }

    public String[] fields() {
        return fields;
    }

    /**
     * The list of indices to lookup
     */
    @Override
    public FieldCapabilitiesRequest indices(String... indices) {
        this.indices = Objects.requireNonNull(indices, "indices must not be null");
        return this;
    }

    public FieldCapabilitiesRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indices options must not be null");
        return this;
    }

    public FieldCapabilitiesRequest includeUnmapped(boolean includeUnmapped) {
        this.includeUnmapped = includeUnmapped;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public boolean includeUnmapped() {
        return includeUnmapped;
    }

    /**
     * Allows to filter indices if the provided {@link QueryBuilder} rewrites to `match_none` on every shard.
     */
    public FieldCapabilitiesRequest indexFilter(QueryBuilder indexFilter) {
        this.indexFilter = indexFilter;
        return this;
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }
    /**
     * Allows adding search runtime fields if provided.
     */
    public FieldCapabilitiesRequest runtimeFields(Map<String, Object> runtimeFieldsSection) {
        this.runtimeFields = runtimeFieldsSection;
        return this;
    }

    public Map<String, Object> runtimeFields() {
        return this.runtimeFields;
    }

    Long nowInMillis() {
        return nowInMillis;
    }

    void nowInMillis(long nowInMillis) {
        this.nowInMillis = nowInMillis;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (fields == null || fields.length == 0) {
            validationException = ValidateActions.addValidationError("no fields specified", validationException);
        }
        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesRequest that = (FieldCapabilitiesRequest) o;
        return includeUnmapped == that.includeUnmapped &&
            mergeResults == that.mergeResults &&
            Arrays.equals(indices, that.indices) &&
            indicesOptions.equals(that.indicesOptions) &&
            Arrays.equals(fields, that.fields) &&
            Objects.equals(indexFilter, that.indexFilter) &&
            Objects.equals(nowInMillis, that.nowInMillis) &&
            Objects.equals(runtimeFields, that.runtimeFields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indicesOptions, includeUnmapped, mergeResults, indexFilter, nowInMillis, runtimeFields);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
