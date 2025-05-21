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
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class FieldCapabilitiesRequest extends LegacyActionRequest implements IndicesRequest.Replaceable, ToXContentObject {
    public static final String NAME = "field_caps_request";
    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    private String clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
    private String[] fields = Strings.EMPTY_ARRAY;
    private String[] filters = Strings.EMPTY_ARRAY;
    private String[] types = Strings.EMPTY_ARRAY;
    private boolean includeUnmapped = false;
    private boolean includeEmptyFields = true;
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
        runtimeFields = in.readGenericMap();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            filters = in.readStringArray();
            types = in.readStringArray();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            includeEmptyFields = in.readBoolean();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.FIELD_CAPS_ADD_CLUSTER_ALIAS)
            || in.getTransportVersion().isPatchFrom(TransportVersions.V_8_19_FIELD_CAPS_ADD_CLUSTER_ALIAS)) {
            clusterAlias = in.readOptionalString();
        } else {
            clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        }
    }

    public FieldCapabilitiesRequest() {}

    /**
     * Returns <code>true</code> iff the results should be merged.
     * <p>
     * Note that when using the high-level REST client, results are always merged (this flag is always considered 'true').
     */
    public boolean isMergeResults() {
        return mergeResults;
    }

    /**
     * If set to <code>true</code> the response will contain only a merged view of the per index field capabilities.
     * Otherwise only unmerged per index field capabilities are returned.
     * <p>
     * Note that when using the high-level REST client, results are always merged (this flag is always considered 'true').
     */
    public void setMergeResults(boolean mergeResults) {
        this.mergeResults = mergeResults;
    }

    void clusterAlias(String clusterAlias) {
        this.clusterAlias = clusterAlias;
    }

    String clusterAlias() {
        return clusterAlias;
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
        out.writeGenericMap(runtimeFields);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            out.writeStringArray(filters);
            out.writeStringArray(types);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeBoolean(includeEmptyFields);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.FIELD_CAPS_ADD_CLUSTER_ALIAS)
            || out.getTransportVersion().isPatchFrom(TransportVersions.V_8_19_FIELD_CAPS_ADD_CLUSTER_ALIAS)) {
            out.writeOptionalString(clusterAlias);
        }
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

    public FieldCapabilitiesRequest filters(String... filters) {
        this.filters = filters;
        return this;
    }

    public String[] filters() {
        return filters;
    }

    public FieldCapabilitiesRequest types(String... types) {
        this.types = types;
        return this;
    }

    public String[] types() {
        return types;
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

    public FieldCapabilitiesRequest includeEmptyFields(boolean includeEmptyFields) {
        this.includeEmptyFields = includeEmptyFields;
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

    public boolean includeEmptyFields() {
        return includeEmptyFields;
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
        return includeUnmapped == that.includeUnmapped
            && mergeResults == that.mergeResults
            && Arrays.equals(indices, that.indices)
            && indicesOptions.equals(that.indicesOptions)
            && Arrays.equals(fields, that.fields)
            && Objects.equals(indexFilter, that.indexFilter)
            && Objects.equals(nowInMillis, that.nowInMillis)
            && Arrays.equals(filters, that.filters)
            && Arrays.equals(types, that.types)
            && Objects.equals(runtimeFields, that.runtimeFields)
            && includeEmptyFields == that.includeEmptyFields;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            indicesOptions,
            includeUnmapped,
            mergeResults,
            indexFilter,
            nowInMillis,
            runtimeFields,
            includeEmptyFields
        );
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(fields);
        result = 31 * result + Arrays.hashCode(filters);
        result = 31 * result + Arrays.hashCode(types);
        return result;
    }

    @Override
    public String getDescription() {
        final StringBuilder stringBuilder = new StringBuilder("indices[");
        Strings.collectionToDelimitedStringWithLimit(Arrays.asList(indices), ",", 1024, stringBuilder);
        return FieldCapabilitiesNodeRequest.completeDescription(stringBuilder, fields, filters, types, includeEmptyFields);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return FieldCapabilitiesRequest.this.getDescription();
            }
        };
    }
}
