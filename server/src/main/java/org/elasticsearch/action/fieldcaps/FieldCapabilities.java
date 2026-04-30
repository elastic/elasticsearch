/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ParserConstructor;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM;
import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_METRIC_PARAM;

/**
 * Describes the capabilities of a field optionally merged across multiple indices.
 */
public class FieldCapabilities implements Writeable, ToXContentObject {

    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField IS_METADATA_FIELD = new ParseField("metadata_field");
    public static final ParseField SEARCHABLE_FIELD = new ParseField("searchable");
    public static final ParseField AGGREGATABLE_FIELD = new ParseField("aggregatable");
    public static final ParseField TIME_SERIES_DIMENSION_FIELD = new ParseField(TIME_SERIES_DIMENSION_PARAM);
    public static final ParseField TIME_SERIES_METRIC_FIELD = new ParseField(TIME_SERIES_METRIC_PARAM);
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField NON_SEARCHABLE_INDICES_FIELD = new ParseField("non_searchable_indices");
    public static final ParseField NON_AGGREGATABLE_INDICES_FIELD = new ParseField("non_aggregatable_indices");
    public static final ParseField NON_DIMENSION_INDICES_FIELD = new ParseField("non_dimension_indices");
    public static final ParseField METRIC_CONFLICTS_INDICES_FIELD = new ParseField("metric_conflicts_indices");
    public static final ParseField INFERENCE_FIELD = new ParseField("inference");
    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField SEARCH_INFERENCE_ID_FIELD = new ParseField("search_inference_id");
    public static final ParseField INFERENCE_CONFLICTS_INDICES_FIELD = new ParseField("inference_conflicts_indices");

    static final TransportVersion FIELD_CAPS_INFERENCE_INFO = TransportVersion.fromName("field_caps_inference_info");

    private final String name;
    private final String type;
    private final boolean isMetadataField;
    private final boolean isSearchable;
    private final boolean isAggregatable;
    private final boolean isDimension;
    private final TimeSeriesParams.MetricType metricType;

    private final String[] indices;
    private final String[] nonSearchableIndices;
    private final String[] nonAggregatableIndices;
    private final String[] nonDimensionIndices;
    private final String[] metricConflictsIndices;

    @Nullable
    private final String inferenceId;
    @Nullable
    private final String searchInferenceId;
    @Nullable
    private final String[] inferenceConflictsIndices;

    private final Map<String, Set<String>> meta;

    /**
     * Constructor for a set of indices.
     * @param name The name of the field
     * @param type The type associated with the field.
     * @param isMetadataField Whether this field is a metadata field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param isDimension Whether this field can be used as dimension
     * @param metricType If this field is a metric field, returns the metric's type or null for non-metrics fields
     * @param indices The list of indices where this field name is defined as {@code type}.
     *                When {@code includeIndices} is set to {@code false}, this list is only
     *                present if there is a mapping conflict (e.g. the same field has different
     *                types across indices).
     *                When {@code includeIndices} is set to {@code true}, this list is always
     *                present and contains all indices where the field exists, regardless of
     *                mapping conflicts.
     * @param nonSearchableIndices The list of indices where this field is not searchable,
     *                             or null if the field is searchable in all indices.
     * @param nonAggregatableIndices The list of indices where this field is not aggregatable,
     *                               or null if the field is aggregatable in all indices.
     * @param nonDimensionIndices The list of indices where this field is not a dimension
     * @param metricConflictsIndices The list of indices where this field is has different metric types or not mark as a metric
     * @param inferenceId The id of the inference endpoint that backs this field (when all indices agree),
     *                    or null when the field is not backed by inference in every index or when the indices disagree.
     * @param searchInferenceId The id of the inference endpoint used at query time, only set when distinct from {@code inferenceId}
     *                          and consistent across all indices; null otherwise.
     * @param inferenceConflictsIndices The list of indices where this field is backed by inference but with different inference ids
     *                                  than the others, or null when there is no conflict (or the field is not inference-backed
     *                                  in every index).
     * @param meta Merged metadata across indices.
     */
    public FieldCapabilities(
        String name,
        String type,
        boolean isMetadataField,
        boolean isSearchable,
        boolean isAggregatable,
        boolean isDimension,
        TimeSeriesParams.MetricType metricType,
        String[] indices,
        String[] nonSearchableIndices,
        String[] nonAggregatableIndices,
        String[] nonDimensionIndices,
        String[] metricConflictsIndices,
        @Nullable String inferenceId,
        @Nullable String searchInferenceId,
        @Nullable String[] inferenceConflictsIndices,
        Map<String, Set<String>> meta
    ) {
        this.name = name;
        this.type = type;
        this.isMetadataField = isMetadataField;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.isDimension = isDimension;
        this.metricType = metricType;
        this.indices = indices;
        this.nonSearchableIndices = nonSearchableIndices;
        this.nonAggregatableIndices = nonAggregatableIndices;
        this.nonDimensionIndices = nonDimensionIndices;
        this.metricConflictsIndices = metricConflictsIndices;
        this.inferenceId = inferenceId;
        this.searchInferenceId = searchInferenceId;
        this.inferenceConflictsIndices = inferenceConflictsIndices;
        this.meta = Objects.requireNonNull(meta);
    }

    /**
     * Constructor without inference info, kept for callers that never deal with inference-backed fields.
     */
    public FieldCapabilities(
        String name,
        String type,
        boolean isMetadataField,
        boolean isSearchable,
        boolean isAggregatable,
        boolean isDimension,
        TimeSeriesParams.MetricType metricType,
        String[] indices,
        String[] nonSearchableIndices,
        String[] nonAggregatableIndices,
        String[] nonDimensionIndices,
        String[] metricConflictsIndices,
        Map<String, Set<String>> meta
    ) {
        this(
            name,
            type,
            isMetadataField,
            isSearchable,
            isAggregatable,
            isDimension,
            metricType,
            indices,
            nonSearchableIndices,
            nonAggregatableIndices,
            nonDimensionIndices,
            metricConflictsIndices,
            null,
            null,
            null,
            meta
        );
    }

    /**
     * Constructor for non-timeseries field caps. Useful for testing
     * Constructor for a set of indices.
     * @param name The name of the field
     * @param type The type associated with the field.
     * @param isMetadataField Whether this field is a metadata field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param indices The list of indices where this field name is defined as {@code type},
     *                or null if all indices have the same {@code type} for the field.
     * @param nonSearchableIndices The list of indices where this field is not searchable,
     *                             or null if the field is searchable in all indices.
     * @param nonAggregatableIndices The list of indices where this field is not aggregatable,
     *                               or null if the field is aggregatable in all indices.
     * @param meta Merged metadata across indices.
     */
    public FieldCapabilities(
        String name,
        String type,
        boolean isMetadataField,
        boolean isSearchable,
        boolean isAggregatable,
        String[] indices,
        String[] nonSearchableIndices,
        String[] nonAggregatableIndices,
        Map<String, Set<String>> meta
    ) {
        this(
            name,
            type,
            isMetadataField,
            isSearchable,
            isAggregatable,
            false,
            null,
            indices,
            nonSearchableIndices,
            nonAggregatableIndices,
            null,
            null,
            null,
            null,
            null,
            meta
        );

    }

    /**
     * Constructor for a set of indices used by parser
     * @param name The name of the field
     * @param type The type associated with the field.
     * @param isMetadataField Whether this field is a metadata field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param isDimension Whether this field can be used as dimension
     * @param metricType If this field is a metric field, returns the metric's type or null for non-metrics fields
     * @param indices The list of indices where this field name is defined as {@code type},
     *                or null if all indices have the same {@code type} for the field.
     * @param nonSearchableIndices The list of indices where this field is not searchable,
     *                             or null if the field is searchable in all indices.
     * @param nonAggregatableIndices The list of indices where this field is not aggregatable,
     *                               or null if the field is aggregatable in all indices.
     * @param nonDimensionIndices The list of indices where this field is not a dimension
     * @param metricConflictsIndices The list of indices where this field is has different metric types or not mark as a metric
     * @param inference Inference-related fields parsed from the {@code inference} sub-object, or null if absent.
     * @param meta Merged metadata across indices.
     */
    @SuppressWarnings("unused")
    @ParserConstructor
    public FieldCapabilities(
        String name,
        String type,
        Boolean isMetadataField,
        boolean isSearchable,
        boolean isAggregatable,
        Boolean isDimension,
        String metricType,
        List<String> indices,
        List<String> nonSearchableIndices,
        List<String> nonAggregatableIndices,
        List<String> nonDimensionIndices,
        List<String> metricConflictsIndices,
        InferenceCapabilities inference,
        Map<String, Set<String>> meta
    ) {
        this(
            name,
            type,
            isMetadataField == null ? false : isMetadataField,
            isSearchable,
            isAggregatable,
            isDimension == null ? false : isDimension,
            metricType != null ? TimeSeriesParams.MetricType.fromString(metricType) : null,
            indices != null ? indices.toArray(new String[0]) : null,
            nonSearchableIndices != null ? nonSearchableIndices.toArray(new String[0]) : null,
            nonAggregatableIndices != null ? nonAggregatableIndices.toArray(new String[0]) : null,
            nonDimensionIndices != null ? nonDimensionIndices.toArray(new String[0]) : null,
            metricConflictsIndices != null ? metricConflictsIndices.toArray(new String[0]) : null,
            inference != null ? inference.inferenceId() : null,
            inference != null ? inference.searchInferenceId() : null,
            inference != null && inference.inferenceConflictsIndices() != null
                ? inference.inferenceConflictsIndices().toArray(new String[0])
                : null,
            meta != null ? meta : Collections.emptyMap()
        );
    }

    FieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.isMetadataField = in.readBoolean();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
        this.isDimension = in.readBoolean();
        this.metricType = in.readOptionalEnum(TimeSeriesParams.MetricType.class);
        this.indices = in.readOptionalStringArray();
        this.nonSearchableIndices = in.readOptionalStringArray();
        this.nonAggregatableIndices = in.readOptionalStringArray();
        this.nonDimensionIndices = in.readOptionalStringArray();
        this.metricConflictsIndices = in.readOptionalStringArray();
        if (in.getTransportVersion().supports(FIELD_CAPS_INFERENCE_INFO)) {
            this.inferenceId = in.readOptionalString();
            this.searchInferenceId = in.readOptionalString();
            this.inferenceConflictsIndices = in.readOptionalStringArray();
        } else {
            this.inferenceId = null;
            this.searchInferenceId = null;
            this.inferenceConflictsIndices = null;
        }
        meta = in.readMap(i -> i.readCollectionAsSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(isMetadataField);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        out.writeBoolean(isDimension);
        out.writeOptionalEnum(metricType);
        out.writeOptionalStringArray(indices);
        out.writeOptionalStringArray(nonSearchableIndices);
        out.writeOptionalStringArray(nonAggregatableIndices);
        out.writeOptionalStringArray(nonDimensionIndices);
        out.writeOptionalStringArray(metricConflictsIndices);
        if (out.getTransportVersion().supports(FIELD_CAPS_INFERENCE_INFO)) {
            out.writeOptionalString(inferenceId);
            out.writeOptionalString(searchInferenceId);
            out.writeOptionalStringArray(inferenceConflictsIndices);
        }
        out.writeMap(meta, StreamOutput::writeStringCollection);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(IS_METADATA_FIELD.getPreferredName(), isMetadataField);
        builder.field(SEARCHABLE_FIELD.getPreferredName(), isSearchable);
        builder.field(AGGREGATABLE_FIELD.getPreferredName(), isAggregatable);
        if (isDimension) {
            builder.field(TIME_SERIES_DIMENSION_FIELD.getPreferredName(), true);
        }
        if (metricType != null) {
            builder.field(TIME_SERIES_METRIC_FIELD.getPreferredName(), metricType);
        }
        if (indices != null) {
            builder.array(INDICES_FIELD.getPreferredName(), indices);
        }
        if (nonSearchableIndices != null) {
            builder.array(NON_SEARCHABLE_INDICES_FIELD.getPreferredName(), nonSearchableIndices);
        }
        if (nonAggregatableIndices != null) {
            builder.array(NON_AGGREGATABLE_INDICES_FIELD.getPreferredName(), nonAggregatableIndices);
        }
        if (nonDimensionIndices != null) {
            builder.array(NON_DIMENSION_INDICES_FIELD.getPreferredName(), nonDimensionIndices);
        }
        if (metricConflictsIndices != null) {
            builder.array(METRIC_CONFLICTS_INDICES_FIELD.getPreferredName(), metricConflictsIndices);
        }
        if (hasInferenceInfo()) {
            builder.startObject(INFERENCE_FIELD.getPreferredName());
            if (inferenceId != null) {
                builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
            }
            if (searchInferenceId != null) {
                builder.field(SEARCH_INFERENCE_ID_FIELD.getPreferredName(), searchInferenceId);
            }
            if (inferenceConflictsIndices != null) {
                builder.array(INFERENCE_CONFLICTS_INDICES_FIELD.getPreferredName(), inferenceConflictsIndices);
            }
            builder.endObject();
        }
        if (meta.isEmpty() == false) {
            builder.startObject("meta");
            List<Map.Entry<String, Set<String>>> entries = new ArrayList<>(meta.entrySet());
            entries.sort(Map.Entry.comparingByKey()); // provide predictable order
            for (Map.Entry<String, Set<String>> entry : entries) {
                String[] values = entry.getValue().toArray(Strings.EMPTY_ARRAY);
                Arrays.sort(values, String::compareTo); // provide predictable order
                builder.array(entry.getKey(), values);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private boolean hasInferenceInfo() {
        return inferenceId != null || searchInferenceId != null || inferenceConflictsIndices != null;
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * Whether this field is a metadata field.
     */
    public boolean isMetadataField() {
        return isMetadataField;
    }

    /**
     * Whether this field can be aggregated on all indices.
     */
    public boolean isAggregatable() {
        return isAggregatable;
    }

    /**
     * Whether this field is indexed for search on all indices.
     */
    public boolean isSearchable() {
        return isSearchable;
    }

    /**
     * Whether this field is a dimension in any indices.
     */
    public boolean isDimension() {
        return isDimension;
    }

    /**
     * The metric type
     */
    public TimeSeriesParams.MetricType getMetricType() {
        return metricType;
    }

    /**
     * The type of the field.
     */
    public String getType() {
        return type;
    }

    /**
     * The list of indices where this field name is defined as {@code type},
     * or null if all indices have the same {@code type} for the field.
     */
    public String[] indices() {
        return indices;
    }

    /**
     * The list of indices where this field is not searchable,
     * or null if the field is searchable in all indices.
     */
    public String[] nonSearchableIndices() {
        return nonSearchableIndices;
    }

    /**
     * The list of indices where this field is not aggregatable,
     * or null if the field is aggregatable in all indices.
     */
    public String[] nonAggregatableIndices() {
        return nonAggregatableIndices;
    }

    /**
     * The list of indices where this field has different dimension or metric flag
     */
    public String[] nonDimensionIndices() {
        return nonDimensionIndices;
    }

    /**
     * The list of indices where this field has different dimension or metric flag
     */
    public String[] metricConflictsIndices() {
        return metricConflictsIndices;
    }

    /**
     * The id of the inference endpoint that backs this field, present only when every index that maps the field maps it as
     * inference-backed and they all agree on the inference id. Returns null otherwise.
     */
    @Nullable
    public String getInferenceId() {
        return inferenceId;
    }

    /**
     * The id of the inference endpoint used at query time, present only when every index that maps the field maps it as
     * inference-backed, they all agree on the search inference id, and that id differs from {@link #getInferenceId()}.
     */
    @Nullable
    public String getSearchInferenceId() {
        return searchInferenceId;
    }

    /**
     * The list of indices that map this field as inference-backed but use a different inference id (or search inference id) than
     * the others. Null when there is no such conflict.
     */
    @Nullable
    public String[] inferenceConflictsIndices() {
        return inferenceConflictsIndices;
    }

    /**
     * Return merged metadata across indices.
     */
    public Map<String, Set<String>> meta() {
        return meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilities that = (FieldCapabilities) o;
        return isMetadataField == that.isMetadataField
            && isSearchable == that.isSearchable
            && isAggregatable == that.isAggregatable
            && isDimension == that.isDimension
            && Objects.equals(metricType, that.metricType)
            && Objects.equals(name, that.name)
            && Objects.equals(type, that.type)
            && Arrays.equals(indices, that.indices)
            && Arrays.equals(nonSearchableIndices, that.nonSearchableIndices)
            && Arrays.equals(nonAggregatableIndices, that.nonAggregatableIndices)
            && Arrays.equals(nonDimensionIndices, that.nonDimensionIndices)
            && Arrays.equals(metricConflictsIndices, that.metricConflictsIndices)
            && Objects.equals(inferenceId, that.inferenceId)
            && Objects.equals(searchInferenceId, that.searchInferenceId)
            && Arrays.equals(inferenceConflictsIndices, that.inferenceConflictsIndices)
            && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            name,
            type,
            isMetadataField,
            isSearchable,
            isAggregatable,
            isDimension,
            metricType,
            inferenceId,
            searchInferenceId,
            meta
        );
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(nonSearchableIndices);
        result = 31 * result + Arrays.hashCode(nonAggregatableIndices);
        result = 31 * result + Arrays.hashCode(nonDimensionIndices);
        result = 31 * result + Arrays.hashCode(metricConflictsIndices);
        result = 31 * result + Arrays.hashCode(inferenceConflictsIndices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Container for the {@code inference} sub-object on a {@link FieldCapabilities} entry. Only used by the parser; not part of the
     * wire protocol.
     */
    public record InferenceCapabilities(
        @Nullable String inferenceId,
        @Nullable String searchInferenceId,
        @Nullable List<String> inferenceConflictsIndices
    ) {}

    static class Builder {
        private final String name;
        private final String type;
        private boolean isMetadataField;
        private int searchableIndices = 0;
        private int aggregatableIndices = 0;
        private int dimensionIndices = 0;
        private TimeSeriesParams.MetricType metricType;
        private boolean hasConflictMetricType;
        private final List<IndexCaps> indicesList;
        private final Map<String, Set<String>> meta;
        private int totalIndices;
        // Inference accumulation state.
        // The first index sets the baseline. Subsequent indices either match (no change), introduce a different inference id
        // (conflict — record the indices), or have no inference info (mixed — drops the whole inference object at build time).
        private boolean inferenceSeen;
        private String firstInferenceId;
        private String firstSearchInferenceId;
        private boolean hasNonInferenceIndex;
        private boolean hasConflictInferenceId;

        Builder(String name, String type) {
            this.name = name;
            this.type = type;
            this.metricType = null;
            this.hasConflictMetricType = false;
            this.indicesList = new ArrayList<>();
            this.meta = new HashMap<>();
        }

        private boolean assertIndicesSorted(String[] indices) {
            for (int i = 1; i < indices.length; i++) {
                assert indices[i - 1].compareTo(indices[i]) < 0 : "indices [" + Arrays.toString(indices) + "] aren't sorted";
            }
            if (indicesList.isEmpty() == false) {
                final IndexCaps lastCaps = indicesList.get(indicesList.size() - 1);
                final String lastIndex = lastCaps.indices[lastCaps.indices.length - 1];
                assert lastIndex.compareTo(indices[0]) < 0
                    : "indices aren't sorted; previous [" + lastIndex + "], current [" + indices[0] + "]";
            }
            return true;
        }

        /**
         * Collect the field capabilities for an index. Convenience overload for callers that never deal with inference info.
         */
        void add(
            String[] indices,
            boolean isMetadataField,
            boolean search,
            boolean agg,
            boolean isDimension,
            TimeSeriesParams.MetricType metricType,
            Map<String, String> meta
        ) {
            add(indices, isMetadataField, search, agg, isDimension, metricType, null, null, meta);
        }

        /**
         * Collect the field capabilities for an index, including the inference ids when the field is backed by an inference endpoint.
         */
        void add(
            String[] indices,
            boolean isMetadataField,
            boolean search,
            boolean agg,
            boolean isDimension,
            TimeSeriesParams.MetricType metricType,
            @Nullable String inferenceId,
            @Nullable String searchInferenceId,
            Map<String, String> meta
        ) {
            assert assertIndicesSorted(indices);
            totalIndices += indices.length;
            if (search) {
                searchableIndices += indices.length;
            }
            if (agg) {
                aggregatableIndices += indices.length;
            }
            if (isDimension) {
                dimensionIndices += indices.length;
            }
            this.isMetadataField |= isMetadataField;
            // If we have discrepancy in metric types or in some indices this field is not marked as a metric field - we will
            // treat is a non-metric field and report this discrepancy in metricConflictsIndices
            if (indicesList.isEmpty()) {
                this.metricType = metricType;
            } else if (this.metricType != metricType) {
                hasConflictMetricType = true;
                this.metricType = null;
            }
            // Track inference info across indices: the field is reported as inference-backed only when every index that maps it
            // is inference-backed. Different inference ids across indices become a conflict reported per-index.
            if (inferenceId == null) {
                hasNonInferenceIndex = true;
            } else if (inferenceSeen == false) {
                inferenceSeen = true;
                firstInferenceId = inferenceId;
                firstSearchInferenceId = searchInferenceId;
            } else if (Objects.equals(firstInferenceId, inferenceId) == false
                || Objects.equals(firstSearchInferenceId, searchInferenceId) == false) {
                    hasConflictInferenceId = true;
                }
            indicesList.add(new IndexCaps(indices, search, agg, isDimension, metricType, inferenceId, searchInferenceId));
            for (Map.Entry<String, String> entry : meta.entrySet()) {
                this.meta.computeIfAbsent(entry.getKey(), key -> new HashSet<>()).add(entry.getValue());
            }
        }

        void getIndices(Set<String> into) {
            for (int i = 0; i < indicesList.size(); i++) {
                IndexCaps indexCaps = indicesList.get(i);
                for (String element : indexCaps.indices) {
                    into.add(element);
                }
            }
        }

        private String[] filterIndices(int length, Predicate<IndexCaps> pred) {
            int index = 0;
            final String[] dst = new String[length];
            for (IndexCaps indexCaps : indicesList) {
                if (pred.test(indexCaps)) {
                    System.arraycopy(indexCaps.indices, 0, dst, index, indexCaps.indices.length);
                    index += indexCaps.indices.length;
                }
            }
            assert index == length : index + "!=" + length;
            return dst;
        }

        FieldCapabilities build(boolean withIndices) {
            final String[] indices = withIndices ? filterIndices(totalIndices, Predicates.always()) : null;

            // Iff this field is searchable in some indices AND non-searchable in others
            // we record the list of non-searchable indices
            final boolean isSearchable = searchableIndices == totalIndices;
            final String[] nonSearchableIndices;
            if (isSearchable || searchableIndices == 0) {
                nonSearchableIndices = null;
            } else {
                nonSearchableIndices = filterIndices(totalIndices - searchableIndices, ic -> ic.isSearchable == false);
            }

            // Iff this field is aggregatable in some indices AND non-aggregatable in others
            // we keep the list of non-aggregatable indices
            final boolean isAggregatable = aggregatableIndices == totalIndices;
            final String[] nonAggregatableIndices;
            if (isAggregatable || aggregatableIndices == 0) {
                nonAggregatableIndices = null;
            } else {
                nonAggregatableIndices = filterIndices(totalIndices - aggregatableIndices, ic -> ic.isAggregatable == false);
            }

            // Collect all indices that have dimension == false if this field is marked as a dimension in at least one index
            final boolean isDimension = dimensionIndices == totalIndices;
            final String[] nonDimensionIndices;
            if (isDimension || dimensionIndices == 0) {
                nonDimensionIndices = null;
            } else {
                nonDimensionIndices = filterIndices(totalIndices - dimensionIndices, ic -> ic.isDimension == false);
            }

            final String[] metricConflictsIndices;
            if (hasConflictMetricType) {
                // Collect all indices that have this field. If it is marked differently in different indices, we cannot really
                // make a decisions which index is "right" and which index is "wrong" so collecting all indices where this field
                // is present is probably the only sensible thing to do here
                metricConflictsIndices = Objects.requireNonNullElseGet(indices, () -> filterIndices(totalIndices, Predicates.always()));
            } else {
                metricConflictsIndices = null;
            }

            // Inference output: emit only if every index agrees this field is inference-backed; on conflicting ids, emit the
            // inference object with the inference id omitted and a per-index conflict array.
            final String resolvedInferenceId;
            final String resolvedSearchInferenceId;
            final String[] resolvedInferenceConflictsIndices;
            if (inferenceSeen && hasNonInferenceIndex == false) {
                if (hasConflictInferenceId) {
                    resolvedInferenceId = null;
                    resolvedSearchInferenceId = null;
                    resolvedInferenceConflictsIndices = filterIndices(totalIndices, ic -> ic.inferenceId != null);
                } else {
                    resolvedInferenceId = firstInferenceId;
                    resolvedSearchInferenceId = Objects.equals(firstInferenceId, firstSearchInferenceId) ? null : firstSearchInferenceId;
                    resolvedInferenceConflictsIndices = null;
                }
            } else {
                resolvedInferenceId = null;
                resolvedSearchInferenceId = null;
                resolvedInferenceConflictsIndices = null;
            }

            final Function<Map.Entry<String, Set<String>>, Set<String>> entryValueFunction = Map.Entry::getValue;
            Map<String, Set<String>> immutableMeta = meta.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entryValueFunction.andThen(Set::copyOf)));
            return new FieldCapabilities(
                name,
                type,
                isMetadataField,
                isSearchable,
                isAggregatable,
                isDimension,
                metricType,
                indices,
                nonSearchableIndices,
                nonAggregatableIndices,
                nonDimensionIndices,
                metricConflictsIndices,
                resolvedInferenceId,
                resolvedSearchInferenceId,
                resolvedInferenceConflictsIndices,
                immutableMeta
            );
        }
    }

    private record IndexCaps(
        String[] indices,
        boolean isSearchable,
        boolean isAggregatable,
        boolean isDimension,
        TimeSeriesParams.MetricType metricType,
        @Nullable String inferenceId,
        @Nullable String searchInferenceId
    ) {}
}
