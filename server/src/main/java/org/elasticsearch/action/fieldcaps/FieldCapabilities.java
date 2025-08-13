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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
     * @param indices The list of indices where this field name is defined as {@code type},
     *                or null if all indices have the same {@code type} for the field.
     * @param nonSearchableIndices The list of indices where this field is not searchable,
     *                             or null if the field is searchable in all indices.
     * @param nonAggregatableIndices The list of indices where this field is not aggregatable,
     *                               or null if the field is aggregatable in all indices.
     * @param nonDimensionIndices The list of indices where this field is not a dimension
     * @param metricConflictsIndices The list of indices where this field is has different metric types or not mark as a metric
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
        this.meta = Objects.requireNonNull(meta);
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
            meta != null ? meta : Collections.emptyMap()
        );
    }

    FieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.isMetadataField = in.readBoolean();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
            this.isDimension = in.readBoolean();
            this.metricType = in.readOptionalEnum(TimeSeriesParams.MetricType.class);
        } else {
            this.isDimension = false;
            this.metricType = null;
        }
        this.indices = in.readOptionalStringArray();
        this.nonSearchableIndices = in.readOptionalStringArray();
        this.nonAggregatableIndices = in.readOptionalStringArray();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
            this.nonDimensionIndices = in.readOptionalStringArray();
            this.metricConflictsIndices = in.readOptionalStringArray();
        } else {
            this.nonDimensionIndices = null;
            this.metricConflictsIndices = null;
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
            out.writeBoolean(isDimension);
            out.writeOptionalEnum(metricType);
        }
        out.writeOptionalStringArray(indices);
        out.writeOptionalStringArray(nonSearchableIndices);
        out.writeOptionalStringArray(nonAggregatableIndices);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
            out.writeOptionalStringArray(nonDimensionIndices);
            out.writeOptionalStringArray(metricConflictsIndices);
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
            && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, type, isMetadataField, isSearchable, isAggregatable, isDimension, metricType, meta);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(nonSearchableIndices);
        result = 31 * result + Arrays.hashCode(nonAggregatableIndices);
        result = 31 * result + Arrays.hashCode(nonDimensionIndices);
        result = 31 * result + Arrays.hashCode(metricConflictsIndices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

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
         * Collect the field capabilities for an index.
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
            indicesList.add(new IndexCaps(indices, search, agg, isDimension, metricType));
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
                immutableMeta
            );
        }
    }

    private record IndexCaps(
        String[] indices,
        boolean isSearchable,
        boolean isAggregatable,
        boolean isDimension,
        TimeSeriesParams.MetricType metricType
    ) {}
}
