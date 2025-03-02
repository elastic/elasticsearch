/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FieldCapabilitiesBuilder {
    private final String name;
    private final String type;

    private boolean isMetadataField;
    private boolean isSearchable;
    private boolean isAggregatable;
    private boolean isDimension;
    private @Nullable TimeSeriesParams.MetricType metricType;

    private @Nullable String[] indices;
    private @Nullable String[] nonSearchableIndices;
    private @Nullable String[] nonAggregatableIndices;
    private @Nullable String[] nonDimensionIndices;
    private @Nullable String[] metricConflictsIndices;

    private Map<String, Set<String>> meta;

    public FieldCapabilitiesBuilder(String name, String type) {
        this.name = name;
        this.type = type;

        this.isSearchable = true;
        this.isAggregatable = true;

        this.meta = Collections.emptyMap();
    }

    public FieldCapabilitiesBuilder isMetadataField(boolean isMetadataField) {
        this.isMetadataField = isMetadataField;
        return this;
    }

    public FieldCapabilitiesBuilder isSearchable(boolean isSearchable) {
        this.isSearchable = isSearchable;
        return this;
    }

    public FieldCapabilitiesBuilder isAggregatable(boolean isAggregatable) {
        this.isAggregatable = isAggregatable;
        return this;
    }

    public FieldCapabilitiesBuilder isDimension(boolean isDimension) {
        this.isDimension = isDimension;
        return this;
    }

    public FieldCapabilitiesBuilder metricType(TimeSeriesParams.MetricType metricType) {
        this.metricType = metricType;
        return this;
    }

    public FieldCapabilitiesBuilder indices(String... indices) {
        this.indices = Arrays.copyOf(indices, indices.length);
        return this;
    }

    public FieldCapabilitiesBuilder nonSearchableIndices(String... nonSearchableIndices) {
        this.nonSearchableIndices = nonSearchableIndices;
        return this;
    }

    public FieldCapabilitiesBuilder nonAggregatableIndices(String... nonAggregatableIndices) {
        this.nonAggregatableIndices = nonAggregatableIndices;
        return this;
    }

    public FieldCapabilitiesBuilder nonDimensionIndices(String... nonDimensionIndices) {
        this.nonDimensionIndices = nonDimensionIndices;
        return this;
    }

    public FieldCapabilitiesBuilder metricConflictsIndices(String... metricConflictsIndices) {
        this.metricConflictsIndices = metricConflictsIndices;
        return this;
    }

    public FieldCapabilitiesBuilder meta(Map<String, Set<String>> meta) {
        this.meta = new HashMap<>(meta);
        return this;
    }

    public FieldCapabilities build() {
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
            meta
        );
    }
}
