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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class IndexFieldCapabilitiesBuilder {
    private final String name;
    private final String type;

    private boolean isMetadataField;
    private boolean isSearchable;
    private boolean isAggregatable;
    private boolean isDimension;
    private @Nullable TimeSeriesParams.MetricType metricType;
    private Map<String, String> meta;

    public IndexFieldCapabilitiesBuilder(String name, String type) {
        this.name = name;
        this.type = type;

        this.isSearchable = true;
        this.isAggregatable = true;

        this.meta = Collections.emptyMap();
    }

    public IndexFieldCapabilitiesBuilder isMetadataField(boolean isMetadataField) {
        this.isMetadataField = isMetadataField;
        return this;
    }

    public IndexFieldCapabilitiesBuilder isSearchable(boolean isSearchable) {
        this.isSearchable = isSearchable;
        return this;
    }

    public IndexFieldCapabilitiesBuilder isAggregatable(boolean isAggregatable) {
        this.isAggregatable = isAggregatable;
        return this;
    }

    public IndexFieldCapabilitiesBuilder isDimension(boolean isDimension) {
        this.isDimension = isDimension;
        return this;
    }

    public IndexFieldCapabilitiesBuilder metricType(TimeSeriesParams.MetricType metricType) {
        this.metricType = metricType;
        return this;
    }

    public IndexFieldCapabilitiesBuilder meta(@Nullable Map<String, String> meta) {
        this.meta = meta != null ? new TreeMap<>(meta) : null;
        return this;
    }

    public IndexFieldCapabilities build() {
        return new IndexFieldCapabilities(name, type, isMetadataField, isSearchable, isAggregatable, isDimension, metricType, meta);
    }
}
