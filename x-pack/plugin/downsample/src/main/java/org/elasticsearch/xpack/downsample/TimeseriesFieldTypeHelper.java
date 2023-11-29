/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM;
import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_METRIC_PARAM;

class TimeseriesFieldTypeHelper {

    private final MapperService mapperService;
    private final String timestampField;

    private TimeseriesFieldTypeHelper(final MapperService mapperService, final String timestampField) {
        this.mapperService = mapperService;
        this.timestampField = timestampField;
    }

    public boolean isTimeSeriesLabel(final String field, final Map<String, ?> unused) {
        final MappingLookup lookup = mapperService.mappingLookup();
        final MappedFieldType fieldType = lookup.getFieldType(field);
        return fieldType != null
            && (timestampField.equals(field) == false)
            && (fieldType.isAggregatable())
            && (fieldType.isDimension() == false)
            && (mapperService.isMetadataField(field) == false);
    }

    public boolean isTimeSeriesMetric(final String unused, final Map<String, ?> fieldMapping) {
        final String metricType = (String) fieldMapping.get(TIME_SERIES_METRIC_PARAM);
        return metricType != null
            && List.of(TimeSeriesParams.MetricType.values()).contains(TimeSeriesParams.MetricType.fromString(metricType));
    }

    public boolean isTimeSeriesDimension(final String unused, final Map<String, ?> fieldMapping) {
        return Boolean.TRUE.equals(fieldMapping.get(TIME_SERIES_DIMENSION_PARAM));
    }

    static class Builder {
        private final MapperService mapperService;

        Builder(final MapperService mapperService) {
            this.mapperService = mapperService;
        }

        public TimeseriesFieldTypeHelper build(final String timestampField) throws IOException {
            return new TimeseriesFieldTypeHelper(mapperService, timestampField);
        }
    }
}
