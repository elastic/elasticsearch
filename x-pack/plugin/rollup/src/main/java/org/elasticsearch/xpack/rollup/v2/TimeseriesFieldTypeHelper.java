/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.Arrays;
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
        final MappedFieldType fieldType = mapperService.mappingLookup().getFieldType(field);
        return fieldType != null
            && (timestampField.equals(field) == false)
            && (fieldType.isAggregatable())
            && (fieldType.isDimension() == false)
            && (mapperService.isMetadataField(field) == false);
    }

    public boolean isTimeSeriesMetric(final String unused, final Map<String, ?> fieldMapping) {
        final String metricType = (String) fieldMapping.get(TIME_SERIES_METRIC_PARAM);
        return metricType != null
            && Arrays.asList(TimeSeriesParams.MetricType.values()).contains(TimeSeriesParams.MetricType.valueOf(metricType));
    }

    public boolean isTimeSeriesDimension(final String unused, final Map<String, ?> fieldMapping) {
        return Boolean.TRUE.equals(fieldMapping.get(TIME_SERIES_DIMENSION_PARAM));
    }

    static class Builder {
        private final IndicesService indicesService;
        private final Map<String, Object> indexMapping;
        private final IndexMetadata indexMetadata;

        Builder(final IndicesService indicesService, final Map<String, Object> indexMapping, final IndexMetadata indexMetadata) {
            this.indicesService = indicesService;
            this.indexMapping = indexMapping;
            this.indexMetadata = indexMetadata;
        }

        public TimeseriesFieldTypeHelper build(final String timestampField) throws IOException {
            final MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata);
            final CompressedXContent sourceIndexCompressedXContent = new CompressedXContent(indexMapping);
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, sourceIndexCompressedXContent, MapperService.MergeReason.INDEX_TEMPLATE);
            return new TimeseriesFieldTypeHelper(mapperService, timestampField);
        }
    }
}
