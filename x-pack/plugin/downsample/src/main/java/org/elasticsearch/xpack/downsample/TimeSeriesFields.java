/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.admin.cluster.stats.MappingVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.PassThroughObjectMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM;
import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_METRIC_PARAM;

/**
 * This class contains the metric, dimension and label fields that can be downsampled. These fields will be used to retrieve the values
 * from the original document and create the downsampled document.
 *
 * In most cases, the name of the field will suffice; However, when we have multi-fields there are some edge cases that require to load the
 * value from a sub-field:
 * - When a label is a multi-field, it is possible that the parent field is not aggregatable, in this case, we will use an aggregatable
 * sub-field, if it exists, as an alternative source.
 * - Dimensions have priority over labels, so if a label field has a dimension subfield, we categorise the field as a dimension and use
 * that as source.
 * The parent fields which values will be sourced by a sub-field are listed in `subFieldSources`.
 * In the following example, `my_text` is label but the value will be sourced using the field data of `my_text.keyword`:
 * ```
 *    "my-text": {
 *         "type": "text",
 *         "fields": {
 *             "keyword": {
 *                 "type": "keyword"
 *             }
 *         }
 *     }
 * ```
 * In another example, `my_label` is label, but it contains a dimension sub-field which has priority. The `my_label` will be considered a
 * dimension and `my_label.dimension` will be used to retrieve the value:
 * ```
 *     "my-label": {
 *         "type": "keyword",
 *         "fields": {
 *             "dimension": {
 *                 "type": "keyword",
 *                 "time_series_dimension": true
 *             }
 *         }
 *     }
 * ```
 */
record TimeSeriesFields(String[] metricFields, String[] dimensionFields, String[] labelFields, Map<String, String> multiFieldSources) {
    public static final Set<TimeSeriesParams.MetricType> METRIC_TYPES = Set.of(TimeSeriesParams.MetricType.values());

    static boolean isTimeSeriesLabel(final String field, MapperService mapperService, String timestampField) {
        final MappingLookup lookup = mapperService.mappingLookup();
        final MappedFieldType fieldType = lookup.getFieldType(field);
        return fieldType != null
            && (timestampField.equals(field) == false)
            && (fieldType.isAggregatable())
            && (fieldType.isDimension() == false)
            && (mapperService.isMetadataField(field) == false);
    }

    static boolean isTimeSeriesMetric(final Map<String, ?> fieldMapping) {
        final String metricType = (String) fieldMapping.get(TIME_SERIES_METRIC_PARAM);
        return metricType != null && METRIC_TYPES.contains(TimeSeriesParams.MetricType.fromString(metricType));
    }

    static class Collector {
        private final MapperService mapperService;
        private final String timestampField;
        private final Set<String> metricFields = new HashSet<>();
        private final Set<String> dimensionFields = new HashSet<>();
        private final Set<String> labelFields = new HashSet<>();
        private final Map<String, String> alternativeSources = new HashMap<>();

        Collector(final MapperService mapperService, final String timestampField) {
            this.mapperService = mapperService;
            this.timestampField = timestampField;
        }

        public TimeSeriesFields collect(Map<String, ?> mapping) {
            MappingVisitor.visitMapping(mapping, this::trackFields, this::trackMultiFields);
            return new TimeSeriesFields(
                metricFields.toArray(String[]::new),
                dimensionFields.toArray(String[]::new),
                labelFields.toArray(String[]::new),
                Collections.unmodifiableMap(alternativeSources)
            );
        }

        private void trackFields(String field, Map<String, ?> mapping) {
            var flattenedDimensions = extractFlattenedDimensions(field, mapping);
            if (flattenedDimensions != null) {
                dimensionFields.addAll(flattenedDimensions);
            } else if (isTimeSeriesDimension(mapping)) {
                dimensionFields.add(field);
            } else if (isTimeSeriesMetric(mapping)) {
                metricFields.add(field);
            } else if (isTimeSeriesLabel(field)) {
                labelFields.add(field);
            }
        }

        private void trackMultiFields(String field, Map<String, ?> mapping) {
            if (isTimeSeriesMetric(mapping)) {
                throw new IllegalArgumentException(
                    "Downsampling failed because index mapping contains time series metrics as a multi field [" + field + "]"
                );
            }
            String parentField = field.substring(0, field.lastIndexOf('.'));
            // If the parent field is a dimension, we consider it already tracked.
            if (dimensionFields.contains(parentField) || metricFields.contains(parentField)) {
                return;
            }
            // We do not check for flattened dimension subfields because even if it is accepted as a valid mapping
            // indexing a document fails.
            if (isTimeSeriesDimension(mapping)) {
                dimensionFields.add(parentField);
                labelFields.remove(parentField);
                alternativeSources.put(parentField, field);
            } else if (isTimeSeriesLabel(field) && labelFields.contains(parentField) == false) {
                labelFields.add(parentField);
                alternativeSources.put(parentField, field);
            }
        }

        public boolean isTimeSeriesLabel(final String field) {
            return TimeSeriesFields.isTimeSeriesLabel(field, mapperService, timestampField);
        }

        public boolean isTimeSeriesDimension(final Map<String, ?> fieldMapping) {
            return Boolean.TRUE.equals(fieldMapping.get(TIME_SERIES_DIMENSION_PARAM)) && isPassthroughField(fieldMapping) == false;
        }

        public static boolean isPassthroughField(final Map<String, ?> fieldMapping) {
            return PassThroughObjectMapper.CONTENT_TYPE.equals(fieldMapping.get(ContextMapping.FIELD_TYPE));
        }

        public List<String> extractFlattenedDimensions(final String field, final Map<String, ?> fieldMapping) {
            var mapper = mapperService.mappingLookup().getMapper(field);
            if (mapper instanceof FlattenedFieldMapper == false) {
                return null;
            }
            Object dimensions = fieldMapping.get(FlattenedFieldMapper.TIME_SERIES_DIMENSIONS_ARRAY_PARAM);
            if (dimensions instanceof List<?> actualList) {
                return actualList.stream().map(field_in_flattened -> field + '.' + field_in_flattened).toList();
            }
            return null;
        }
    }
}
