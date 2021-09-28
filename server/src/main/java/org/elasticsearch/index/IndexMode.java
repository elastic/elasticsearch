/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.core.TimeValue.NSEC_PER_MSEC;

/**
 * "Mode" that controls which behaviors and settings an index supports.
 */
public enum IndexMode {
    STANDARD {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {}

        @Override
        public void validateWithSource(DocumentParserContext context) {}
    },
    TIME_SERIES {
        public static final String TIMESTAMP_FIELD = "@timestamp";

        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            if (settings.get(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING) != Integer.valueOf(1)) {
                throw new IllegalArgumentException(error(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING));
            }
            for (Setting<?> unsupported : TIME_SERIES_UNSUPPORTED) {
                if (false == Objects.equals(unsupported.getDefault(Settings.EMPTY), settings.get(unsupported))) {
                    throw new IllegalArgumentException(error(unsupported));
                }
            }
        }

        @Override
        public void validateWithSource(DocumentParserContext context) {
            validateTimestamp(context);
        }

        private void validateTimestamp(DocumentParserContext context) {
            IndexableField[] fields = context.rootDoc().getFields(TIMESTAMP_FIELD);
            if (fields.length == 0) {
                throw new IllegalArgumentException("time series index @timestamp field is missing");
            }

            long numberOfValues = Arrays.stream(fields)
                .filter(indexableField -> indexableField.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC)
                .count();
            if (numberOfValues > 1) {
                throw new IllegalArgumentException("time series index @timestamp field encountered multiple values");
            }

            long timestamp = fields[0].numericValue().longValue();
            if (context.mappingLookup().getMapper(TIMESTAMP_FIELD).typeName().equals(DateFieldMapper.DATE_NANOS_CONTENT_TYPE)) {
                timestamp /= NSEC_PER_MSEC;
            }

            long startTime = context.indexSettings().getTimeSeriesStartTime();
            long endTime = context.indexSettings().getTimeSeriesEndTime();

            if (timestamp < startTime) {
                throw new IllegalArgumentException(
                    "time series index @timestamp value [" + timestamp + "] must be larger than " + startTime
                );
            }

            if (timestamp > endTime) {
                throw new IllegalArgumentException(
                    "time series index @timestamp value [" + timestamp + "] must be smaller than " + endTime
                );
            }
        }

        private String error(Setting<?> unsupported) {
            return "[" + IndexSettings.MODE.getKey() + "=time_series] is incompatible with [" + unsupported.getKey() + "]";
        }
    };

    private static final List<Setting<?>> TIME_SERIES_UNSUPPORTED = List.of(
        IndexSortConfig.INDEX_SORT_FIELD_SETTING,
        IndexSortConfig.INDEX_SORT_ORDER_SETTING,
        IndexSortConfig.INDEX_SORT_MODE_SETTING,
        IndexSortConfig.INDEX_SORT_MISSING_SETTING
    );

    static final List<Setting<?>> VALIDATE_WITH_SETTINGS = List.copyOf(
        Stream.concat(Stream.of(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING), TIME_SERIES_UNSUPPORTED.stream()).collect(toSet())
    );

    abstract void validateWithOtherSettings(Map<Setting<?>, Object> settings);

    public abstract void validateWithSource(DocumentParserContext context);
}
