/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * "Mode" that controls which behaviors and settings an index supports.
 * <p>
 * For the most part this class concentrates on validating settings and
 * mappings. Most different behavior is controlled by forcing settings
 * to be set or not set and by enabling extra fields in the mapping.
 */
public enum IndexMode {
    STANDARD {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            settingRequiresTimeSeries(settings, IndexMetadata.INDEX_ROUTING_PATH);
            settingRequiresTimeSeries(settings, IndexSettings.TIME_SERIES_START_TIME);
            settingRequiresTimeSeries(settings, IndexSettings.TIME_SERIES_END_TIME);
        }

        private void settingRequiresTimeSeries(Map<Setting<?>, Object> settings, Setting<?> setting) {
            if (false == Objects.equals(setting.getDefault(Settings.EMPTY), settings.get(setting))) {
                throw new IllegalArgumentException("[" + setting.getKey() + "] requires [" + IndexSettings.MODE.getKey() + "=time_series]");
            }
        }

        @Override
        public void validateMapping(MappingLookup lookup) {};

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {}

        @Override
        public void completeMappings(MappingParserContext context, Map<String, Object> mapping, RootObjectMapper.Builder builder) {}
    },
    TIME_SERIES {
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
            settingRequiresTimeSeries(settings, IndexMetadata.INDEX_ROUTING_PATH);
            // TODO make start and stop time required
        }

        private void settingRequiresTimeSeries(Map<Setting<?>, Object> settings, Setting<?> setting) {
            if (Objects.equals(setting.getDefault(Settings.EMPTY), settings.get(setting))) {
                throw new IllegalArgumentException("[" + IndexSettings.MODE.getKey() + "=time_series] requires [" + setting.getKey() + "]");
            }
        }

        private String error(Setting<?> unsupported) {
            return tsdbMode() + " is incompatible with [" + unsupported.getKey() + "]";
        }

        @Override
        public void validateMapping(MappingLookup lookup) {
            if (((RoutingFieldMapper) lookup.getMapper(RoutingFieldMapper.NAME)).required()) {
                throw new IllegalArgumentException(routingRequiredBad());
            }
        }

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {
            if (indexRouting != null || searchRouting != null) {
                throw new IllegalArgumentException(routingRequiredBad());
            }
        }

        private String routingRequiredBad() {
            return "routing is forbidden on CRUD operations that target indices in " + tsdbMode();
        }

        private String tsdbMode() {
            return "[" + IndexSettings.MODE.getKey() + "=time_series]";
        }

        @Override
        public void completeMappings(MappingParserContext context, Map<String, Object> mapping, RootObjectMapper.Builder builder) {
            if (false == mapping.containsKey(DataStreamTimestampFieldMapper.NAME)) {
                mapping.put(DataStreamTimestampFieldMapper.NAME, new HashMap<>(Map.of("enabled", true)));
            } else {
                validateTimeStampField(mapping.get(DataStreamTimestampFieldMapper.NAME));
            }

            Optional<Mapper.Builder> timestamp = builder.getBuilder(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            if (timestamp.isEmpty()) {
                builder.add(
                    new DateFieldMapper.Builder(
                        DataStreamTimestampFieldMapper.DEFAULT_PATH,
                        DateFieldMapper.Resolution.MILLISECONDS,
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                        context.scriptCompiler(),
                        DateFieldMapper.IGNORE_MALFORMED_SETTING.get(context.getSettings()),
                        context.getIndexSettings().getIndexVersionCreated()
                    )
                );
            }
        }

        private void validateTimeStampField(Object timestampFieldValue) {
            if (false == (timestampFieldValue instanceof Map)) {
                throw new IllegalArgumentException(
                    "time series index [" + DataStreamTimestampFieldMapper.NAME + "] meta field format error"
                );
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> timeStampFieldValueMap = (Map<String, Object>) timestampFieldValue;
            if (false == Maps.deepEquals(timeStampFieldValueMap, Map.of("enabled", true))
                && false == Maps.deepEquals(timeStampFieldValueMap, Map.of("enabled", "true"))) {
                throw new IllegalArgumentException(
                    "time series index [" + DataStreamTimestampFieldMapper.NAME + "] meta field must be enabled"
                );
            }
        }
    };

    private static final List<Setting<?>> TIME_SERIES_UNSUPPORTED = List.of(
        IndexSortConfig.INDEX_SORT_FIELD_SETTING,
        IndexSortConfig.INDEX_SORT_ORDER_SETTING,
        IndexSortConfig.INDEX_SORT_MODE_SETTING,
        IndexSortConfig.INDEX_SORT_MISSING_SETTING
    );

    static final List<Setting<?>> VALIDATE_WITH_SETTINGS = List.copyOf(
        Stream.concat(
            Stream.of(
                IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING,
                IndexMetadata.INDEX_ROUTING_PATH,
                IndexSettings.TIME_SERIES_START_TIME,
                IndexSettings.TIME_SERIES_END_TIME
            ),
            TIME_SERIES_UNSUPPORTED.stream()
        ).collect(toSet())
    );

    abstract void validateWithOtherSettings(Map<Setting<?>, Object> settings);

    /**
     * Validate the mapping for this index.
     */
    public abstract void validateMapping(MappingLookup lookup);

    /**
     * Validate aliases targeting this index.
     */
    public abstract void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting);

    /**
     * Validate and/or modify the mappings after after they've been parsed.
     */
    public abstract void completeMappings(MappingParserContext context, Map<String, Object> mapping, RootObjectMapper.Builder builder);
}
