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
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.RootObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * "Mode" that controls which behaviors and settings an index supports.
 */
public enum IndexMode {
    STANDARD {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {}

        @Override
        public void completeMappings(MappingParserContext context, RootObjectMapper.Builder builder) {}
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
        }

        private String error(Setting<?> unsupported) {
            return "[" + IndexSettings.MODE.getKey() + "=time_series] is incompatible with [" + unsupported.getKey() + "]";
        }

        @Override
        public void completeMappings(MappingParserContext context, RootObjectMapper.Builder builder) {
            Optional<Mapper.Builder> timestamp = builder.getBuilder("@timestamp");
            if (timestamp.isEmpty()) {
                builder.add(
                    new DateFieldMapper.Builder(
                        "@timestamp",
                        DateFieldMapper.Resolution.MILLISECONDS,
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                        context.scriptCompiler(),
                        DateFieldMapper.IGNORE_MALFORMED_SETTING.get(context.getSettings()),
                        context.getIndexSettings().getIndexVersionCreated()
                    )
                );
                return;
            }
            if (false == timestamp.get() instanceof DateFieldMapper.Builder) {
                throw new IllegalArgumentException("@timestamp must be [date] or [date_nanos]");
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
        Stream.concat(Stream.of(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING), TIME_SERIES_UNSUPPORTED.stream()).collect(toSet())
    );

    abstract void validateWithOtherSettings(Map<Setting<?>, Object> settings);

    /**
     * Validate and/or modify the mappings after after they've been parsed.
     */
    public abstract void completeMappings(MappingParserContext context, RootObjectMapper.Builder builder);
}
