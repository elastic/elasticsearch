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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * "Mode" that controls which behaviors and settings an index supports.
 */
public enum IndexMode {
    STANDARD {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            if (false == Objects.equals(
                IndexMetadata.INDEX_ROUTING_PATH.getDefault(Settings.EMPTY),
                settings.get(IndexMetadata.INDEX_ROUTING_PATH)
            )) {
                throw new IllegalArgumentException(
                    "[" + IndexMetadata.INDEX_ROUTING_PATH.getKey() + "] requires [" + IndexSettings.MODE.getKey() + "=time_series]"
                );
            }
        }
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
            if (IndexMetadata.INDEX_ROUTING_PATH.getDefault(Settings.EMPTY).equals(settings.get(IndexMetadata.INDEX_ROUTING_PATH))) {
                throw new IllegalArgumentException(
                    "[" + IndexSettings.MODE.getKey() + "=time_series] requires [" + IndexMetadata.INDEX_ROUTING_PATH.getKey() + "]"
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
        Stream.concat(
            Stream.of(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING, IndexMetadata.INDEX_ROUTING_PATH),
            TIME_SERIES_UNSUPPORTED.stream()
        ).collect(toSet())
    );

    abstract void validateWithOtherSettings(Map<Setting<?>, Object> settings);
}
