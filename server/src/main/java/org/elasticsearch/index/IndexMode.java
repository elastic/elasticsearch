/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * The "mode" of the index.
 */
public enum IndexMode {
    /**
     * Elasticsearch's traditional, search engine-like mode.
     */
    STANDARD {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {}

        @Override
        public boolean organizeIntoTimeSeries() {
            return false;
        }

        @Override
        public void checkDocWriteRequest(OpType opType, String indexName) {}
    },
    /**
     * A mode for time series data which automatically organizes data using
     * the {@link MappedFieldType#isDimension() dimensions} to get better
     * storage efficiency and enable some additional operations.
     */
    TIME_SERIES {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            if (settings.get(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING) != Integer.valueOf(1)) {
                throw new IllegalArgumentException(
                    "["
                        + IndexSettings.MODE.getKey()
                        + "=time_series] is incompatible with ["
                        + IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.getKey()
                        + "]"
                );
            }
            for (Setting<?> unsupported : TIME_SERIES_UNSUPPORTED) {
                if (false == Objects.equals(unsupported.getDefault(Settings.EMPTY), settings.get(unsupported))) {
                    throw new IllegalArgumentException("Can't set [" + unsupported.getKey() + "] in time series mode");
                }
            }
        }

        @Override
        public boolean organizeIntoTimeSeries() {
            return true;
        }

        @Override
        public void checkDocWriteRequest(OpType opType, String indexName) {
            switch (opType) {
                case INDEX:
                case CREATE:
                    return;
                case DELETE:
                case UPDATE:
                    throw new IllegalArgumentException(
                        "[" + opType + "] is not supported because the destination index [" + indexName + "] is in time series mode"
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
        Stream.concat(Stream.of(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING), TIME_SERIES_UNSUPPORTED.stream()).collect(toSet())
    );

    abstract void validateWithOtherSettings(Map<Setting<?>, Object> settings);

    public abstract boolean organizeIntoTimeSeries();

    public abstract void checkDocWriteRequest(DocWriteRequest.OpType opType, String indexName);
}
