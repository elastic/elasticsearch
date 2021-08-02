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
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.List;
import java.util.Map;

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

    static final List<Setting<?>> VALIDATE_WITH_SETTINGS = List.of(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING);

    abstract void validateWithOtherSettings(Map<Setting<?>, Object> settings);

    public abstract boolean organizeIntoTimeSeries();

    public abstract void checkDocWriteRequest(DocWriteRequest.OpType opType, String indexName);
}
