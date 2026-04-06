/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Set;

/**
 * Configuration needed to transform loaded values into blocks.
 * {@link MappedFieldType}s will find me in
 * {@link MappedFieldType.BlockLoaderContext#blockLoaderFunctionConfig()} and
 * use this configuration to choose the appropriate implementation for
 * transforming loaded values into blocks.
 */
public interface BlockLoaderFunctionConfig {
    /**
     * Name used in descriptions.
     */
    Function function();

    record JustFunction(Function function) implements BlockLoaderFunctionConfig {}

    /**
     * Configuration for loading time-series metadata fields from {@code _source}.
     * Controls which field types to include (dimensions, metrics, or both) and which dimensions to exclude.
     */
    record TimeSeriesMetadata(boolean loadMetrics, Set<String> withoutFields) implements BlockLoaderFunctionConfig {
        @Override
        public Function function() {
            return Function.TIME_SERIES_METADATA;
        }
    }

    enum Function {
        AMD_COUNT,
        AMD_DEFAULT,
        AMD_MAX,
        AMD_MIN,
        AMD_SUM,
        BYTE_LENGTH,
        MV_MAX,
        MV_MIN,
        LENGTH,
        V_COSINE,
        V_DOT_PRODUCT,
        V_HAMMING,
        V_L1NORM,
        V_L2NORM,
        TIME_SERIES_METADATA
    }
}
