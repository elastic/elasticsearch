/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.function.Function;

/**
 * Utility functions for timeseries mapper parameters
 */
public final class TimeseriesParams {

    public static FieldMapper.Parameter<Boolean> dimension(boolean defaultValue, Function<FieldMapper, Boolean> initializer) {
        return FieldMapper.Parameter.boolParam("dimension", true, initializer, defaultValue)
            .setMergeValidator((o, n, c) -> o == n || (o == false && n));  // dimensions can be updated from 'false' to 'true', not vv
    }
}
