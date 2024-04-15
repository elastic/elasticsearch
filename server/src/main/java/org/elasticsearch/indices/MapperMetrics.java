/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.util.ValueWithThreadLocalOverride;
import org.elasticsearch.index.mapper.SourceFieldMetrics;

/**
 * Groups together all metrics used in mappers.
 * Main purpose of this class is to avoid verbosity of passing individual metric instances around.
 */
public class MapperMetrics {
    public static MapperMetrics NOOP = new MapperMetrics();

    public static ValueWithThreadLocalOverride<SourceFieldMetrics> SOURCE_FIELD_METRICS = new ValueWithThreadLocalOverride<>(
        SourceFieldMetrics.NOOP
    );

    public static void init(SourceFieldMetrics sourceFieldMetrics) {
        SOURCE_FIELD_METRICS = new ValueWithThreadLocalOverride<>(sourceFieldMetrics);
    }

    private MapperMetrics() {}
}
