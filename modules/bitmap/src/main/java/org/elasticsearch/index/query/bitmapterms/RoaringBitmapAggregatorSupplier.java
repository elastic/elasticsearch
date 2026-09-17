/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

/** Creates a collector for a supported numeric values source. */
@FunctionalInterface
interface RoaringBitmapAggregatorSupplier {
    Aggregator build(
        String name,
        ValuesSource.Numeric valuesSource,
        ValuesSourceConfig config,
        InternalRoaringBitmap.BitmapFormat width,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException;
}
