/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;

import java.util.List;
import java.util.Map;

public class InternalNestedTests extends InternalSingleBucketAggregationTestCase<InternalNested> {
    @Override
    protected InternalNested createTestInstance(
        String name,
        long docCount,
        InternalAggregations aggregations,
        Map<String, Object> metadata
    ) {
        return new InternalNested(name, docCount, aggregations, metadata);
    }

    @Override
    protected void extraAssertReduced(InternalNested reduced, List<InternalNested> inputs) {
        // Nothing extra to assert
    }
}
