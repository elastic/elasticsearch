/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.missing;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;

import java.util.List;
import java.util.Map;

public class InternalMissingTests extends InternalSingleBucketAggregationTestCase<InternalMissing> {
    @Override
    protected InternalMissing createTestInstance(
        String name,
        long docCount,
        InternalAggregations aggregations,
        Map<String, Object> metadata
    ) {
        return new InternalMissing(name, docCount, aggregations, metadata);
    }

    @Override
    protected void extraAssertReduced(InternalMissing reduced, List<InternalMissing> inputs) {
        // Nothing extra to assert
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedMissing.class;
    }
}
