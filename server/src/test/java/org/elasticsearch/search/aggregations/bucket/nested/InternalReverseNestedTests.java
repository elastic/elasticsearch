/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalReverseNestedTests extends InternalSingleBucketAggregationTestCase<InternalReverseNested> {
    @Override
    protected InternalReverseNested createTestInstance(String name, long docCount, InternalAggregations aggregations,
            Map<String, Object> metadata) {
        return new InternalReverseNested(name, docCount, aggregations, metadata);
    }

    @Override
    protected void extraAssertReduced(InternalReverseNested reduced, List<InternalReverseNested> inputs) {
        // Nothing extra to assert
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedReverseNested.class;
    }

    @Override
    protected void assertFromXContent(InternalReverseNested aggregation, ParsedAggregation parsedAggregation) throws IOException {
        super.assertFromXContent(aggregation, parsedAggregation);
        assertTrue(parsedAggregation instanceof ReverseNested);
    }
}
