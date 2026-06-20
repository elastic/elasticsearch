/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

public class VariableWidthHistogramTests extends BaseAggregationTestCase<VariableWidthHistogramAggregationBuilder> {

    @Override
    protected VariableWidthHistogramAggregationBuilder createTestAggregatorBuilder() {
        VariableWidthHistogramAggregationBuilder factory = new VariableWidthHistogramAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        factory.field(INT_FIELD_NAME);
        if (randomBoolean()) {
            factory.setNumBuckets(randomIntBetween(1, 1000));
        }
        if (randomBoolean()) {
            // shard_size must be greater than 1
            factory.setShardSize(randomIntBetween(2, 10000));
        }
        if (randomBoolean()) {
            // initial_buffer must be greater than 0
            factory.setInitialBuffer(randomIntBetween(1, 50000));
        }
        return factory;
    }

    /**
     * Before {@code variable_width_histogram_shard_size}, {@code shard_size} and {@code initial_buffer} were not put on
     * the wire. A node serializing to an older node must therefore drop them, and the receiving side falls back to the
     * unset ({@code -1}) sentinel, i.e. the computed defaults. {@code buckets} must still round-trip.
     */
    public void testSerializationBeforeShardSizeSupport() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(
            TransportVersion.fromName("variable_width_histogram_shard_size")
        );
        VariableWidthHistogramAggregationBuilder original = new VariableWidthHistogramAggregationBuilder("test");
        original.field(INT_FIELD_NAME);
        original.setNumBuckets(7);
        original.setShardSize(123);
        original.setInitialBuffer(456);

        VariableWidthHistogramAggregationBuilder deserialized = (VariableWidthHistogramAggregationBuilder) copyNamedWriteable(
            original,
            namedWriteableRegistry(),
            AggregationBuilder.class,
            before
        );

        // buckets survives, but shard_size/initial_buffer are dropped and resolve to the defaults derived from buckets
        assertEquals(7 * 50, deserialized.getShardSize());
        assertEquals(Math.min(10 * (7 * 50), 50000), deserialized.getInitialBuffer());
    }
}
