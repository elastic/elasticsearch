/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalBucketMetricValueTests extends InternalAggregationTestCase<InternalBucketMetricValue> {

    @Override
    protected InternalBucketMetricValue createTestInstance(String name, Map<String, Object> metadata) {
        double value = frequently() ? randomDoubleBetween(-10000, 100000, true)
                : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN });
        String[] keys = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = randomAlphaOfLength(10);
        }
        return new InternalBucketMetricValue(name, keys, value, randomNumericDocValueFormat(), metadata);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalBucketMetricValue reduced, List<InternalBucketMetricValue> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected void assertFromXContent(InternalBucketMetricValue bucketMetricValue, ParsedAggregation parsedAggregation) {
        BucketMetricValue parsed = ((BucketMetricValue) parsedAggregation);
        assertArrayEquals(bucketMetricValue.keys(), parsed.keys());
        if (Double.isInfinite(bucketMetricValue.value()) == false) {
            assertEquals(bucketMetricValue.value(), parsed.value(), 0);
            assertEquals(bucketMetricValue.getValueAsString(), parsed.getValueAsString());
        } else {
            // we write Double.NEGATIVE_INFINITY and Double.POSITIVE_INFINITY to xContent as 'null', so we
            // cannot differentiate between them. Also we cannot recreate the exact String representation
            assertEquals(parsed.value(), Double.NEGATIVE_INFINITY, 0);
        }
    }

    @Override
    protected InternalBucketMetricValue mutateInstance(InternalBucketMetricValue instance) {
        String name = instance.getName();
        String[] keys = instance.keys();
        double value = instance.value();
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            if (Double.isFinite(value)) {
                value += between(1, 100);
            } else {
                value = randomDoubleBetween(0, 100000, true);
            }
            break;
        case 2:
            keys = Arrays.copyOf(keys, keys.length + 1);
            keys[keys.length - 1] = randomAlphaOfLengthBetween(1, 20);
            break;
        case 3:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalBucketMetricValue(name, keys, value, formatter, metadata);
    }
}
