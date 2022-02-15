/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class HistogramTests extends BaseAggregationTestCase<HistogramAggregationBuilder> {

    @Override
    protected HistogramAggregationBuilder createTestAggregatorBuilder() {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        factory.field(INT_FIELD_NAME);
        factory.interval(randomDouble() * 1000);
        if (randomBoolean()) {
            double minBound = randomDouble();
            double maxBound = randomDoubleBetween(minBound, 1, true);
            factory.extendedBounds(minBound, maxBound);
        }
        if (randomBoolean()) {
            factory.format("###.##");
        }
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        if (randomBoolean()) {
            factory.minDocCount(randomIntBetween(0, 100));
        }
        if (randomBoolean()) {
            factory.missing(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            factory.offset(randomIntBetween(0, 100000));
        }
        if (randomBoolean()) {
            List<BucketOrder> order = randomOrder();
            if (order.size() == 1 && randomBoolean()) {
                factory.order(order.get(0));
            } else {
                factory.order(order);
            }
        }
        return factory;
    }

    public void testInvalidBounds() {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder("foo");
        factory.field(INT_FIELD_NAME);
        factory.interval(randomDouble() * 1000);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.NaN, 1.0); });
        assertThat(ex.getMessage(), startsWith("min bound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.POSITIVE_INFINITY, 1.0); });
        assertThat(ex.getMessage(), startsWith("min bound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.NEGATIVE_INFINITY, 1.0); });
        assertThat(ex.getMessage(), startsWith("min bound must be finite, got: "));

        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.NaN); });
        assertThat(ex.getMessage(), startsWith("max bound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.POSITIVE_INFINITY); });
        assertThat(ex.getMessage(), startsWith("max bound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.NEGATIVE_INFINITY); });
        assertThat(ex.getMessage(), startsWith("max bound must be finite, got: "));

        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.5, 0.4); });
        assertThat(ex.getMessage(), equalTo("max bound [0.4] must be greater than min bound [0.5]"));
    }

    private List<BucketOrder> randomOrder() {
        List<BucketOrder> orders = new ArrayList<>();
        switch (randomInt(4)) {
            case 0 -> orders.add(BucketOrder.key(randomBoolean()));
            case 1 -> orders.add(BucketOrder.count(randomBoolean()));
            case 2 -> orders.add(BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomBoolean()));
            case 3 -> orders.add(
                BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomAlphaOfLengthBetween(3, 20), randomBoolean())
            );
            case 4 -> {
                int numOrders = randomIntBetween(1, 3);
                for (int i = 0; i < numOrders; i++) {
                    orders.addAll(randomOrder());
                }
            }
            default -> fail();
        }
        return orders;
    }

}
