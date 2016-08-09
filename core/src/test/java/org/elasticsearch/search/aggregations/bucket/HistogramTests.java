/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class HistogramTests extends BaseAggregationTestCase<HistogramAggregationBuilder> {

    @Override
    protected HistogramAggregationBuilder createTestAggregatorBuilder() {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder("foo");
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
            int branch = randomInt(5);
            switch (branch) {
            case 0:
                factory.order(Order.COUNT_ASC);
                break;
            case 1:
                factory.order(Order.COUNT_DESC);
                break;
            case 2:
                factory.order(Order.KEY_ASC);
                break;
            case 3:
                factory.order(Order.KEY_DESC);
                break;
            case 4:
                factory.order(Order.aggregation("foo", true));
                break;
            case 5:
                factory.order(Order.aggregation("foo", false));
                break;
            }
        }
        return factory;
    }

    public void testInvalidBounds() {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder("foo");
        factory.field(INT_FIELD_NAME);
        factory.interval(randomDouble() * 1000);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.NaN, 1.0); });
        assertThat(ex.getMessage(), startsWith("minBound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.POSITIVE_INFINITY, 1.0); });
        assertThat(ex.getMessage(), startsWith("minBound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.NEGATIVE_INFINITY, 1.0); });
        assertThat(ex.getMessage(), startsWith("minBound must be finite, got: "));

        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.NaN); });
        assertThat(ex.getMessage(), startsWith("maxBound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.POSITIVE_INFINITY); });
        assertThat(ex.getMessage(), startsWith("maxBound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.NEGATIVE_INFINITY); });
        assertThat(ex.getMessage(), startsWith("maxBound must be finite, got: "));

        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.5, 0.4); });
        assertThat(ex.getMessage(), equalTo("maxBound [0.4] must be greater than minBound [0.5]"));
    }

}
