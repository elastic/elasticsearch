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

public class HistogramTests extends BaseAggregationTestCase<HistogramAggregationBuilder> {

    @Override
    protected HistogramAggregationBuilder createTestAggregatorBuilder() {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder("foo");
        factory.field(INT_FIELD_NAME);
        factory.interval(randomDouble() * 1000);
        if (randomBoolean()) {
            factory.extendedBounds(randomDouble(), randomDouble());
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

}
