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
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBoundsTests;
import org.elasticsearch.search.aggregations.BucketOrder;

import java.util.ArrayList;
import java.util.List;

public class DateHistogramTests extends BaseAggregationTestCase<DateHistogramAggregationBuilder> {

    @Override
    protected DateHistogramAggregationBuilder createTestAggregatorBuilder() {
        DateHistogramAggregationBuilder factory = new DateHistogramAggregationBuilder("foo");
        factory.field(INT_FIELD_NAME);
        if (randomBoolean()) {
            factory.interval(randomIntBetween(1, 100000));
        } else {
            if (randomBoolean()) {
                factory.dateHistogramInterval(randomFrom(DateHistogramInterval.YEAR, DateHistogramInterval.QUARTER,
                        DateHistogramInterval.MONTH, DateHistogramInterval.WEEK, DateHistogramInterval.DAY, DateHistogramInterval.HOUR,
                        DateHistogramInterval.MINUTE, DateHistogramInterval.SECOND));
            } else {
                int branch = randomInt(4);
                switch (branch) {
                case 0:
                    factory.dateHistogramInterval(DateHistogramInterval.seconds(randomIntBetween(1, 1000)));
                    break;
                case 1:
                    factory.dateHistogramInterval(DateHistogramInterval.minutes(randomIntBetween(1, 1000)));
                    break;
                case 2:
                    factory.dateHistogramInterval(DateHistogramInterval.hours(randomIntBetween(1, 1000)));
                    break;
                case 3:
                    factory.dateHistogramInterval(DateHistogramInterval.days(randomIntBetween(1, 1000)));
                    break;
                case 4:
                    factory.dateHistogramInterval(DateHistogramInterval.weeks(randomIntBetween(1, 1000)));
                    break;
                default:
                    throw new IllegalStateException("invalid branch: " + branch);
                }
            }
        }
        if (randomBoolean()) {
            factory.extendedBounds(ExtendedBoundsTests.randomExtendedBounds());
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
            if(order.size() == 1 && randomBoolean()) {
                factory.order(order.get(0));
            } else {
                factory.order(order);
            }
        }
        return factory;
    }

    private List<BucketOrder> randomOrder() {
        List<BucketOrder> orders = new ArrayList<>();
        switch (randomInt(4)) {
            case 0:
                orders.add(BucketOrder.key(randomBoolean()));
                break;
            case 1:
                orders.add(BucketOrder.count(randomBoolean()));
                break;
            case 2:
                orders.add(BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomBoolean()));
                break;
            case 3:
                orders.add(BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomAlphaOfLengthBetween(3, 20), randomBoolean()));
                break;
            case 4:
                int numOrders = randomIntBetween(1, 3);
                for (int i = 0; i < numOrders; i++) {
                    orders.addAll(randomOrder());
                }
                break;
            default:
                fail();
        }
        return orders;
    }

}
