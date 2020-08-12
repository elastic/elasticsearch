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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;

import java.util.ArrayList;
import java.util.List;

public class DateHistogramTests extends BaseAggregationTestCase<DateHistogramAggregationBuilder> {

    @Override
    protected DateHistogramAggregationBuilder createTestAggregatorBuilder() {
        DateHistogramAggregationBuilder factory = new DateHistogramAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        factory.field(INT_FIELD_NAME);
        if (randomBoolean()) {
            factory.fixedInterval(new DateHistogramInterval(randomIntBetween(1, 100000) + "ms"));
        } else {
            if (randomBoolean()) {
                factory.calendarInterval(randomFrom(DateHistogramInterval.YEAR, DateHistogramInterval.QUARTER,
                        DateHistogramInterval.MONTH, DateHistogramInterval.WEEK, DateHistogramInterval.DAY, DateHistogramInterval.HOUR,
                        DateHistogramInterval.MINUTE, DateHistogramInterval.SECOND));
            } else {
                int branch = randomInt(3);
                switch (branch) {
                case 0:
                    factory.fixedInterval(DateHistogramInterval.seconds(randomIntBetween(1, 1000)));
                    break;
                case 1:
                    factory.fixedInterval(DateHistogramInterval.minutes(randomIntBetween(1, 1000)));
                    break;
                case 2:
                    factory.fixedInterval(DateHistogramInterval.hours(randomIntBetween(1, 1000)));
                    break;
                case 3:
                    factory.fixedInterval(DateHistogramInterval.days(randomIntBetween(1, 1000)));
                    break;
                default:
                    throw new IllegalStateException("invalid branch: " + branch);
                }
            }
        }
        if (randomBoolean()) {
            factory.extendedBounds(LongBoundsTests.randomExtendedBounds());
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
