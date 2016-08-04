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
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBoundsTests;

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
