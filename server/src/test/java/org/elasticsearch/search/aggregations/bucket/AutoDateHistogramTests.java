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
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;

public class AutoDateHistogramTests extends BaseAggregationTestCase<AutoDateHistogramAggregationBuilder> {

    @Override
    protected AutoDateHistogramAggregationBuilder createTestAggregatorBuilder() {
        AutoDateHistogramAggregationBuilder builder = new AutoDateHistogramAggregationBuilder(randomAlphaOfLengthBetween(1, 10));
        builder.field(INT_FIELD_NAME);
        builder.setNumBuckets(randomIntBetween(1, 100000));
        //TODO[PCS]: add builder pattern here
        if (randomBoolean()) {
            builder.format("###.##");
        }
        if (randomBoolean()) {
            builder.missing(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            builder.timeZone(randomZone());
        }
        return builder;
    }

}
