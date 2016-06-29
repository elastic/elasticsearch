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
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;

public class GeoHashGridTests extends BaseAggregationTestCase<GeoGridAggregationBuilder> {

    @Override
    protected GeoGridAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAsciiOfLengthBetween(3, 20);
        GeoGridAggregationBuilder factory = new GeoGridAggregationBuilder(name);
        if (randomBoolean()) {
            int precision = randomIntBetween(1, 12);
            factory.precision(precision);
        }
        if (randomBoolean()) {
            factory.size(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        return factory;
    }

}
