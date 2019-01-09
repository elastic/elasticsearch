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
import org.elasticsearch.search.aggregations.bucket.geogrid2.GeoGridAggregationBuilder2;
import org.elasticsearch.search.aggregations.bucket.geogrid2.GeoGridType;
import org.elasticsearch.search.aggregations.bucket.geogrid2.GeoHashType;

public class GeoGridTests extends BaseAggregationTestCase<GeoGridAggregationBuilder2> {

    public static final GeoGridType GEOHASH_TYPE = new GeoHashType();

    /**
     * Pick a random hash type
     */
    public static GeoGridType randomType() {
        // With more types, will use randomIntBetween() to pick one
        return GEOHASH_TYPE;
    }

    /**
     * Pick a random precision for the given hash type.
     */
    public static int randomPrecision(final GeoGridType type) {
        if (type.getClass() == GeoHashType.class) {
            return randomIntBetween(1, 12);
        }
        throw new IllegalArgumentException(type.getClass() + " was not added to the test");
    }

    public static int maxPrecision(GeoGridType type) {
        if (type.getClass() == GeoHashType.class) {
            return 12;
        }
        throw new IllegalArgumentException(type.getClass() + " was not added to the test");
    }

    @Override
    protected GeoGridAggregationBuilder2 createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        GeoGridType type = randomType();
        GeoGridAggregationBuilder2 factory = new GeoGridAggregationBuilder2(name, type);
        if (randomBoolean()) {
            factory.precision(randomPrecision(factory.type()));
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
