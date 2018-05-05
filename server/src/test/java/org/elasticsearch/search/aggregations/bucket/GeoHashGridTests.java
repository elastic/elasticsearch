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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashType;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoPlusCodeHandler;

public class GeoHashGridTests extends BaseAggregationTestCase<GeoGridAggregationBuilder> {

    /**
     * Pick a random hash type
     */
    public static GeoHashType randomType() {
        return randomFrom(GeoHashType.values());
    }

    /**
     * Pick a random precision for the given hash type.
     */
    public static int randomPrecision(final GeoHashType type) {
        int precision;
        switch (type) {
            case GEOHASH:
                precision = randomIntBetween(1, 12);
                break;
            case PLUSCODE:
                precision = RandomPicks.randomFrom(random(), GeoPlusCodeHandler.ALLOWED_LENGTHS);
                break;
            default:
                throw new IllegalArgumentException("GeoHashType." + type.name() + " was not added to the test");
        }
        return precision;
    }

    public static int maxPrecision(GeoHashType type) {
        switch (type) {
            case GEOHASH:
                return 12;
            case PLUSCODE:
                return GeoPlusCodeHandler.MAX_LENGTH;
            default:
                throw new IllegalArgumentException("GeoHashType." + type.name() + " was not added to the test");
        }
    }

    @Override
    protected GeoGridAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        GeoHashType type = randomBoolean() ? randomType() : null;
        GeoGridAggregationBuilder factory = new GeoGridAggregationBuilder(name, type);
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
