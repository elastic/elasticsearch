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
package org.elasticsearch.search.aggregations.metrics.geoheatmap;

import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilderTests;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.geoheatmap.GeoHeatmapAggregationBuilder;

/**
 * Randomized test for construction and serialization
 */
public class BaseGeoHeatmapTests extends BaseAggregationTestCase<GeoHeatmapAggregationBuilder> {

    @Override
    protected GeoHeatmapAggregationBuilder createTestAggregatorBuilder() {

        String name = randomAsciiOfLengthBetween(3, 20);
        GeoHeatmapAggregationBuilder factory = new GeoHeatmapAggregationBuilder(name);
        if (randomBoolean()) {
            factory.gridLevel(randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            factory.maxCells(randomIntBetween(2, 5000) * 2);
        }

        InternalBuilderTests it = new InternalBuilderTests();
        factory.geom(it.getRandomShapeQuery());

        String field = randomNumericField();
        factory.field(field);

        return factory;
    }

    class InternalBuilderTests extends GeoShapeQueryBuilderTests {
        public GeoShapeQueryBuilder getRandomShapeQuery() {
            return doCreateTestQueryBuilder();
        }
    }
}
