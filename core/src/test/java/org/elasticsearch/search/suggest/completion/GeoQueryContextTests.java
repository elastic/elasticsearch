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

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class GeoQueryContextTests extends QueryContextTestCase<GeoQueryContext> {

    public static GeoQueryContext randomGeoQueryContext() {
        final GeoQueryContext.Builder builder = GeoQueryContext.builder();
        builder.setGeoPoint(new GeoPoint(randomDouble(), randomDouble()));
        maybeSet(builder::setBoost, randomIntBetween(1, 10));
        maybeSet(builder::setPrecision, randomIntBetween(1, 12));
        List<Integer> neighbours = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 12); i++) {
            neighbours.add(randomIntBetween(1, 12));
        }
        maybeSet(builder::setNeighbours, neighbours);
        return builder.build();
    }

    @Override
    protected GeoQueryContext createTestModel() {
        return randomGeoQueryContext();
    }

    @Override
    protected GeoQueryContext fromXContent(XContentParser parser) throws IOException {
        return GeoQueryContext.fromXContent(parser);
    }

    public void testNullGeoPointIsIllegal() {
        final GeoQueryContext geoQueryContext = randomGeoQueryContext();
        final GeoQueryContext.Builder builder = GeoQueryContext.builder()
            .setNeighbours(geoQueryContext.getNeighbours())
            .setPrecision(geoQueryContext.getPrecision())
            .setBoost(geoQueryContext.getBoost());
        try {
            builder.build();
            fail("null geo point is illegal");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), equalTo("geoPoint must not be null"));
        }
    }

    public void testIllegalArguments() {
        final GeoQueryContext.Builder builder = GeoQueryContext.builder();

        try {
            builder.setGeoPoint(null);
            fail("geoPoint must not be null");
        } catch (NullPointerException e) {
            assertEquals(e.getMessage(), "geoPoint must not be null");
        }
        try {
            builder.setBoost(-randomIntBetween(1, Integer.MAX_VALUE));
            fail("boost must be positive");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "boost must be greater than 0");
        }
        int precision = 0;
        try {
            do {
                precision = randomInt();
            } while (precision >= 1 && precision <= 12);
            builder.setPrecision(precision);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "precision must be between 1 and 12");
        }
        try {
            List<Integer> neighbours = new ArrayList<>();
            neighbours.add(precision);
            for (int i = 1; i < randomIntBetween(1, 11); i++) {
                neighbours.add(i);
            }
            Collections.shuffle(neighbours, random());
            builder.setNeighbours(neighbours);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "neighbour value must be between 1 and 12");
        }
    }
}
