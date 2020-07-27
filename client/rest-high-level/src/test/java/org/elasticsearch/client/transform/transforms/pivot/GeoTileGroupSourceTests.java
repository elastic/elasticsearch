/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class GeoTileGroupSourceTests extends AbstractXContentTestCase<GeoTileGroupSource> {

    public static GeoTileGroupSource randomGeoTileGroupSource() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoTileGroupSource(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomIntBetween(1, GeoTileUtils.MAX_ZOOM),
            randomBoolean() ? null : new GeoBoundingBox(
                new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
            )
        );
    }

    @Override
    protected GeoTileGroupSource createTestInstance() {
        return randomGeoTileGroupSource();
    }

    @Override
    protected GeoTileGroupSource doParseInstance(XContentParser parser) throws IOException {
        return GeoTileGroupSource.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> !field.isEmpty();
    }
}
