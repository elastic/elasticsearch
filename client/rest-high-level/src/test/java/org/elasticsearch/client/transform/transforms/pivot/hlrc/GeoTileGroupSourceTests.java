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

package org.elasticsearch.client.transform.transforms.pivot.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GeoTileGroupSource;

import static org.hamcrest.Matchers.equalTo;

public class GeoTileGroupSourceTests extends AbstractResponseTestCase<
    GeoTileGroupSource,
    org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource> {

    public static GeoTileGroupSource randomGeoTileGroupSource() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoTileGroupSource(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, GeoTileUtils.MAX_ZOOM),
            randomBoolean()
                ? null
                : new GeoBoundingBox(
                    new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                    new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
                )
        );
    }

    @Override
    protected GeoTileGroupSource createServerTestInstance(XContentType xContentType) {
        return randomGeoTileGroupSource();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource doParseToClientInstance(XContentParser parser) {
        return org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        GeoTileGroupSource serverTestInstance,
        org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource clientInstance
    ) {
        assertThat(serverTestInstance.getField(), equalTo(clientInstance.getField()));
        assertNull(clientInstance.getScript());
        assertThat(serverTestInstance.getPrecision(), equalTo(clientInstance.getPrecision()));
        assertThat(serverTestInstance.getGeoBoundingBox(), equalTo(clientInstance.getGeoBoundingBox()));
    }

}
