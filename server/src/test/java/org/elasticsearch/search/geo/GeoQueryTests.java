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

package org.elasticsearch.search.geo;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Rectangle;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.geo.RandomShapeGenerator.createGeometryCollectionWithin;
import static org.elasticsearch.test.geo.RandomShapeGenerator.xRandomPoint;
import static org.elasticsearch.test.geo.RandomShapeGenerator.xRandomRectangle;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public abstract class GeoQueryTests extends ESSingleNodeTestCase {
    protected static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.QUADTREE
    };

    protected XContentBuilder createMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape");
        if (randomBoolean()) {
            xcb = xcb.field("tree", randomFrom(PREFIX_TREES))
            .field("strategy", randomFrom(SpatialStrategy.RECURSIVE, SpatialStrategy.TERM));
        }
        xcb = xcb.endObject().endObject().endObject().endObject();

        return xcb;
    }

    public void testNullShape() throws Exception {
        String mapping = Strings.toString(createMapping());
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
        ensureGreen();

        client().prepareIndex("test").setId("aNullshape").setSource("{\"location\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet("test", "aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    }
}
