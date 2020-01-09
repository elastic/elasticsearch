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

import static org.apache.lucene.util.LuceneTestCase.expectThrows;
import static org.apache.lucene.util.LuceneTestCase.random;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class GeoShapeQueryTests extends GeoQueryTests {
    private static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.QUADTREE
    };

    @Override
    protected XContentBuilder createMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape")
            .endObject()
            .endObject()
            .endObject();

        return xcb;
    }

    protected XContentBuilder createMapping(String tree) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape")
            .field("tree", tree)
            .endObject()
            .endObject()
            .endObject();

        return xcb;
    }

    protected XContentBuilder createRandomMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape");
        if (randomBoolean()) {
            xcb = xcb.field("tree", randomFrom(PREFIX_TREES))
                .field("strategy", randomFrom(SpatialStrategy.RECURSIVE, SpatialStrategy.TERM));
        }
        xcb = xcb.endObject().endObject().endObject();

        return xcb;
    }

    protected XContentBuilder createGeohashMapping() throws Exception {
        return createMapping(LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.GEOHASH);
    }

    protected XContentBuilder createQuadtreeMapping() throws Exception {
        return createMapping(LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.QUADTREE);
    }

    public void testNullShape() throws Exception {
        super.testNullShape();
    }

    public void testIndexPointsFilterRectangle() throws Exception {
        super.testIndexPointsFilterRectangle(Strings.toString(createRandomMapping()));
    }

    public void testIndexedShapeReference() throws Exception {
        super.testIndexedShapeReference(Strings.toString(createRandomMapping()));
    }

    public void testShapeFetchingPath() throws Exception {
        super.testShapeFetchingPath();
    }

    public void testQueryRandomGeoCollection() throws Exception {
        super.testQueryRandomGeoCollection(
            Strings.toString(randomBoolean() ? createMapping() : createQuadtreeMapping()));
    }

    public void testRandomGeoCollectionQuery() throws Exception {
        super.testRandomGeoCollectionQuery();
    }

    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        super.testShapeFilterWithDefinedGeoCollection(Strings.toString(createQuadtreeMapping()));
    }

    public void testFieldAlias() throws IOException {
        super.testFieldAlias(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .endObject()
            .startObject("alias")
            .field("type", "alias")
            .field("path", "location")
            .endObject()
            .endObject()
            .endObject()
            .endObject());
    }

    // Test for issue #34418
    public void testEnvelopeSpanningDateline() throws Exception {
        super.testEnvelopeSpanningDateline();
    }

    public void testGeometryCollectionRelations() throws Exception {
        super.testGeometryCollectionRelations();
    }

    public void testEdgeCases() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape")
            .endObject().endObject().endObject();
        String mapping = Strings.toString(xcb);
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex("test").setId("blakely").setSource(jsonBuilder().startObject()
                .field("name", "Blakely Island")
                .startObject("geo")
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(-122.83).value(48.57).endArray()
                .startArray().value(-122.77).value(48.56).endArray()
                .startArray().value(-122.79).value(48.53).endArray()
                .startArray().value(-122.83).value(48.57).endArray() // close the polygon
                .endArray().endArray()
                .endObject()
                .endObject()).setRefreshPolicy(IMMEDIATE).get();

        EnvelopeBuilder query = new EnvelopeBuilder(new Coordinate(-122.88, 48.62), new Coordinate(-122.82, 48.54));

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoIntersectionQuery("geo", query))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("blakely"));
    }

    public void testIndexedShapeReferenceSourceDisabled() throws Exception {
        String mapping = Strings.toString(createRandomMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        createIndex("shapes", Settings.EMPTY, "shape_type", "_source", "enabled=false");
        ensureGreen();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes").setId("Big_Rectangle").setSource(jsonBuilder().startObject()
            .field("shape", shape).endObject()).setRefreshPolicy(IMMEDIATE).get();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("geo", "Big_Rectangle")).get());
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testReusableBuilder() throws IOException {
        PolygonBuilder polygon = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close())
                .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(175, -5).coordinate(185, -5).coordinate(185, 5)
                        .coordinate(175, 5).close()));
        assertUnmodified(polygon);

        LineStringBuilder linestring = new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close());
        assertUnmodified(linestring);
    }

    private void assertUnmodified(ShapeBuilder builder) throws IOException {
        String before = Strings.toString(jsonBuilder().startObject().field("area", builder).endObject());
        builder.buildS4J();
        String after = Strings.toString(jsonBuilder().startObject().field("area", builder).endObject());
        assertThat(before, equalTo(after));
    }

    /** tests querying a random geometry collection with a point */
    public void testPointQuery() throws Exception {
        // Create a random geometry collection to index.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        double[] pt = new double[] {GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude()};
        PointBuilder pb = new PointBuilder(pt[0], pt[1]);
        gcb.shape(pb);

        // don't use random as permits quadtree
        String mapping = Strings.toString(randomBoolean() ? createMapping() : createQuadtreeMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", pb);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testContainsShapeQuery() throws Exception {
        // Create a random geometry collection.
        Rectangle mbr = xRandomRectangle(random(), xRandomPoint(random()), true);
        boolean usePrefixTrees = randomBoolean();
        GeometryCollectionBuilder gcb;
        if (usePrefixTrees) {
            gcb = createGeometryCollectionWithin(random(), mbr);
        } else {
            // vector strategy does not yet support multipoint queries
            gcb = new GeometryCollectionBuilder();
            int numShapes = RandomNumbers.randomIntBetween(random(), 1, 4);
            for (int i = 0; i < numShapes; ++i) {
                ShapeBuilder shape;
                do {
                    shape = RandomShapeGenerator.createShapeWithin(random(), mbr);
                } while (shape instanceof MultiPointBuilder);
                gcb.shape(shape);
            }
        }

        // don't use random mapping as permits quadtree
        String mapping = Strings.toString(usePrefixTrees ? createQuadtreeMapping() : createMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // index the mbr of the collection
        EnvelopeBuilder env = new EnvelopeBuilder(new Coordinate(mbr.getMinX(), mbr.getMaxY()),
                new Coordinate(mbr.getMaxX(), mbr.getMinY()));
        docSource = env.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("2").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(randomIntBetween(0, gcb.numShapes() - 1)));
        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("geo", filterShape)
                .relation(ShapeRelation.CONTAINS);
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits().value, greaterThan(0L));
    }

    public void testExistsQuery() throws Exception {
        // Create a random geometry collection.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        logger.info("Created Random GeometryCollection containing {} shapes", gcb.numShapes());

        String mapping = Strings.toString(randomBoolean() ? createMapping() : createQuadtreeMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ExistsQueryBuilder eqb = QueryBuilders.existsQuery("geo");
        SearchResponse result = client().prepareSearch("test").setQuery(eqb).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testPointsOnly() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties").startObject("geo")
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .field("tree_levels", "6")
                .field("distance_error_pct", "0.01")
                .field("points_only", true)
                .endObject()
                .endObject().endObject());

        client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).get();
        ensureGreen();

        ShapeBuilder shape = RandomShapeGenerator.createShape(random());
        try {
            client().prepareIndex("geo_points_only").setId("1")
                    .setSource(jsonBuilder().startObject().field("geo", shape).endObject())
                    .setRefreshPolicy(IMMEDIATE).get();
        } catch (MapperParsingException e) {
            // RandomShapeGenerator created something other than a POINT type, verify the correct exception is thrown
            assertThat(e.getCause().getMessage(), containsString("is configured for points only"));
            return;
        }

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only")
                .setQuery(geoIntersectionQuery("geo", shape))
                .get();

        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testPointsOnlyExplicit() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .field("tree_levels", "6")
            .field("distance_error_pct", "0.01")
            .field("points_only", true)
            .endObject()
            .endObject().endObject());

        client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).get();
        ensureGreen();

        // MULTIPOINT
        ShapeBuilder shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.MULTIPOINT);
        client().prepareIndex("geo_points_only").setId("1")
            .setSource(jsonBuilder().startObject().field("geo", shape).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        // POINT
        shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.POINT);
        client().prepareIndex("geo_points_only").setId("2")
            .setSource(jsonBuilder().startObject().field("geo", shape).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only")
            .setQuery(matchAllQuery())
            .get();

        assertEquals(2, response.getHits().getTotalHits().value);
    }

}
