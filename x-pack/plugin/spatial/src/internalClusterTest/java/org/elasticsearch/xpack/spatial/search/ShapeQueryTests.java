/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.MultiPolygonBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.locationtech.jts.geom.Coordinate;


import java.util.Collection;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class ShapeQueryTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateSpatialPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    protected abstract XContentBuilder createDefaultMapping() throws Exception;

    static String defaultFieldName = "xy";
    static String defaultIndexName = "test-points";

    public void testNullShape() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("aNullshape")
            .setSource("{\"geo\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet(defaultIndexName, "aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    };

    public void testIndexPointsFilterRectangle() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName).setId("1").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 1")
            .field(defaultFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName).setId("2").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 2")
            .field(defaultFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));
        GeometryCollectionBuilder builder = new GeometryCollectionBuilder().shape(shape);
        Geometry geometry = builder.buildGeometry().get(0);
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(new ShapeQueryBuilder(defaultFieldName, geometry)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        // default query, without specifying relation (expect intersects)
        searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(new ShapeQueryBuilder(defaultFieldName, geometry))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIndexPointsCircle() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName).setId("1").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 1")
            .field(defaultFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName).setId("2").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 2")
            .field(defaultFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        CircleBuilder shape = new CircleBuilder().center(new Coordinate(-30, -30)).radius("1");
        GeometryCollectionBuilder builder = new GeometryCollectionBuilder().shape(shape);
        Geometry geometry = builder.buildGeometry().get(0);

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(new ShapeQueryBuilder(defaultFieldName, geometry)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIndexPointsPolygon() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName).setId("1").setSource(jsonBuilder()
            .startObject()
            .field(defaultFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName).setId("2").setSource(jsonBuilder()
            .startObject()
            .field(defaultFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        CoordinatesBuilder cb = new CoordinatesBuilder();
        cb.coordinate(new Coordinate(-35, -35))
            .coordinate(new Coordinate(-35, -25))
            .coordinate(new Coordinate(-25, -25))
            .coordinate(new Coordinate(-25, -35))
            .coordinate(new Coordinate(-35, -35));
        PolygonBuilder shape = new PolygonBuilder(cb);
        GeometryCollectionBuilder builder = new GeometryCollectionBuilder().shape(shape);
        Geometry geometry = builder.buildGeometry();
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(new ShapeQueryBuilder(defaultFieldName, geometry)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        SearchHits searchHits = searchResponse.getHits();
        assertThat(searchHits.getTotalHits().value, equalTo(1L));
        assertThat(searchHits.getAt(0).getId(), equalTo("1"));
    }

    public void testIndexPointsMultiPolygon() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName).setId("1").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 1")
            .field(defaultFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName).setId("2").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 2")
            .field(defaultFieldName, "POINT(-40 -40)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName).setId("3").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 3")
            .field(defaultFieldName, "POINT(-50 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        CoordinatesBuilder encloseDocument1Cb = new CoordinatesBuilder();
        encloseDocument1Cb.coordinate(new Coordinate(-35, -35))
            .coordinate(new Coordinate(-35, -25))
            .coordinate(new Coordinate(-25, -25))
            .coordinate(new Coordinate(-25, -35))
            .coordinate(new Coordinate(-35, -35));
        PolygonBuilder encloseDocument1Shape = new PolygonBuilder(encloseDocument1Cb);

        CoordinatesBuilder encloseDocument2Cb = new CoordinatesBuilder();
        encloseDocument2Cb.coordinate(new Coordinate(-55, -55))
            .coordinate(new Coordinate(-55, -45))
            .coordinate(new Coordinate(-45, -45))
            .coordinate(new Coordinate(-45, -55))
            .coordinate(new Coordinate(-55, -55));
        PolygonBuilder encloseDocument2Shape = new PolygonBuilder(encloseDocument2Cb);

        MultiPolygonBuilder mp = new MultiPolygonBuilder();
        mp.polygon(encloseDocument1Shape).polygon(encloseDocument2Shape);

        GeometryCollectionBuilder builder = new GeometryCollectionBuilder().shape(mp);
        Geometry geometry = builder.buildGeometry();
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(new ShapeQueryBuilder(defaultFieldName, geometry)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).getId(), not(equalTo("2")));
        assertThat(searchResponse.getHits().getAt(1).getId(), not(equalTo("2")));
    }

    public void testIndexPointsRectangle() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName).setId("1").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 1")
            .field(defaultFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName).setId("2").setSource(jsonBuilder()
            .startObject()
            .field("name", "Document 2")
            .field(defaultFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        Rectangle rectangle = new Rectangle(-50, -40, -45, -55);

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(new ShapeQueryBuilder(defaultFieldName, rectangle)
                .relation(ShapeRelation.INTERSECTS))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
    }

    public void testIndexPointsIndexedRectangle() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName).setId("point1").setSource(jsonBuilder()
            .startObject()
            .field(defaultFieldName, "POINT(-30 -30)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(defaultIndexName).setId("point2").setSource(jsonBuilder()
            .startObject()
            .field(defaultFieldName, "POINT(-45 -50)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        String indexedShapeIndex = "indexed_query_shapes";
        String indexedShapePath = "shape";
        String queryShapesMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject(indexedShapePath)
            .field("type", "shape")
            .endObject()
            .endObject()
            .endObject());
        client().admin().indices().prepareCreate(indexedShapeIndex).setMapping(queryShapesMapping).get();
        ensureGreen();

        client().prepareIndex(indexedShapeIndex).setId("shape1").setSource(jsonBuilder()
            .startObject()
            .field(indexedShapePath, "BBOX(-50, -40, -45, -55)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex(indexedShapeIndex).setId("shape2").setSource(jsonBuilder()
            .startObject()
            .field(indexedShapePath, "BBOX(-60, -50, -50, -60)")
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(new ShapeQueryBuilder(defaultFieldName, "shape1")
                .relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex(indexedShapeIndex)
                .indexedShapePath(indexedShapePath))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("point2"));

        searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(new ShapeQueryBuilder(defaultFieldName, "shape2")
                .relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex(indexedShapeIndex)
                .indexedShapePath(indexedShapePath))
            .get();
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
    }

    public void testDistanceQuery() throws Exception {
        client().admin().indices().prepareCreate("test_distance").setMapping("location", "type=shape")
            .execute().actionGet();
        ensureGreen();

        CircleBuilder circleBuilder = new CircleBuilder().center(new Coordinate(1, 0)).radius(10, DistanceUnit.METERS);

        client().index(new IndexRequest("test_distance")
            .source(jsonBuilder().startObject().field("location", new PointBuilder(2, 2)).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest("test_distance")
            .source(jsonBuilder().startObject().field("location", new PointBuilder(3, 1)).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest("test_distance")
            .source(jsonBuilder().startObject().field("location", new PointBuilder(-20, -30)).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest("test_distance")
            .source(jsonBuilder().startObject().field("location", new PointBuilder(20, 30)).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();

        SearchResponse response = client().prepareSearch("test_distance")
            .setQuery(new ShapeQueryBuilder("location", circleBuilder.buildGeometry()).relation(ShapeRelation.WITHIN))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch("test_distance")
            .setQuery(new ShapeQueryBuilder("location", circleBuilder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch("test_distance")
            .setQuery(new ShapeQueryBuilder("location", circleBuilder.buildGeometry()).relation(ShapeRelation.DISJOINT))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch("test_distance")
            .setQuery(new ShapeQueryBuilder("location", circleBuilder.buildGeometry()).relation(ShapeRelation.CONTAINS))
            .get();
        assertEquals(0, response.getHits().getTotalHits().value);
    }
}
