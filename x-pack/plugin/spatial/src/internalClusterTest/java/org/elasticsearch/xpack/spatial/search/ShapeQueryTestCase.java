/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class ShapeQueryTestCase extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateSpatialPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    protected abstract XContentBuilder createDefaultMapping() throws Exception;

    static String defaultFieldName = "xy";
    static String defaultIndexName = "test-points";

    public void testNullShape() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        indicesAdmin().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        prepareIndex(defaultIndexName).setId("aNullshape")
            .setSource("{\"geo\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        GetResponse result = client().prepareGet(defaultIndexName, "aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    };

    public void testIndexPointsFilterRectangle() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        indicesAdmin().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Rectangle rectangle = new Rectangle(-45, 45, 45, -45);

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, rectangle).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            }
        );

        // default query, without specifying relation (expect intersects)

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName).setQuery(new ShapeQueryBuilder(defaultFieldName, rectangle)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            }
        );
    }

    public void testIndexPointsCircle() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        indicesAdmin().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Circle circle = new Circle(-30, -30, 1);

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, circle).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            }
        );
    }

    public void testIndexPointsPolygon() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        indicesAdmin().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Polygon polygon = new Polygon(new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 }));

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, polygon).relation(ShapeRelation.INTERSECTS)),
            response -> {
                SearchHits searchHits = response.getHits();
                assertThat(searchHits.getTotalHits().value, equalTo(1L));
                assertThat(searchHits.getAt(0).getId(), equalTo("1"));
            }
        );
    }

    public void testIndexPointsMultiPolygon() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        indicesAdmin().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-40 -40)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("3")
            .setSource(jsonBuilder().startObject().field("name", "Document 3").field(defaultFieldName, "POINT(-50 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Polygon encloseDocument1Shape = new Polygon(
            new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 })
        );
        Polygon encloseDocument2Shape = new Polygon(
            new LinearRing(new double[] { -55, -55, -45, -45, -55 }, new double[] { -55, -45, -45, -55, -55 })
        );

        MultiPolygon mp = new MultiPolygon(List.of(encloseDocument1Shape, encloseDocument2Shape));

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, mp).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getHits().length, equalTo(2));
                assertThat(response.getHits().getAt(0).getId(), not(equalTo("2")));
                assertThat(response.getHits().getAt(1).getId(), not(equalTo("2")));
            }
        );
    }

    public void testIndexPointsRectangle() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        indicesAdmin().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Rectangle rectangle = new Rectangle(-50, -40, -45, -55);

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, rectangle).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            }
        );
    }

    public void testIndexPointsIndexedRectangle() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        indicesAdmin().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        prepareIndex(defaultIndexName).setId("point1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("point2")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        String indexedShapeIndex = "indexed_query_shapes";
        String indexedShapePath = "shape";
        String queryShapesMapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(indexedShapePath)
                .field("type", "shape")
                .endObject()
                .endObject()
                .endObject()
        );
        indicesAdmin().prepareCreate(indexedShapeIndex).setMapping(queryShapesMapping).get();
        ensureGreen();

        prepareIndex(indexedShapeIndex).setId("shape1")
            .setSource(jsonBuilder().startObject().field(indexedShapePath, "BBOX(-50, -40, -45, -55)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(indexedShapeIndex).setId("shape2")
            .setSource(jsonBuilder().startObject().field(indexedShapePath, "BBOX(-60, -50, -50, -60)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(
                    new ShapeQueryBuilder(defaultFieldName, "shape1").relation(ShapeRelation.INTERSECTS)
                        .indexedShapeIndex(indexedShapeIndex)
                        .indexedShapePath(indexedShapePath)
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("point2"));
            }
        );

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName)
                .setQuery(
                    new ShapeQueryBuilder(defaultFieldName, "shape2").relation(ShapeRelation.INTERSECTS)
                        .indexedShapeIndex(indexedShapeIndex)
                        .indexedShapePath(indexedShapePath)
                ),
            0L
        );
    }

    public void testDistanceQuery() throws Exception {
        indicesAdmin().prepareCreate("test_distance").setMapping("location", "type=shape").get();
        ensureGreen();

        Circle circle = new Circle(1, 0, 10);

        client().index(
            new IndexRequest("test_distance").source(
                jsonBuilder().startObject().field("location", WellKnownText.toWKT(new Point(2, 2))).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test_distance").source(
                jsonBuilder().startObject().field("location", WellKnownText.toWKT(new Point(3, 1))).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test_distance").source(
                jsonBuilder().startObject().field("location", WellKnownText.toWKT(new Point(-20, -30))).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test_distance").source(
                jsonBuilder().startObject().field("location", WellKnownText.toWKT(new Point(20, 30))).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();

        assertHitCount(
            client().prepareSearch("test_distance").setQuery(new ShapeQueryBuilder("location", circle).relation(ShapeRelation.WITHIN)),
            2L
        );

        assertHitCount(
            client().prepareSearch("test_distance").setQuery(new ShapeQueryBuilder("location", circle).relation(ShapeRelation.INTERSECTS)),
            2L
        );

        assertHitCount(
            client().prepareSearch("test_distance").setQuery(new ShapeQueryBuilder("location", circle).relation(ShapeRelation.DISJOINT)),
            2L
        );

        assertHitCount(
            client().prepareSearch("test_distance").setQuery(new ShapeQueryBuilder("location", circle).relation(ShapeRelation.CONTAINS)),
            0L
        );
    }
}
