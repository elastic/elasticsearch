/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.hamcrest.CoreMatchers;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class ShapeQueryTestCase extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateSpatialPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        String mapping = Strings.toString(createDefaultMapping());
        indicesAdmin().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        prepareIndex(defaultIndexName).setId("aNullshape")
            .setSource("{\"" + defaultFieldName + "\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().field("name", "Document 1").field(defaultFieldName, "POINT(-30 -30)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        prepareIndex(defaultIndexName).setId("2")
            .setSource(jsonBuilder().startObject().field("name", "Document 2").field(defaultFieldName, "POINT(-45 -50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(defaultIndexName).setId("3")
            .setSource(jsonBuilder().startObject().field("name", "Document 3").field(defaultFieldName, "POINT(50 50)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(defaultIndexName).setId("4")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "Document 4")
                    .field(defaultFieldName, new String[] { "POINT(-30 -30)", "POINT(50 50)" })
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(defaultIndexName).setId("5")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "Document 5")
                    .field(defaultFieldName, new String[] { "POINT(60 60)", "POINT(50 50)" })
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
    }

    protected abstract XContentBuilder createDefaultMapping() throws Exception;

    static String defaultFieldName = "xy";
    static String defaultIndexName = "test-points";

    public void testNullShape() {
        GetResponse result = client().prepareGet(defaultIndexName, "aNullshape").get();
        assertThat(result.getField(defaultFieldName), nullValue());
    };

    public void testIndexPointsFilterRectangle() {
        Rectangle rectangle = new Rectangle(-45, 45, 45, -45);

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, rectangle).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                assertThat(response.getHits().getHits().length, equalTo(2));
                assertThat(response.getHits().getAt(0).getId(), anyOf(equalTo("1"), equalTo("4")));
                assertThat(response.getHits().getAt(1).getId(), anyOf(equalTo("1"), equalTo("4")));
            }
        );

        // default query, without specifying relation (expect intersects)
        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName).setQuery(new ShapeQueryBuilder(defaultFieldName, rectangle)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                assertThat(response.getHits().getHits().length, equalTo(2));
                assertThat(response.getHits().getAt(0).getId(), anyOf(equalTo("1"), equalTo("4")));
                assertThat(response.getHits().getAt(1).getId(), anyOf(equalTo("1"), equalTo("4")));
            }
        );
    }

    public void testIndexPointsCircle() {
        Circle circle = new Circle(-30, -30, 1);

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, circle).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                assertThat(response.getHits().getHits().length, equalTo(2));
                assertThat(response.getHits().getAt(0).getId(), anyOf(equalTo("1"), equalTo("4")));
                assertThat(response.getHits().getAt(1).getId(), anyOf(equalTo("1"), equalTo("4")));
            }
        );
    }

    public void testIndexPointsPolygon() {
        Polygon polygon = new Polygon(new LinearRing(new double[] { -35, -35, -25, -25, -35 }, new double[] { -35, -25, -25, -35, -35 }));

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, polygon).relation(ShapeRelation.INTERSECTS)),
            response -> {
                SearchHits searchHits = response.getHits();
                assertThat(searchHits.getTotalHits().value(), equalTo(2L));
                assertThat(searchHits.getAt(0).getId(), anyOf(equalTo("1"), equalTo("4")));
                assertThat(searchHits.getAt(1).getId(), anyOf(equalTo("1"), equalTo("4")));
            }
        );
    }

    public void testIndexPointsMultiPolygon() {
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
                assertThat(response.getHits().getTotalHits().value(), equalTo(3L));
                assertThat(response.getHits().getHits().length, equalTo(3));
                assertThat(response.getHits().getAt(0).getId(), not(equalTo("3")));
                assertThat(response.getHits().getAt(1).getId(), not(equalTo("3")));
                assertThat(response.getHits().getAt(2).getId(), not(equalTo("3")));
            }
        );
    }

    public void testIndexPointsRectangle() {
        Rectangle rectangle = new Rectangle(-50, -40, -45, -55);

        assertNoFailuresAndResponse(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, rectangle).relation(ShapeRelation.INTERSECTS)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            }
        );
    }

    public void testIndexPointsIndexedRectangle() throws Exception {
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
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getHits().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
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

    public void testDistanceQuery() {
        Circle circle = new Circle(-25, -25, 10);

        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, circle).relation(ShapeRelation.INTERSECTS)),
            2L
        );

        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, circle).relation(ShapeRelation.WITHIN)),
            1L
        );

        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, circle).relation(ShapeRelation.DISJOINT)),
            3L
        );

        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, circle).relation(ShapeRelation.CONTAINS)),
            0L
        );
    }

    public void testIndexPointsQueryLinearRing() {
        LinearRing linearRing = new LinearRing(new double[] { -50, -50 }, new double[] { 50, 50 });

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ShapeQueryBuilder(defaultFieldName, linearRing)
        );
        assertThat(ex.getMessage(), CoreMatchers.containsString("[LINEARRING] geometries are not supported"));

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ShapeQueryBuilder(defaultFieldName, new GeometryCollection<>(List.of(linearRing)))
        );
        assertThat(ex.getMessage(), CoreMatchers.containsString("[LINEARRING] geometries are not supported"));
    }

    public void testIndexPointsQueryLine() {
        Line line = new Line(new double[] { 100, -30 }, new double[] { -100, -30 });
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, line).relation(ShapeRelation.INTERSECTS)),
            2L
        );
    }

    public void testIndexPointsQueryMultiLine() {
        MultiLine multiLine = new MultiLine(
            List.of(
                new Line(new double[] { 100, -30 }, new double[] { -100, -30 }),
                new Line(new double[] { 100, -20 }, new double[] { -100, -20 })
            )
        );

        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, multiLine).relation(ShapeRelation.INTERSECTS)),
            2L
        );
    }

    public void testIndexPointsQueryPoint() {
        Point point = new Point(-30, -30);
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, point).relation(ShapeRelation.INTERSECTS)),
            2L
        );
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, point).relation(ShapeRelation.WITHIN)),
            1L
        );
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, point).relation(ShapeRelation.CONTAINS)),
            2L
        );
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, point).relation(ShapeRelation.DISJOINT)),
            3L
        );
    }

    public void testIndexPointsQueryMultiPoint() {
        MultiPoint multiPoint = new MultiPoint(List.of(new Point(-30, -30), new Point(50, 50)));
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, multiPoint).relation(ShapeRelation.INTERSECTS)),
            4L
        );
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, multiPoint).relation(ShapeRelation.WITHIN)),
            3L
        );
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, multiPoint).relation(ShapeRelation.CONTAINS)),
            1L
        );
        assertHitCount(
            client().prepareSearch(defaultIndexName)
                .setQuery(new ShapeQueryBuilder(defaultFieldName, multiPoint).relation(ShapeRelation.DISJOINT)),
            1L
        );
    }
}
