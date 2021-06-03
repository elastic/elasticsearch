/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiLineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.hamcrest.CoreMatchers;

public class ShapeQueryOverPointTests extends ShapeQueryTests {
    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject(defaultFieldName)
            .field("type", "point")
            .endObject().endObject().endObject();

        return xcb;
    }

    public void testProcessRelationSupport() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        Rectangle rectangle = new Rectangle(-35, -25, -25, -35);

        for (ShapeRelation shapeRelation : ShapeRelation.values()) {
            if (shapeRelation.equals(ShapeRelation.INTERSECTS) == false) {
                SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () ->
                    client().prepareSearch("test")
                        .setQuery(new ShapeQueryBuilder(defaultFieldName, rectangle)
                            .relation(shapeRelation))
                        .get());
                assertThat(e.getCause().getMessage(),
                    CoreMatchers.containsString(shapeRelation
                        + " query relation not supported for Field [" + defaultFieldName + "]"));
            }
        }
    }

    public void testQueryLine() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        Line line = new Line(new double[]{-25, -25}, new double[]{-35, -35});

        try {
            client().prepareSearch("test")
                .setQuery(new ShapeQueryBuilder(defaultFieldName, line)).get();
        } catch (
            SearchPhaseExecutionException e) {
            assertThat(e.getCause().getMessage(),
                CoreMatchers.containsString("does not support " + ShapeType.LINESTRING + " queries"));
        }
    }

    public void testQueryLinearRing() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        LinearRing linearRing = new LinearRing(new double[]{-25,-35,-25}, new double[]{-25,-35,-25});

        try {
            // LinearRing extends Line implements Geometry: expose the build process
            ShapeQueryBuilder queryBuilder = new ShapeQueryBuilder(defaultFieldName, linearRing);
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client(), SearchAction.INSTANCE);
            searchRequestBuilder.setQuery(queryBuilder);
            searchRequestBuilder.setIndices("test");
            searchRequestBuilder.get();
        } catch (
            SearchPhaseExecutionException e) {
            assertThat(e.getCause().getMessage(),
                CoreMatchers.containsString("Field [" + defaultFieldName + "] does not support LINEARRING queries"));
        }
    }

    public void testQueryMultiLine() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        CoordinatesBuilder coords1 = new CoordinatesBuilder()
            .coordinate(-35,-35)
            .coordinate(-25,-25);
        CoordinatesBuilder coords2 = new CoordinatesBuilder()
            .coordinate(-15,-15)
            .coordinate(-5,-5);
        LineStringBuilder lsb1 = new LineStringBuilder(coords1);
        LineStringBuilder lsb2 = new LineStringBuilder(coords2);
        MultiLineStringBuilder mlb = new MultiLineStringBuilder().linestring(lsb1).linestring(lsb2);
        MultiLine multiline = (MultiLine) mlb.buildGeometry();

        try {
            client().prepareSearch("test")
                .setQuery(new ShapeQueryBuilder(defaultFieldName, multiline)).get();
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(),
                CoreMatchers.containsString("does not support " + ShapeType.MULTILINESTRING + " queries"));
        }
    }

    public void testQueryMultiPoint() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        MultiPointBuilder mpb = new MultiPointBuilder().coordinate(-35,-25).coordinate(-15,-5);
        MultiPoint multiPoint = mpb.buildGeometry();

        try {
            client().prepareSearch("test")
                .setQuery(new ShapeQueryBuilder(defaultFieldName, multiPoint)).get();
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(),
                CoreMatchers.containsString("does not support " + ShapeType.MULTIPOINT + " queries"));
        }
    }

    public void testQueryPoint() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        PointBuilder pb = new PointBuilder().coordinate(-35, -25);
        Point point = pb.buildGeometry();

        try {
            client().prepareSearch("test")
                .setQuery(new ShapeQueryBuilder(defaultFieldName, point)).get();
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(),
                CoreMatchers.containsString("does not support " + ShapeType.POINT + " queries"));
        }
    }

}
