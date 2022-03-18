/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class GeoPointShapeQueryTests extends GeoPointShapeQueryTestCase {

    @Override
    protected void createMapping(String indexName, String fieldName, Settings settings) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "geo_point")
            .endObject()
            .endObject()
            .endObject();
        client().admin().indices().prepareCreate(indexName).setMapping(xcb).setSettings(settings).get();
    }

    public void testFieldAlias() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(defaultGeoFieldName)
                .field("type", "geo_point")
                .endObject()
                .startObject("alias")
                .field("type", "alias")
                .field("path", defaultGeoFieldName)
                .endObject()
                .endObject()
                .endObject()
        );

        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        Point point = GeometryTestUtils.randomPoint(false);
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultGeoFieldName, WellKnownText.toWKT(point)).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(geoShapeQuery("alias", point)).get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testQueryPointFromMultiPointFormats() throws Exception {
        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        double[] geojsonDoubles = new double[] { 45.0, 35.0 };
        HashMap<String, Object> geojson = new HashMap<>();
        geojson.put("type", "Point");
        geojson.put("coordinates", geojsonDoubles);
        double[] pointDoubles = new double[] { 35.0, 25.0 };
        Object[] points = new Object[] { "-35, -45", "POINT(-35 -25)", pointDoubles, geojson };
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultGeoFieldName, points).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Point pointA = new Point(-45, -35);
        Point pointB = new Point(-35, -25);
        Point pointC = new Point(35, 25);
        Point pointD = new Point(45, 35);
        Point pointInvalid = new Point(-35, -35);
        for (Point point : new Point[] { pointA, pointB, pointC, pointD, pointInvalid }) {
            int expectedDocs = point.equals(pointInvalid) ? 0 : 1;
            int disjointDocs = point.equals(pointInvalid) ? 1 : 0;
            {
                SearchResponse response = client().prepareSearch(defaultIndexName)
                    .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, point))
                    .get();
                SearchHits searchHits = response.getHits();
                assertEquals("Doc matches %s" + point, expectedDocs, searchHits.getTotalHits().value);
            }
            {
                SearchResponse response = client().prepareSearch(defaultIndexName)
                    .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, point).relation(ShapeRelation.WITHIN))
                    .get();
                SearchHits searchHits = response.getHits();
                assertEquals("Doc WITHIN %s" + point, 0, searchHits.getTotalHits().value);
            }
            {
                SearchResponse response = client().prepareSearch(defaultIndexName)
                    .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, point).relation(ShapeRelation.CONTAINS))
                    .get();
                SearchHits searchHits = response.getHits();
                assertEquals("Doc CONTAINS %s" + point, expectedDocs, searchHits.getTotalHits().value);
            }
            {
                SearchResponse response = client().prepareSearch(defaultIndexName)
                    .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, point).relation(ShapeRelation.DISJOINT))
                    .get();
                SearchHits searchHits = response.getHits();
                assertEquals("Doc DISJOINT with %s" + point, disjointDocs, searchHits.getTotalHits().value);
            }
        }
    }
}
