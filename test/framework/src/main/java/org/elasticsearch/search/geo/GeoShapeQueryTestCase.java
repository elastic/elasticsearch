/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.geo;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public abstract class GeoShapeQueryTestCase extends BaseShapeQueryTestCase<GeoShapeQueryBuilder> {

    private final SpatialQueryBuilders<GeoShapeQueryBuilder> geoShapeQueryBuilder = SpatialQueryBuilders.GEO;

    @Override
    protected SpatialQueryBuilders<GeoShapeQueryBuilder> queryBuilder() {
        return geoShapeQueryBuilder;
    }

    @Override
    protected String fieldTypeName() {
        return "geo_shape";
    }

    private final DatelinePointShapeQueryTestCase dateline = new DatelinePointShapeQueryTestCase();

    public void testRectangleSpanningDateline() throws Exception {
        dateline.testRectangleSpanningDateline(this);
    }

    public void testPolygonSpanningDateline() throws Exception {
        dateline.testPolygonSpanningDateline(this);
    }

    public void testMultiPolygonSpanningDateline() throws Exception {
        dateline.testMultiPolygonSpanningDateline(this);
    }

    // Test for issue #34418
    public void testEnvelopeSpanningDateline() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        String doc1 = """
            {
              "geo": {
                "coordinates": [ -33.918711, 18.847685 ],
                "type": "Point"
              }
            }""";
        client().index(new IndexRequest(defaultIndexName).id("1").source(doc1, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc2 = """
            {
              "geo": {
                "coordinates": [ -49, 18.847685 ],
                "type": "Point"
              }
            }""";
        client().index(new IndexRequest(defaultIndexName).id("2").source(doc2, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc3 = """
            {
              "geo": {
                "coordinates": [ 49, 18.847685 ],
                "type": "Point"
              }
            }""";
        client().index(new IndexRequest(defaultIndexName).id("3").source(doc3, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        @SuppressWarnings("unchecked")
        CheckedSupplier<GeoShapeQueryBuilder, IOException> querySupplier = randomFrom(
            () -> queryBuilder().shapeQuery(defaultFieldName, new Rectangle(-21, -39, 44, 9)).relation(ShapeRelation.WITHIN),
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(defaultFieldName)
                    .startObject("shape")
                    .field("type", "envelope")
                    .startArray("coordinates")
                    .startArray()
                    .value(-21)
                    .value(44)
                    .endArray()
                    .startArray()
                    .value(-39)
                    .value(9)
                    .endArray()
                    .endArray()
                    .endObject()
                    .field("relation", "within")
                    .endObject()
                    .endObject();
                try (XContentParser parser = createParser(builder)) {
                    parser.nextToken();
                    return GeoShapeQueryBuilder.fromXContent(parser);
                }
            },
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(defaultFieldName)
                    .field("shape", "BBOX (-21, -39, 44, 9)")
                    .field("relation", "within")
                    .endObject()
                    .endObject();
                try (XContentParser parser = createParser(builder)) {
                    parser.nextToken();
                    return GeoShapeQueryBuilder.fromXContent(parser);
                }
            }
        );
        assertResponse(client().prepareSearch(defaultIndexName).setQuery(querySupplier.get()), response -> {
            assertEquals(2, response.getHits().getTotalHits().value());
            assertNotEquals("1", response.getHits().getAt(0).getId());
            assertNotEquals("1", response.getHits().getAt(1).getId());
        });
    }

    public void testIndexRectangleSpanningDateLine() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Rectangle envelope = new Rectangle(178, -178, 10, -10);

        XContentBuilder docSource = GeoJson.toXContent(envelope, jsonBuilder().startObject().field(defaultFieldName), null).endObject();
        prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        Point filterShape = new Point(179, 0);

        assertHitCountAndNoFailures(
            client().prepareSearch(defaultIndexName).setQuery(queryBuilder().intersectionQuery(defaultFieldName, filterShape)),
            1
        );
    }

    protected Line makeRandomLine() {
        return randomValueOtherThanMany(
            l -> GeometryNormalizer.needsNormalize(Orientation.CCW, l),
            () -> GeometryTestUtils.randomLine(false)
        );
    }

    protected Polygon makeRandomPolygon() {
        return randomValueOtherThanMany(
            p -> GeometryNormalizer.needsNormalize(Orientation.CCW, p),
            () -> GeometryTestUtils.randomPolygon(false)
        );
    }

    protected GeometryCollection<Geometry> makeRandomGeometryCollection() {
        return GeometryTestUtils.randomGeometryCollection(false);
    }

    protected GeometryCollection<Geometry> makeRandomGeometryCollectionWithoutCircle(Geometry... extra) {
        GeometryCollection<Geometry> randomCollection = GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
        if (extra.length == 0) return randomCollection;

        List<Geometry> geometries = new ArrayList<>();
        for (Geometry geometry : randomCollection) {
            geometries.add(geometry);
        }
        Collections.addAll(geometries, extra);
        return new GeometryCollection<>(geometries);
    }

    protected Point nextPoint() {
        return GeometryTestUtils.randomPoint(false);
    }

    protected Polygon nextPolygon() {
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();
        return new Polygon(new LinearRing(randomPoly.getPolyLons(), randomPoly.getPolyLats()));
    }

    protected Polygon nextPolygon2() {
        return GeometryTestUtils.randomPolygon(false);
    }
}
