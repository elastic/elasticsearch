/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.SpatialUtils;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper.ShapeFieldType;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeIndexer;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryProcessor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.xpack.spatial.ingest.CircleProcessor.CircleShapeFieldType;
import static org.elasticsearch.xpack.spatial.ingest.CircleProcessor.CircleShapeFieldType.GEO_SHAPE;
import static org.elasticsearch.xpack.spatial.ingest.CircleProcessor.CircleShapeFieldType.SHAPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CircleProcessorTests extends ESTestCase {

    public void testNumSides() {
        double radiusDistanceMeters = randomDoubleBetween(0.01, 6371000, true);
        CircleShapeFieldType shapeType = randomFrom(SHAPE, GEO_SHAPE);
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, radiusDistanceMeters, shapeType);

        // radius is same as error distance
        assertThat(processor.numSides(radiusDistanceMeters), equalTo(4));
        // radius is much smaller than error distance
        assertThat(processor.numSides(0), equalTo(4));
        // radius is much larger than error distance
        assertThat(processor.numSides(Math.pow(radiusDistanceMeters, 100)), equalTo(1000));
        // radius is 5 times longer than error distance
        assertThat(processor.numSides(5*radiusDistanceMeters), equalTo(10));

    }

    public void testFieldNotFound() throws Exception {
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, 10, GEO_SHAPE);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("not present as part of path [field]"));
    }

    public void testFieldNotFoundWithIgnoreMissing() throws Exception {
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", true, 10, GEO_SHAPE);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullValue() throws Exception {
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, 10, GEO_SHAPE);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] is null, cannot process it."));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", true, 10, GEO_SHAPE);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    @SuppressWarnings("unchecked")
    public void testJson() throws IOException {
        Circle circle = new Circle(101.0, 1.0, 10);
        HashMap<String, Object> map = new HashMap<>();
        HashMap<String, Object> circleMap = new HashMap<>();
        circleMap.put("type", "Circle");
        circleMap.put("coordinates", List.of(circle.getLon(), circle.getLat()));
        circleMap.put("radius", circle.getRadiusMeters() + "m");
        map.put("field", circleMap);
        Geometry expectedPoly = SpatialUtils.createRegularGeoShapePolygon(circle, 4);
        assertThat(expectedPoly, instanceOf(Polygon.class));
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, 10, GEO_SHAPE);
        processor.execute(ingestDocument);
        Map<String, Object> polyMap = ingestDocument.getFieldValue("field", Map.class);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        GeoJson.toXContent(expectedPoly, builder, ToXContent.EMPTY_PARAMS);
        Tuple<XContentType, Map<String, Object>> expected = XContentHelper.convertToMap(BytesReference.bytes(builder),
            true, XContentType.JSON);
        assertThat(polyMap, equalTo(expected.v2()));
    }

    public void testWKT() {
        Circle circle = new Circle(101.0, 0.0, 2);
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", WellKnownText.toWKT(circle));
        Geometry expectedPoly = SpatialUtils.createRegularGeoShapePolygon(circle, 4);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field",false, 2, GEO_SHAPE);
        processor.execute(ingestDocument);
        String polyString = ingestDocument.getFieldValue("field", String.class);
        assertThat(polyString, equalTo(WellKnownText.toWKT(expectedPoly)));
    }

    public void testInvalidWKT() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "invalid");
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, 10, GEO_SHAPE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("invalid circle definition"));
        map.put("field", "POINT (30 10)");
        e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("invalid circle definition"));
    }

    public void testMissingField() {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, 10, GEO_SHAPE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] not present as part of path [field]"));
    }

    public void testInvalidType() {
        Map<String, Object> field = new HashMap<>();
        field.put("coordinates", List.of(100, 100));
        field.put("radius", "10m");
        Map<String, Object> map = new HashMap<>();
        map.put("field", field);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, 10, GEO_SHAPE);

        for (Object value : new Object[] { null, 4.0, "not_circle"}) {
            field.put("type", value);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("invalid circle definition"));
        }
    }

    public void testInvalidCoordinates() {
        Map<String, Object> field = new HashMap<>();
        field.put("type", "circle");
        field.put("radius", "10m");
        Map<String, Object> map = new HashMap<>();
        map.put("field", field);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, 10, GEO_SHAPE);

        for (Object value : new Object[] { null, "not_circle"}) {
            field.put("coordinates", value);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("invalid circle definition"));
        }
    }

    public void testInvalidRadius() {
        Map<String, Object> field = new HashMap<>();
        field.put("type", "circle");
        field.put("coordinates", List.of(100.0, 1.0));
        Map<String, Object> map = new HashMap<>();
        map.put("field", field);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", null, "field", "field", false, 10, GEO_SHAPE);

        for (Object value : new Object[] { null, "NotNumber", "10.0fs"}) {
            field.put("radius", value);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("invalid circle definition"));
        }
    }

    public void testGeoShapeQueryAcrossDateline() throws IOException {
        String fieldName = "circle";
        Circle circle = new Circle(179.999746, 67.1726, randomDoubleBetween(1000, 300000, true));
        int numSides = randomIntBetween(4, 1000);
        Geometry geometry = SpatialUtils.createRegularGeoShapePolygon(circle, numSides);

        GeoShapeWithDocValuesFieldType shapeType
            = new GeoShapeWithDocValuesFieldType(fieldName, true, false, Orientation.RIGHT, null, Collections.emptyMap());

        SearchExecutionContext mockedContext = mock(SearchExecutionContext.class);
        when(mockedContext.getFieldType(any())).thenReturn(shapeType);
        Query sameShapeQuery = shapeType.geoShapeQuery(geometry, fieldName, ShapeRelation.INTERSECTS, mockedContext);
        Query pointOnDatelineQuery = shapeType.geoShapeQuery(new Point(180, circle.getLat()), fieldName,
            ShapeRelation.INTERSECTS, mockedContext);

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            GeoShapeIndexer indexer = new GeoShapeIndexer(true, fieldName);
            for (IndexableField field : indexer.indexShape(indexer.prepareForIndexing(geometry))) {
                doc.add(field);
            }
            w.addDocument(doc);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                assertThat(searcher.search(sameShapeQuery, 1).totalHits.value, equalTo(1L));
                assertThat(searcher.search(pointOnDatelineQuery, 1).totalHits.value, equalTo(1L));
            }
        }
    }

    public void testShapeQuery() throws IOException {
        String fieldName = "circle";
        Circle circle = new Circle(0, 0, 10);
        int numSides = randomIntBetween(4, 1000);
        Geometry geometry = SpatialUtils.createRegularShapePolygon(circle, numSides);

        MappedFieldType shapeType = new ShapeFieldType(fieldName, true, Orientation.RIGHT, null, Collections.emptyMap());

        ShapeQueryProcessor processor = new ShapeQueryProcessor();
        SearchExecutionContext mockedContext = mock(SearchExecutionContext.class);
        when(mockedContext.getFieldType(any())).thenReturn(shapeType);
        Query sameShapeQuery = processor.shapeQuery(geometry, fieldName, ShapeRelation.INTERSECTS, mockedContext);
        Query centerPointQuery = processor.shapeQuery(new Point(circle.getLon(), circle.getLat()), fieldName,
            ShapeRelation.INTERSECTS, mockedContext);

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            ShapeIndexer indexer = new ShapeIndexer(fieldName);
            for (IndexableField field : indexer.indexShape(geometry)) {
                doc.add(field);
            }
            w.addDocument(doc);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                assertThat(searcher.search(sameShapeQuery, 1).totalHits.value, equalTo(1L));
                assertThat(searcher.search(centerPointQuery, 1).totalHits.value, equalTo(1L));
            }
        }
    }
}
