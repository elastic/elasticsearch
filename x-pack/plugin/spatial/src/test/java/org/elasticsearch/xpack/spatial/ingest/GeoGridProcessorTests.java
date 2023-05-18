/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeometryParserFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.xpack.spatial.ingest.GeoGridProcessor.TileFieldType.GEOHASH;
import static org.elasticsearch.xpack.spatial.ingest.GeoGridProcessor.TileFieldType.GEOHEX;
import static org.elasticsearch.xpack.spatial.ingest.GeoGridProcessor.TileFieldType.GEOTILE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GeoGridProcessorTests extends ESTestCase {

    public void testFieldNotFound() {
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.WKT, GEOTILE);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("not present as part of path [field]"));
    }

    public void testFieldNotFoundWithIgnoreMissing() {
        GeoGridProcessor processor = makeGridProcessor(true, GeometryParserFormat.WKT, GEOTILE);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullValue() {
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.WKT, GEOTILE);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] is null, cannot process it."));
    }

    public void testNullValueWithIgnoreMissing() {
        GeoGridProcessor processor = makeGridProcessor(true, GeometryParserFormat.WKT, GEOTILE);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testGeohash2GeoJson() throws IOException {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "u0");
        Geometry expectedPoly = new GeoGridProcessor.GeohashHandler("u0").makeGeometry();
        assertThat(expectedPoly, instanceOf(Rectangle.class));
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.GEOJSON, GEOHASH);
        processor.execute(ingestDocument);
        assertFieldAsGeoJSON(ingestDocument, "field", expectedPoly);
    }

    public void testGeotile2GeoJson() throws IOException {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "6/32/22");
        Geometry expectedPoly = new GeoGridProcessor.GeotileHandler("6/32/22").makeGeometry();
        assertThat(expectedPoly, instanceOf(Rectangle.class));
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.GEOJSON, GEOTILE);
        processor.execute(ingestDocument);
        assertFieldAsGeoJSON(ingestDocument, "field", expectedPoly);
    }

    public void testGeohex2GeoJson() throws IOException {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "811fbffffffffff");
        Geometry expectedPoly = new GeoGridProcessor.GeohexHandler("811fbffffffffff").makeGeometry();
        assertThat(expectedPoly, instanceOf(Polygon.class));
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.GEOJSON, GEOHEX);
        processor.execute(ingestDocument);
        assertFieldAsGeoJSON(ingestDocument, "field", expectedPoly);
    }

    public void testGeohash2WKT() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "u0");
        Geometry expectedPoly = new GeoGridProcessor.GeohashHandler("u0").makeGeometry();
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.WKT, GEOHASH);
        processor.execute(ingestDocument);
        String polyString = ingestDocument.getFieldValue("field", String.class);
        assertThat(polyString, equalTo(WellKnownText.toWKT(expectedPoly)));
    }

    public void testGeotile2WKT() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "6/32/22");
        Geometry expectedPoly = new GeoGridProcessor.GeotileHandler("6/32/22").makeGeometry();
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.WKT, GEOTILE);
        processor.execute(ingestDocument);
        String polyString = ingestDocument.getFieldValue("field", String.class);
        assertThat(polyString, equalTo(WellKnownText.toWKT(expectedPoly)));
    }

    public void testGeohex2WKT() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "811fbffffffffff");
        Geometry expectedPoly = new GeoGridProcessor.GeohexHandler("811fbffffffffff").makeGeometry();
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.WKT, GEOHEX);
        processor.execute(ingestDocument);
        String polyString = ingestDocument.getFieldValue("field", String.class);
        assertThat(polyString, equalTo(WellKnownText.toWKT(expectedPoly)));
    }

    public void testGeohex_WithChildrenAndPrecision() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("geohex", "811fbffffffffff");
        Geometry expectedGeometry = new GeoGridProcessor.GeohexHandler("811fbffffffffff").makeGeometry();
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(
            "geohex",
            "polygon",
            "parent",
            "children",
            "non_children",
            "depth",
            GeometryParserFormat.WKT,
            GEOHEX
        );
        processor.execute(ingestDocument);
        String polyString = ingestDocument.getFieldValue("polygon", String.class);
        assertThat(polyString, equalTo(WellKnownText.toWKT(expectedGeometry)));
        assertTrue("should have parent field", ingestDocument.hasField("parent"));
        String parent = ingestDocument.getFieldValue("parent", String.class);
        assertThat(parent, equalTo("801ffffffffffff"));
        assertTrue("should have children field", ingestDocument.hasField("children"));
        List<?> children = ingestDocument.getFieldValue("children", List.class);
        assertThat(children.size(), equalTo(7));
        assertThat(
            children,
            containsInAnyOrder(
                equalTo("821f87fffffffff"),
                equalTo("821f8ffffffffff"),
                equalTo("821f97fffffffff"),
                equalTo("821f9ffffffffff"),
                equalTo("821fa7fffffffff"),
                equalTo("821faffffffffff"),
                equalTo("821fb7fffffffff")
            )
        );
        assertTrue("should have non_children field", ingestDocument.hasField("non_children"));
        List<?> nonChildren = ingestDocument.getFieldValue("non_children", List.class);
        assertThat(nonChildren.size(), equalTo(6));
        int depth = ingestDocument.getFieldValue("depth", Integer.class);
        assertThat(depth, equalTo(1));
    }

    public void testGeotile_WithChildrenAndPrecision() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "4/8/5");
        Geometry expectedGeometry = new GeoGridProcessor.GeotileHandler("4/8/5").makeGeometry();
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(
            "field",
            "shape",
            "encompassing",
            "inner_tiles",
            "intersecting",
            "zoom",
            GeometryParserFormat.WKT,
            GEOTILE
        );
        processor.execute(ingestDocument);
        String polyString = ingestDocument.getFieldValue("shape", String.class);
        assertThat(polyString, equalTo(WellKnownText.toWKT(expectedGeometry)));
        String encompassing = ingestDocument.getFieldValue("encompassing", String.class);
        assertThat(encompassing, equalTo("3/4/2"));
        assertTrue("should have children field", ingestDocument.hasField("inner_tiles"));
        List<?> innerTiles = ingestDocument.getFieldValue("inner_tiles", List.class);
        assertThat(innerTiles.size(), equalTo(4));
        assertThat(innerTiles, containsInAnyOrder(equalTo("5/16/10"), equalTo("5/17/10"), equalTo("5/16/11"), equalTo("5/17/11")));
        assertFalse("should have no intersecting field", ingestDocument.hasField("intersecting"));
        int zoom = ingestDocument.getFieldValue("zoom", Integer.class);
        assertThat(zoom, equalTo(4));
    }

    public void testGeohash_WithChildrenAndPrecision() throws IOException {
        HashMap<String, Object> map = new HashMap<>();
        map.put("geohash", "u0");
        Geometry expectedGeometry = new GeoGridProcessor.GeohashHandler("u0").makeGeometry();
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(
            "geohash",
            "shape",
            "higher_level",
            "next_level",
            "intersecting",
            "precision",
            GeometryParserFormat.GEOJSON,
            GEOHASH
        );
        processor.execute(ingestDocument);
        assertFieldAsGeoJSON(ingestDocument, "shape", expectedGeometry);
        String higher_level = ingestDocument.getFieldValue("higher_level", String.class);
        assertThat(higher_level, equalTo("u"));
        assertTrue("should have next_level field", ingestDocument.hasField("next_level"));
        List<?> next_level = ingestDocument.getFieldValue("next_level", List.class);
        assertThat(next_level.size(), equalTo(32));
        assertFalse("should have no intersecting field", ingestDocument.hasField("intersecting"));
        int precision = ingestDocument.getFieldValue("precision", Integer.class);
        assertThat(precision, equalTo(2));
    }

    public void testInvalidGeohash() {
        assertInvalidTile(GEOHASH, "unsupported symbol", "invalid geohash");
    }

    public void testInvalidGeotile() {
        assertInvalidTile(GEOTILE, "Must be three integers in a form \"zoom/x/y\"", "invalid tile");
    }

    public void testInvalidGeohex() {
        assertInvalidTile(GEOHEX, "NumberFormatException", "not a hex");
    }

    public void testMissingField() {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.WKT, GEOTILE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] not present as part of path [field]"));
    }

    @SuppressWarnings("unchecked")
    private void assertFieldAsGeoJSON(IngestDocument ingestDocument, String field, Geometry expectedGeometry) throws IOException {
        Map<String, Object> polyMap = ingestDocument.getFieldValue(field, Map.class);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        GeoJson.toXContent(expectedGeometry, builder, ToXContent.EMPTY_PARAMS);
        Tuple<XContentType, Map<String, Object>> expected = XContentHelper.convertToMap(
            BytesReference.bytes(builder),
            true,
            XContentType.JSON
        );
        assertThat(polyMap, equalTo(expected.v2()));
    }

    private void assertInvalidTile(GeoGridProcessor.TileFieldType type, String innerError, String content) {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", content);
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(map);
        GeoGridProcessor processor = makeGridProcessor(false, GeometryParserFormat.WKT, type);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(type + " field '" + map.get("field") + "'", e.getMessage(), equalTo("invalid tile definition"));
        assertThat(type + " field '" + map.get("field") + "'", e.getCause().toString(), containsString(innerError));
        map.put("field", "POINT (30 10)");
        e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(type + " field '" + map.get("field") + "'", e.getMessage(), equalTo("invalid tile definition"));
        assertThat(type + " field '" + map.get("field") + "'", e.getCause().toString(), containsString(innerError));
    }

    private GeoGridProcessor makeGridProcessor(
        boolean ignoreMissing,
        GeometryParserFormat format,
        GeoGridProcessor.TileFieldType tileFieldType
    ) {
        GeoGridProcessor.FieldConfig config = new GeoGridProcessor.FieldConfig("field", "field", null, null, null, null);
        return new GeoGridProcessor("tag", null, config, ignoreMissing, format, tileFieldType);
    }

    private GeoGridProcessor makeGridProcessor(
        String field,
        String targetField,
        String parentField,
        String childrenField,
        String nonChildrenField,
        String precisionField,
        GeometryParserFormat format,
        GeoGridProcessor.TileFieldType tileFieldType
    ) {
        GeoGridProcessor.FieldConfig config = new GeoGridProcessor.FieldConfig(
            field,
            targetField,
            parentField,
            childrenField,
            nonChildrenField,
            precisionField
        );
        return new GeoGridProcessor("tag", null, config, false, format, tileFieldType);
    }
}
