/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.SimpleVectorTileFormatter;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.xpack.vectortile.SpatialGeometryFormatterExtension;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

public class GeoShapeWithDocValuesFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        final GeoFormatterFactory<Geometry> geoFormatterFactory = new GeoFormatterFactory<>(
            new SpatialGeometryFormatterExtension().getGeometryFormatterFactories()
        );
        final MappedFieldType mapper = new GeoShapeWithDocValuesFieldMapper.Builder(
            "field",
            Version.CURRENT,
            false,
            false,
            geoFormatterFactory
        ).build(MapperBuilderContext.root(false)).fieldType();

        Map<String, Object> jsonLineString = Map.of("type", "LineString", "coordinates", List.of(List.of(42.0, 27.1), List.of(30.0, 50.0)));
        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(14.0, 15.0));
        Map<String, Object> jsonMalformed = Map.of("type", "Point", "coordinates", "foo");
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.0 15.0)";
        String wktMalformed = "POINT foo";

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed single shape in geojson format
        sourceValue = jsonMalformed;
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in geojson format.
        sourceValue = List.of(jsonLineString, jsonPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes including one malformed in geojson format
        sourceValue = List.of(jsonLineString, jsonMalformed, jsonPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single malformed shape in wkt format
        sourceValue = wktMalformed;
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = List.of(wktLineString, wktPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes including one malformed in wkt format
        sourceValue = List.of(wktLineString, wktMalformed, wktPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }

    public void testFetchStoredValue() throws IOException {
        final GeoFormatterFactory<Geometry> geoFormatterFactory = new GeoFormatterFactory<>(
            new SpatialGeometryFormatterExtension().getGeometryFormatterFactories()
        );

        final MappedFieldType mapper = new GeoShapeWithDocValuesFieldMapper.Builder(
            "field",
            Version.CURRENT,
            false,
            false,
            geoFormatterFactory
        ).setStored(true).build(MapperBuilderContext.root(randomBoolean())).fieldType();

        ByteOrder byteOrder = randomBoolean() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        Map<String, Object> jsonLineString = Map.of("type", "LineString", "coordinates", List.of(List.of(42.0, 27.1), List.of(30.0, 50.0)));
        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(14.0, 15.0));
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.0 15.0)";

        BytesRef wkbLineString = new BytesRef(
            WellKnownBinary.toWKB(new Line(new double[] { 42.0, 30.0 }, new double[] { 27.1, 50.0 }), byteOrder)
        );
        BytesRef wkbPoint = new BytesRef(WellKnownBinary.toWKB(new Point(14.0, 15.0), byteOrder));
        // Test a single shape in wkb format.
        List<Object> storedValues = List.of(wkbLineString);
        assertEquals(List.of(jsonLineString), fetchStoredValue(mapper, storedValues, null));
        assertEquals(List.of(wktLineString), fetchStoredValue(mapper, storedValues, "wkt"));

        // Test a list of shapes in wkb format.
        storedValues = List.of(wkbLineString, wkbPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchStoredValue(mapper, storedValues, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchStoredValue(mapper, storedValues, "wkt"));
    }

    public void testFetchVectorTile() throws IOException {
        fetchVectorTile(GeometryTestUtils.randomPoint());
        fetchVectorTile(GeometryTestUtils.randomMultiPoint(false));
        fetchVectorTile(GeometryTestUtils.randomRectangle());
        fetchVectorTile(GeometryTestUtils.randomLine(false));
        fetchVectorTile(GeometryTestUtils.randomMultiLine(false));
        fetchVectorTile(GeometryTestUtils.randomPolygon(false));
        fetchVectorTile(GeometryTestUtils.randomMultiPolygon(false));
    }

    private void fetchVectorTile(Geometry geometry) throws IOException {
        final GeoFormatterFactory<Geometry> geoFormatterFactory = new GeoFormatterFactory<>(
            new SpatialGeometryFormatterExtension().getGeometryFormatterFactories()
        );
        final MappedFieldType mapper = new GeoShapeWithDocValuesFieldMapper.Builder(
            "field",
            Version.CURRENT,
            false,
            false,
            geoFormatterFactory
        ).build(MapperBuilderContext.root(false)).fieldType();
        final int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        final StringBuilder mvtString = new StringBuilder("mvt(");
        mvtString.append(z).append("/").append(x).append("/").append(y);
        int extent = SimpleVectorTileFormatter.DEFAULT_EXTENT;
        int padPixels = SimpleVectorTileFormatter.DEFAULT_BUFFER_PIXELS;
        if (randomBoolean()) {
            extent = randomIntBetween(1 << 8, 1 << 14);
            mvtString.append(SimpleVectorTileFormatter.EXTENT_PREFIX).append(extent);
        }
        if (randomBoolean()) {
            padPixels = randomIntBetween(0, extent);
            mvtString.append(SimpleVectorTileFormatter.BUFFER_PREFIX).append(padPixels);
        }
        mvtString.append(")");
        final FeatureFactory featureFactory = new FeatureFactory(z, x, y, extent, padPixels);

        final List<?> sourceValue = fetchSourceValue(mapper, WellKnownText.toWKT(geometry), mvtString.toString());
        List<byte[]> features;
        try {
            features = featureFactory.getFeatures(normalize(geometry));
        } catch (IllegalArgumentException iae) {
            // if parsing fails means that we must be ignoring malformed values. In case of mvt might
            // happen that the geometry is out of range (close to the poles).
            features = List.of();
        }
        assertThat(features.size(), Matchers.equalTo(sourceValue.size()));
        for (int i = 0; i < features.size(); i++) {
            assertThat(sourceValue.get(i), Matchers.equalTo(features.get(i)));
        }
    }

    private Geometry normalize(Geometry geometry) {
        if (GeometryNormalizer.needsNormalize(Orientation.CCW, geometry)) {
            return GeometryNormalizer.apply(Orientation.CCW, geometry);
        } else {
            return geometry;
        }
    }

    public void testFetchSourcePolygonDateLine() throws Exception {
        assertFetchGeometry(
            "POLYGON((170 -10, -170 -10, -170 10, 170 10, 170 -10))",
            "MULTIPOLYGON (((180.0 -10.0, 180.0 10.0, 170.0 10.0, 170.0 -10.0, 180.0 -10.0)),"
                + "((-180.0 10.0, -180.0 -10.0, -170.0 -10.0, -170.0 10.0, -180.0 10.0)))",
            Map.of(
                "type",
                "MultiPolygon",
                "coordinates",
                List.of(
                    List.of(
                        List.of(
                            List.of(180.0, -10.0),
                            List.of(180.0, 10.0),
                            List.of(170.0, 10.0),
                            List.of(170.0, -10.0),
                            List.of(180.0, -10.0)
                        )
                    ),
                    List.of(
                        List.of(
                            List.of(-180.0, 10.0),
                            List.of(-180.0, -10.0),
                            List.of(-170.0, -10.0),
                            List.of(-170.0, 10.0),
                            List.of(-180.0, 10.0)
                        )
                    )
                )
            ),
            "MULTIPOLYGON (((180.0 -10.0, 180.0 10.0, 170.0 10.0, 170.0 -10.0, 180.0 -10.0)),"
                + "((-180.0 10.0, -180.0 -10.0, -170.0 -10.0, -170.0 10.0, -180.0 10.0)))"
        );
    }

    public void testFetchSourceEnvelope() throws Exception {
        assertFetchGeometry(
            "BBOX(-10, 10, 10, -10)",
            "BBOX (-10.0, 10.0, 10.0, -10.0)",
            Map.of("type", "Envelope", "coordinates", List.of(List.of(-10.0, 10.0), List.of(10.0, -10.0))),
            "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10)))"
        );
    }

    public void testFetchSourceEnvelopeDateLine() throws Exception {
        assertFetchGeometry(
            "BBOX(10, -10, 10, -10)",
            "BBOX (10.0, -10.0, 10.0, -10.0)",
            Map.of("type", "Envelope", "coordinates", List.of(List.of(10.0, 10.0), List.of(-10.0, -10.0))),
            "MULTIPOLYGON (((-180 -10, -10 -10, -10 10, -180 10, -180 -10)), ((10 -10, 180 -10, 180 10, 10 10, 10 -10)))"
        );
    }

    private void assertFetchGeometry(Object sourceValue, String wktValue, Map<String, Object> jsonValue, String mvtEquivalentAsWKT)
        throws Exception {
        assertFetchSourceGeometry(sourceValue, wktValue, jsonValue);
        assertFetchSourceMVT(sourceValue, mvtEquivalentAsWKT);
        assertFetchStoredGeometry(wktValue, jsonValue);
    }

    private void assertFetchSourceGeometry(Object sourceValue, String wktValue, Map<String, Object> jsonValue) throws IOException {
        final GeoFormatterFactory<Geometry> geoFormatterFactory = new GeoFormatterFactory<>(
            new SpatialGeometryFormatterExtension().getGeometryFormatterFactories()
        );
        final MappedFieldType mapper = new GeoShapeWithDocValuesFieldMapper.Builder(
            "field",
            Version.CURRENT,
            false,
            false,
            geoFormatterFactory
        ).build(MapperBuilderContext.root(false)).fieldType();

        assertEquals(List.of(jsonValue), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktValue), fetchSourceValue(mapper, sourceValue, "wkt"));
    }

    private void assertFetchStoredGeometry(String wktValue, Map<String, Object> jsonValue) throws Exception {
        final GeoFormatterFactory<Geometry> geoFormatterFactory = new GeoFormatterFactory<>(
            new SpatialGeometryFormatterExtension().getGeometryFormatterFactories()
        );
        final MappedFieldType mapper = new GeoShapeWithDocValuesFieldMapper.Builder(
            "field",
            Version.CURRENT,
            false,
            false,
            geoFormatterFactory
        ).setStored(true).build(MapperBuilderContext.root(false)).fieldType();

        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(false), false, wktValue);

        BytesRef wkb = new BytesRef(WellKnownBinary.toWKB(geometry, randomBoolean() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN));

        assertEquals(List.of(jsonValue), fetchStoredValue(mapper, List.of(wkb), null));
        assertEquals(List.of(wktValue), fetchStoredValue(mapper, List.of(wkb), "wkt"));
    }

    private void assertFetchSourceMVT(Object sourceValue, String mvtEquivalentAsWKT) throws IOException {
        final GeoFormatterFactory<Geometry> geoFormatterFactory = new GeoFormatterFactory<>(
            new SpatialGeometryFormatterExtension().getGeometryFormatterFactories()
        );
        final MappedFieldType mapper = new GeoShapeWithDocValuesFieldMapper.Builder(
            "field",
            Version.CURRENT,
            false,
            false,
            geoFormatterFactory
        ).build(MapperBuilderContext.root(false)).fieldType();

        final int extent = randomIntBetween(256, 4096);
        List<?> mvtExpected = fetchSourceValue(mapper, mvtEquivalentAsWKT, "mvt(0/0/0@" + extent + ")");
        List<?> mvt = fetchSourceValue(mapper, sourceValue, "mvt(0/0/0@" + extent + ")");
        assertThat(mvt.size(), Matchers.equalTo(1));
        assertThat(mvt.size(), Matchers.equalTo(mvtExpected.size()));
        assertThat(mvtExpected.get(0), Matchers.instanceOf(byte[].class));
        assertThat(mvt.get(0), Matchers.instanceOf(byte[].class));
        assertThat((byte[]) mvt.get(0), Matchers.equalTo((byte[]) mvtExpected.get(0)));
    }
}
