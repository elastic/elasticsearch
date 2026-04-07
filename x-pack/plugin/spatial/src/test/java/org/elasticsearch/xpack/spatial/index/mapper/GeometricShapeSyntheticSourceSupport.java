/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.lucene.tests.util.LuceneTestCase.rarely;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomList;

/**
 * Synthetic source support for fields the index geometry shapes: shape, geo_shape.
 */
public class GeometricShapeSyntheticSourceSupport implements MapperTestCase.SyntheticSourceSupport {
    private final FieldType fieldType;
    private final boolean ignoreMalformed;

    public GeometricShapeSyntheticSourceSupport(FieldType fieldType, boolean ignoreMalformed) {
        this.fieldType = fieldType;
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    public boolean preservesExactSource() {
        return true;
    }

    @Override
    public MapperTestCase.SyntheticSourceExample example(int maxValues) throws IOException {
        if (randomBoolean()) {
            Value v = generateValue();
            return new MapperTestCase.SyntheticSourceExample(v.input, v.output, this::mapping);
        }

        List<Value> values = randomList(1, maxValues, this::generateValue);
        List<Object> in = values.stream().map(Value::input).toList();
        List<Object> out = values.stream().map(Value::output).toList();

        return new MapperTestCase.SyntheticSourceExample(in, out, this::mapping);
    }

    private record Value(Object input, Object output) {}

    private Value generateValue() {
        if (ignoreMalformed && randomBoolean()) {
            List<Supplier<Object>> choices = List.of(
                () -> randomAlphaOfLength(3),
                ESTestCase::randomInt,
                ESTestCase::randomLong,
                ESTestCase::randomFloat,
                ESTestCase::randomDouble
            );
            Object v = randomFrom(choices).get();
            return new Value(v, v);
        }
        if (randomBoolean()) {
            return new Value(null, null);
        }

        var type = randomFrom(ShapeType.values());
        var isGeoJson = randomBoolean();

        while (true) {
            Geometry candidateGeometry = switch (type) {
                // LINEARRING and CIRCLE are not supported as inputs to fields so just return points
                case POINT, LINEARRING, CIRCLE -> GeometryTestUtils.randomPoint(false);
                case MULTIPOINT -> GeometryTestUtils.randomMultiPoint(false);
                case LINESTRING -> GeometryTestUtils.randomLine(false);
                case MULTILINESTRING -> GeometryTestUtils.randomMultiLine(false);
                case POLYGON -> GeometryTestUtils.randomPolygon(false);
                case MULTIPOLYGON -> GeometryTestUtils.randomMultiPolygon(false);
                case GEOMETRYCOLLECTION -> GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
                case ENVELOPE -> GeometryTestUtils.randomRectangle();
            };

            try {
                if (fieldType == FieldType.GEO_SHAPE) {
                    GeoTestUtils.binaryGeoShapeDocValuesField("f", candidateGeometry);
                } else {
                    GeoTestUtils.binaryCartesianShapeDocValuesField("f", candidateGeometry);
                }

                if (type == ShapeType.ENVELOPE) {
                    var wktString = WellKnownText.toWKT(candidateGeometry);

                    return new Value(wktString, wktString);
                }

                return value(candidateGeometry, isGeoJson);
            } catch (IllegalArgumentException ignored) {
                // It's malformed somehow, loop
            }
        }
    }

    private Value value(Geometry geometry, boolean isGeoJson) {
        var wktString = WellKnownText.toWKT(geometry);

        if (isGeoJson) {
            var map = GeoJson.toMap(geometry);
            return new Value(map, map);
        }

        return new Value(wktString, wktString);
    }

    private void mapping(XContentBuilder b) throws IOException {
        b.field("type", fieldType.getName());
        if (rarely()) {
            b.field("index", false);
        }
        if (rarely()) {
            b.field("doc_values", false);
        }
        if (ignoreMalformed) {
            b.field("ignore_malformed", true);
        }
    }

    @Override
    public List<MapperTestCase.SyntheticSourceInvalidExample> invalidExample() throws IOException {
        return List.of();
    }

    public enum FieldType {
        GEO_SHAPE("geo_shape"),
        SHAPE("shape");

        private final String name;

        FieldType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
