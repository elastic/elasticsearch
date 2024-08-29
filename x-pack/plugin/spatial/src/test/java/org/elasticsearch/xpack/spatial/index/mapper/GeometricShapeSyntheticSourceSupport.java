/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

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
    private final boolean ignoreMalformed;

    public GeometricShapeSyntheticSourceSupport(boolean ignoreMalformed) {
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
            if (v.blockLoaderOutput != null) {
                return new MapperTestCase.SyntheticSourceExample(v.input, v.output, v.blockLoaderOutput, this::mapping);
            }
            return new MapperTestCase.SyntheticSourceExample(v.input, v.output, this::mapping);
        }

        List<Value> values = randomList(1, maxValues, this::generateValue);
        List<Object> in = values.stream().map(Value::input).toList();
        List<Object> out = values.stream().map(Value::output).toList();

        // Block loader infrastructure will never return nulls
        List<Object> outBlockList = values.stream()
            .filter(v -> v.input != null)
            .map(v -> v.blockLoaderOutput != null ? v.blockLoaderOutput : v.output)
            .toList();
        var outBlock = outBlockList.size() == 1 ? outBlockList.get(0) : outBlockList;

        return new MapperTestCase.SyntheticSourceExample(in, out, outBlock, this::mapping);
    }

    private record Value(Object input, Object output, String blockLoaderOutput) {
        Value(Object input, Object output) {
            this(input, output, null);
        }
    }

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

        return switch (type) {
            // LINEARRING and CIRCLE are not supported as inputs to fields so just return points
            case POINT, LINEARRING, CIRCLE -> {
                var point = GeometryTestUtils.randomPoint(false);
                yield value(point, isGeoJson);
            }
            case MULTIPOINT -> {
                var multiPoint = GeometryTestUtils.randomMultiPoint(false);
                yield value(multiPoint, isGeoJson);
            }
            case LINESTRING -> {
                var line = GeometryTestUtils.randomLine(false);
                yield value(line, isGeoJson);
            }
            case MULTILINESTRING -> {
                var multiPoint = GeometryTestUtils.randomMultiLine(false);
                yield value(multiPoint, isGeoJson);
            }
            case POLYGON -> {
                var polygon = GeometryTestUtils.randomPolygon(false);
                yield value(polygon, isGeoJson);
            }
            case MULTIPOLYGON -> {
                var multiPolygon = GeometryTestUtils.randomMultiPolygon(false);
                yield value(multiPolygon, isGeoJson);
            }
            case GEOMETRYCOLLECTION -> {
                var multiPolygon = GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
                yield value(multiPolygon, isGeoJson);
            }
            case ENVELOPE -> {
                var rectangle = GeometryTestUtils.randomRectangle();
                var wktString = WellKnownText.toWKT(rectangle);

                yield new Value(wktString, wktString);
            }
        };
    }

    private static Value value(Geometry geometry, boolean isGeoJson) {
        var wktString = WellKnownText.toWKT(geometry);
        var normalizedWktString = GeometryNormalizer.needsNormalize(Orientation.RIGHT, geometry)
            ? WellKnownText.toWKT(GeometryNormalizer.apply(Orientation.RIGHT, geometry))
            : wktString;

        if (isGeoJson) {
            var map = GeoJson.toMap(geometry);
            return new Value(map, map, normalizedWktString);
        }

        return new Value(wktString, wktString, normalizedWktString);
    }

    private void mapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_shape");
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
}
