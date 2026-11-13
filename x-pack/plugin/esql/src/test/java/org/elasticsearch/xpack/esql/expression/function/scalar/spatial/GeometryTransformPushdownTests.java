/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig.GeometryTransform;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils.TestSearchStats;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression.PushedBlockLoaderExpression;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for the {@code tryPushToFieldLoading} of {@link StSimplify}, {@link StSimplifyPreserveTopology} and
 * {@link StBuffer}, which fuse the function into loading shape doc values that can reconstruct the original geometry.
 */
public class GeometryTransformPushdownTests extends ESTestCase {

    private enum Transform {
        SIMPLIFY(BlockLoaderFunctionConfig.Function.ST_SIMPLIFY, (g, p) -> new StSimplify(Source.EMPTY, g, p)),
        SIMPLIFY_PRESERVE_TOPOLOGY(
            BlockLoaderFunctionConfig.Function.ST_SIMPLIFYPRESERVETOPOLOGY,
            (g, p) -> new StSimplifyPreserveTopology(Source.EMPTY, g, p)
        ),
        BUFFER(BlockLoaderFunctionConfig.Function.ST_BUFFER, (g, p) -> new StBuffer(Source.EMPTY, g, p, null));

        private final BlockLoaderFunctionConfig.Function function;
        private final BiFunction<Expression, Expression, SpatialDocValuesFunction> build;

        Transform(BlockLoaderFunctionConfig.Function function, BiFunction<Expression, Expression, SpatialDocValuesFunction> build) {
            this.function = function;
            this.build = build;
        }

        PushedBlockLoaderExpression push(Expression geometry, Expression parameter, SearchStats stats) {
            return ((BlockLoaderExpression) build.apply(geometry, parameter)).tryPushToFieldLoading(stats);
        }
    }

    public void testPushesShapeFieldWithConstantParameter() {
        for (Transform transform : Transform.values()) {
            FieldAttribute field = field(randomFrom(DataType.GEO_SHAPE, DataType.CARTESIAN_SHAPE));
            double parameter = randomDoubleBetween(0, 10, true);
            PushedBlockLoaderExpression pushed = transform.push(field, doubleLiteral(parameter), stats(true, true));
            assertNotNull(transform.name(), pushed);
            assertSame(field, pushed.field());
            GeometryTransform config = as(pushed.config());
            assertEquals(transform.function, config.function());
            assertEquals(parameter, config.parameter(), 0);
        }
    }

    public void testPushesIntegerParameter() {
        for (Transform transform : Transform.values()) {
            PushedBlockLoaderExpression pushed = transform.push(
                field(DataType.GEO_SHAPE),
                new Literal(Source.EMPTY, 3, DataType.INTEGER),
                stats(true, true)
            );
            assertEquals(transform.name(), 3.0, as(pushed.config()).parameter(), 0);
        }
    }

    /** The fused transformer must give exactly what the function gives on the WKB of the same geometry. */
    public void testTransformerMatchesFunction() {
        for (Transform transform : Transform.values()) {
            DataType type = randomFrom(DataType.GEO_SHAPE, DataType.CARTESIAN_SHAPE);
            double parameter = randomFrom(0.0, randomDoubleBetween(0, 5, true));
            GeometryTransform config = as(transform.push(field(type), doubleLiteral(parameter), stats(true, true)).config());
            BlockLoaderFunctionConfig.GeometryTransformer transformer = config.transformers().create(null);
            for (int i = 0; i < 20; i++) {
                Geometry geometry = type == DataType.GEO_SHAPE
                    ? randomFrom(
                        GeometryTestUtils.randomLine(false),
                        GeometryTestUtils.randomPolygon(false),
                        GeometryTestUtils.randomPoint()
                    )
                    : randomFrom(ShapeTestUtils.randomLine(false), ShapeTestUtils.randomPolygon(false), ShapeTestUtils.randomPoint());
                BytesRef wkb = new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
                Object expected = transform.build.apply(new Literal(Source.EMPTY, wkb, type), doubleLiteral(parameter))
                    .fold(FoldContext.small());
                assertEquals(transform.name() + " " + geometry, expected, transformer.transform(geometry));
            }
        }
    }

    public void testTransformerWarnsOnUnsupportedGeometry() {
        for (Transform transform : Transform.values()) {
            GeometryTransform config = as(transform.push(field(DataType.GEO_SHAPE), doubleLiteral(1.0), stats(true, true)).config());
            List<String> warnings = new ArrayList<>();
            BlockLoaderFunctionConfig.GeometryTransformer transformer = config.transformers()
                .create((exceptionClass, message) -> warnings.add(exceptionClass.getSimpleName() + ": " + message));
            assertNull(transformer.transform(GeometryTestUtils.randomCircle(false)));
            assertThat(transform.name(), warnings, contains(containsString("IllegalArgumentException")));
        }
    }

    public void testDoesNotPushWithoutReconstructableDocValues() {
        for (Transform transform : Transform.values()) {
            assertNull(transform.push(field(DataType.GEO_SHAPE), doubleLiteral(1.0), stats(false, true)));
            assertNull(transform.push(field(DataType.GEO_SHAPE), doubleLiteral(1.0), stats(true, false)));
            assertNull(transform.push(field(DataType.GEO_SHAPE), doubleLiteral(1.0), SearchStats.EMPTY));
        }
    }

    public void testDoesNotPushPoints() {
        for (Transform transform : Transform.values()) {
            DataType type = randomFrom(DataType.GEO_POINT, DataType.CARTESIAN_POINT);
            assertNull(transform.push(field(type), doubleLiteral(1.0), stats(true, true)));
        }
    }

    public void testDoesNotPushNonField() {
        for (Transform transform : Transform.values()) {
            Expression geometry = new ReferenceAttribute(Source.EMPTY, "shape", DataType.GEO_SHAPE);
            assertNull(transform.push(geometry, doubleLiteral(1.0), stats(true, true)));
        }
    }

    /** Parameters that are not constant numbers, or negative tolerances, are left to the evaluator, which reports them. */
    public void testDoesNotPushInvalidOrVariableParameter() {
        for (Transform transform : Transform.values()) {
            FieldAttribute field = field(DataType.GEO_SHAPE);
            SearchStats stats = stats(true, true);
            assertNull(transform.push(field, new Literal(Source.EMPTY, null, DataType.DOUBLE), stats));
            assertNull(transform.push(field, new ReferenceAttribute(Source.EMPTY, "t", DataType.DOUBLE), stats));
        }
        assertNull(Transform.SIMPLIFY.push(field(DataType.GEO_SHAPE), doubleLiteral(-1.0), stats(true, true)));
        assertNull(Transform.SIMPLIFY_PRESERVE_TOPOLOGY.push(field(DataType.GEO_SHAPE), doubleLiteral(-1.0), stats(true, true)));
    }

    /** Negative buffer distances shrink the geometry, so they are pushed like any other distance. */
    public void testPushesNegativeBufferDistance() {
        assertEquals(
            -2.0,
            as(Transform.BUFFER.push(field(DataType.GEO_SHAPE), doubleLiteral(-2.0), stats(true, true)).config()).parameter(),
            0
        );
    }

    /** Buffers with different options must not share a fused field, while the default options are the same as none. */
    public void testBufferOptionsArePartOfConfig() {
        FieldAttribute field = field(DataType.GEO_SHAPE);
        SearchStats stats = stats(true, true);
        GeometryTransform none = as(new StBuffer(Source.EMPTY, field, doubleLiteral(1.0), null).tryPushToFieldLoading(stats).config());
        GeometryTransform defaults = as(
            new StBuffer(Source.EMPTY, field, doubleLiteral(1.0), mapOf("quad_segs", 8, "endcap", "round")).tryPushToFieldLoading(stats)
                .config()
        );
        GeometryTransform flat = as(
            new StBuffer(Source.EMPTY, field, doubleLiteral(1.0), mapOf("endcap", "flat")).tryPushToFieldLoading(stats).config()
        );
        assertEquals(none, defaults);
        assertNotEquals(none, flat);
        assertNotEquals(none.hashCode(), flat.hashCode());

        Geometry line = GeometryTestUtils.randomLine(false);
        BytesRef wkb = new BytesRef(WellKnownBinary.toWKB(line, ByteOrder.LITTLE_ENDIAN));
        Object expected = new StBuffer(
            Source.EMPTY,
            new Literal(Source.EMPTY, wkb, DataType.GEO_SHAPE),
            doubleLiteral(1.0),
            mapOf("endcap", "flat")
        ).fold(FoldContext.small());
        assertEquals(expected, flat.transformers().create(null).transform(line));
    }

    public void testDoesNotPushInvalidBufferOptions() {
        StBuffer buffer = new StBuffer(Source.EMPTY, field(DataType.GEO_SHAPE), doubleLiteral(1.0), mapOf("endcap", "wavy"));
        assertNull(buffer.tryPushToFieldLoading(stats(true, true)));
    }

    private static GeometryTransform as(BlockLoaderFunctionConfig config) {
        assertTrue("expected a geometry transform but was " + config, config instanceof GeometryTransform);
        return (GeometryTransform) config;
    }

    private static Literal doubleLiteral(double value) {
        return new Literal(Source.EMPTY, value, DataType.DOUBLE);
    }

    private static MapExpression mapOf(Object... keyValues) {
        List<Expression> entries = new ArrayList<>(keyValues.length);
        for (int i = 0; i < keyValues.length; i += 2) {
            entries.add(new Literal(Source.EMPTY, new BytesRef((String) keyValues[i]), DataType.KEYWORD));
            Object value = keyValues[i + 1];
            entries.add(
                value instanceof String s
                    ? new Literal(Source.EMPTY, new BytesRef(s), DataType.KEYWORD)
                    : new Literal(Source.EMPTY, value, DataType.fromJavaType(value.getClass()))
            );
        }
        return new MapExpression(Source.EMPTY, entries);
    }

    private static FieldAttribute field(DataType type) {
        return new FieldAttribute(
            Source.EMPTY,
            "shape",
            new EsField("shape", type, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static SearchStats stats(boolean hasDocValues, boolean reconstructable) {
        return new TestSearchStats() {
            @Override
            public boolean hasDocValues(FieldName field) {
                return hasDocValues;
            }

            @Override
            public boolean supportsGeometryDocValueReconstruction(FieldName field) {
                return reconstructable;
            }
        };
    }
}
