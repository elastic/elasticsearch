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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils.TestSearchStats;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression.PushedBlockLoaderExpression;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link StSimplify#tryPushToFieldLoading}, which fuses the simplification into loading shape doc values
 * that can reconstruct the original geometry.
 */
public class StSimplifyPushdownTests extends ESTestCase {

    public void testPushesShapeFieldWithConstantTolerance() {
        DataType type = randomFrom(DataType.GEO_SHAPE, DataType.CARTESIAN_SHAPE);
        FieldAttribute field = field(type);
        double tolerance = randomDoubleBetween(0, 10, true);
        PushedBlockLoaderExpression pushed = simplify(field, new Literal(Source.EMPTY, tolerance, DataType.DOUBLE)).tryPushToFieldLoading(
            stats(true, true)
        );
        assertNotNull(pushed);
        assertSame(field, pushed.field());
        assertEquals(
            new BlockLoaderFunctionConfig.GeometryTransform(BlockLoaderFunctionConfig.Function.ST_SIMPLIFY, tolerance, w -> g -> null),
            pushed.config()
        );
    }

    public void testPushesIntegerTolerance() {
        PushedBlockLoaderExpression pushed = simplify(field(DataType.GEO_SHAPE), new Literal(Source.EMPTY, 3, DataType.INTEGER))
            .tryPushToFieldLoading(stats(true, true));
        assertNotNull(pushed);
        assertEquals(3.0, ((BlockLoaderFunctionConfig.GeometryTransform) pushed.config()).parameter(), 0);
    }

    /** The fused transformer must give exactly what the function gives on the WKB of the same geometry. */
    public void testTransformerMatchesFunction() {
        DataType type = randomFrom(DataType.GEO_SHAPE, DataType.CARTESIAN_SHAPE);
        double tolerance = randomFrom(0.0, randomDoubleBetween(0, 5, true));
        var config = (BlockLoaderFunctionConfig.GeometryTransform) simplify(
            field(type),
            new Literal(Source.EMPTY, tolerance, DataType.DOUBLE)
        ).tryPushToFieldLoading(stats(true, true)).config();
        BlockLoaderFunctionConfig.GeometryTransformer transformer = config.transformers().create(null);
        for (int i = 0; i < 50; i++) {
            Geometry geometry = type == DataType.GEO_SHAPE
                ? randomFrom(GeometryTestUtils.randomLine(false), GeometryTestUtils.randomPolygon(false), GeometryTestUtils.randomPoint())
                : randomFrom(ShapeTestUtils.randomLine(false), ShapeTestUtils.randomPolygon(false), ShapeTestUtils.randomPoint());
            BytesRef wkb = new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
            Object expected = simplify(new Literal(Source.EMPTY, wkb, type), new Literal(Source.EMPTY, tolerance, DataType.DOUBLE)).fold(
                FoldContext.small()
            );
            assertEquals(geometry.toString(), expected, transformer.transform(geometry));
        }
    }

    public void testTransformerWarnsOnUnsupportedGeometry() {
        var config = (BlockLoaderFunctionConfig.GeometryTransform) simplify(
            field(DataType.GEO_SHAPE),
            new Literal(Source.EMPTY, 1.0, DataType.DOUBLE)
        ).tryPushToFieldLoading(stats(true, true)).config();
        List<String> warnings = new ArrayList<>();
        BlockLoaderFunctionConfig.GeometryTransformer transformer = config.transformers()
            .create((exceptionClass, message) -> warnings.add(exceptionClass.getSimpleName() + ": " + message));
        assertNull(transformer.transform(GeometryTestUtils.randomCircle(false)));
        assertThat(warnings, contains(containsString("IllegalArgumentException")));
    }

    public void testDoesNotPushWithoutReconstructableDocValues() {
        Expression tolerance = new Literal(Source.EMPTY, 1.0, DataType.DOUBLE);
        assertNull(simplify(field(DataType.GEO_SHAPE), tolerance).tryPushToFieldLoading(stats(false, true)));
        assertNull(simplify(field(DataType.GEO_SHAPE), tolerance).tryPushToFieldLoading(stats(true, false)));
        assertNull(simplify(field(DataType.GEO_SHAPE), tolerance).tryPushToFieldLoading(SearchStats.EMPTY));
    }

    public void testDoesNotPushPoints() {
        DataType type = randomFrom(DataType.GEO_POINT, DataType.CARTESIAN_POINT);
        assertNull(simplify(field(type), new Literal(Source.EMPTY, 1.0, DataType.DOUBLE)).tryPushToFieldLoading(stats(true, true)));
    }

    public void testDoesNotPushNonField() {
        Expression geometry = new ReferenceAttribute(Source.EMPTY, "shape", DataType.GEO_SHAPE);
        assertNull(simplify(geometry, new Literal(Source.EMPTY, 1.0, DataType.DOUBLE)).tryPushToFieldLoading(stats(true, true)));
    }

    /** Tolerances that are not a non-negative constant are left to the evaluator, which reports them. */
    public void testDoesNotPushInvalidOrVariableTolerance() {
        FieldAttribute field = field(DataType.GEO_SHAPE);
        SearchStats stats = stats(true, true);
        assertNull(simplify(field, new Literal(Source.EMPTY, -1.0, DataType.DOUBLE)).tryPushToFieldLoading(stats));
        assertNull(simplify(field, new Literal(Source.EMPTY, null, DataType.DOUBLE)).tryPushToFieldLoading(stats));
        assertNull(simplify(field, new ReferenceAttribute(Source.EMPTY, "t", DataType.DOUBLE)).tryPushToFieldLoading(stats));
    }

    private static StSimplify simplify(Expression geometry, Expression tolerance) {
        return new StSimplify(Source.EMPTY, geometry, tolerance);
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
