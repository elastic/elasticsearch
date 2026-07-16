/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefVectorBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.evaluator;
import static org.hamcrest.Matchers.containsString;

public class StBufferUnitTests extends ESTestCase {

    protected Expression build(Source source, List<Expression> args) {
        return new StBuffer(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    private Page maybeConvertBytesRefsToOrdinals(Page page) {
        boolean anyBytesRef = false;
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b).elementType() == ElementType.BYTES_REF) {
                anyBytesRef = true;
                break;
            }
        }
        return anyBytesRef && randomBoolean() ? BlockTestUtils.convertBytesRefsToOrdinals(page) : page;
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    protected final DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    protected final Page row(List<Object> values) {
        return maybeConvertBytesRefsToOrdinals(new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values)));
    }

    protected BytesRef process(String wkt, Object distance) {
        return process(wkt, distance, null);
    }

    protected BytesRef process(String wkt, Object distance, MapExpression options) {
        BytesRef wkb = UNSPECIFIED.wktToWkb(wkt);
        DataType distanceType = DataType.fromJavaType(distance.getClass());
        List<Expression> args = options == null
            ? List.of(new Literal(Source.EMPTY, wkb, GEO_POINT), new Literal(Source.EMPTY, distance, distanceType))
            : List.of(new Literal(Source.EMPTY, wkb, GEO_POINT), new Literal(Source.EMPTY, distance, distanceType), options);
        try (
            ExpressionEvaluator eval = evaluator(build(Source.EMPTY, args)).get(driverContext());
            Block block = eval.eval(row(List.of(wkb, distance)))
        ) {
            var result = ((BytesRefVectorBlock) block).asVector().getBytesRef(0, new BytesRef());
            return block.isNull(0) ? null : result;
        }
    }

    private static MapExpression mapOf(Object... keyValues) {
        List<Expression> entries = new ArrayList<>(keyValues.length);
        for (int i = 0; i < keyValues.length; i += 2) {
            Object key = keyValues[i];
            Object value = keyValues[i + 1];
            entries.add(new Literal(Source.EMPTY, key instanceof String s ? new BytesRef(s) : key, DataType.fromJavaType(key.getClass())));
            entries.add(
                new Literal(Source.EMPTY, value instanceof String s ? new BytesRef(s) : value, DataType.fromJavaType(value.getClass()))
            );
        }
        return new MapExpression(Source.EMPTY, entries);
    }

    public void testInvalidDistance() {
        // Non-numeric distance values fail at fold time with a descriptive error.
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> process("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", new BytesRef("invalid"))
        );
        assertThat(ex.getMessage(), containsString("distance for st_buffer must be an integer or floating-point number"));
    }

    public void testZeroDistancePolygon() {
        var wkb = process("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", 0.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertNotNull(result);
        assertTrue(result.startsWith("POLYGON"));
    }

    public void testZeroDistancePoint() {
        // Zero distance returns the original geometry unchanged
        var wkb = process("POINT(0 0)", 0.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertEquals("POINT (0.0 0.0)", result);
    }

    public void testZeroDistanceLine() {
        // Zero distance returns the original geometry unchanged
        var wkb = process("LINESTRING(0 0, 1 1)", 0.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertEquals("LINESTRING (0.0 0.0, 1.0 1.0)", result);
    }

    public void testPositiveDistance() {
        // Buffering a point with positive distance creates a polygon
        var wkb = process("POINT(0 0)", 1.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertNotNull(result);
        assertTrue(result.startsWith("POLYGON"));
    }

    public void testNegativeDistance() {
        // Negative buffer shrinks a polygon
        var wkb = process("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", -1.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertNotNull(result);
        assertTrue(result.startsWith("POLYGON"));
    }

    public void testValidDistanceTypes() {
        process("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", 1.0f);
        process("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", 1.0d);
        process("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", 1);
        process("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", 1L);
    }

    public void testEmptyPolygon() {
        var wkb = process("POLYGON EMPTY", 1.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        // Buffering an empty polygon results in an empty geometry
        assertEquals("POLYGON EMPTY", result);
    }

    public void testLineBuffer() {
        // Buffering a line creates a polygon
        var wkb = process("LINESTRING(0 0, 10 0)", 1.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertNotNull(result);
        assertTrue(result.startsWith("POLYGON"));
    }

    public void testFlatEndCap() {
        // Flat cap on a horizontal line buffered by 2 produces a 5-point rectangle
        var wkb = process("LINESTRING(0 0, 10 0)", 2.0, mapOf("endcap", "flat"));
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertEquals("POLYGON ((10.0 2.0, 10.0 -2.0, 0.0 -2.0, 0.0 2.0, 10.0 2.0))", result);
    }

    public void testSquareEndCap() {
        // Square cap extends the buffer beyond the line endpoints by the buffer distance
        var wkb = process("LINESTRING(0 0, 10 0)", 2.0, mapOf("endcap", "square"));
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertEquals("POLYGON ((10.0 2.0, 12.0 2.0, 12.0 -2.0, 0.0 -2.0, -2.0 -2.0, -2.0 2.0, 10.0 2.0))", result);
    }

    public void testQuadSegs() {
        // Quad segments controls the resolution of round corners; 1 segment per quadrant gives a diamond
        var wkb = process("POINT(0 0)", 1.0, mapOf("quad_segs", 1));
        var result = UNSPECIFIED.wkbToWkt(wkb);
        // 4 corners + closing point = 5 points in WKT
        assertEquals("POLYGON ((1.0 0.0, 0.0 -1.0, -1.0 0.0, 0.0 1.0, 1.0 0.0))", result);
    }

    public void testMitreJoin() {
        // Mitre join produces sharp corners; 2 quad segs but join=mitre keeps the L-shape sharp
        var wkb = process("LINESTRING(0 0, 10 0, 10 10)", 2.0, mapOf("endcap", "flat", "join", "mitre", "quad_segs", 2));
        var result = UNSPECIFIED.wkbToWkt(wkb);
        // 7 vertices: sharp mitre corner + flat caps on both ends
        assertTrue(result.startsWith("POLYGON"));
    }

    public void testInvalidEndCap() {
        InvalidArgumentException ex = expectThrows(
            InvalidArgumentException.class,
            () -> process("POINT(0 0)", 1.0, mapOf("endcap", "diamond"))
        );
        assertThat(ex.getMessage(), containsString("Invalid value [diamond] for option [endcap]"));
    }

    public void testInvalidJoin() {
        InvalidArgumentException ex = expectThrows(
            InvalidArgumentException.class,
            () -> process("LINESTRING(0 0, 10 0)", 1.0, mapOf("join", "invalid"))
        );
        assertThat(ex.getMessage(), containsString("Invalid value [invalid] for option [join]"));
    }

    public void testEndCapButtAlias() {
        // "butt" is an accepted synonym for "flat"; both must produce the same polygon
        var flat = process("LINESTRING(0 0, 10 0)", 2.0, mapOf("endcap", "flat"));
        var butt = process("LINESTRING(0 0, 10 0)", 2.0, mapOf("endcap", "butt"));
        assertEquals(UNSPECIFIED.wkbToWkt(flat), UNSPECIFIED.wkbToWkt(butt));
    }

    public void testJoinMiterAlias() {
        // "miter" is an accepted synonym for "mitre"; both must produce the same polygon
        var mitre = process("LINESTRING(0 0, 10 0, 10 10)", 2.0, mapOf("endcap", "flat", "join", "mitre"));
        var miter = process("LINESTRING(0 0, 10 0, 10 10)", 2.0, mapOf("endcap", "flat", "join", "miter"));
        assertEquals(UNSPECIFIED.wkbToWkt(mitre), UNSPECIFIED.wkbToWkt(miter));
    }

    public void testEndCapCaseInsensitive() {
        // Endcap values are matched case-insensitively
        var lower = process("LINESTRING(0 0, 10 0)", 2.0, mapOf("endcap", "flat"));
        var upper = process("LINESTRING(0 0, 10 0)", 2.0, mapOf("endcap", "FLAT"));
        assertEquals(UNSPECIFIED.wkbToWkt(lower), UNSPECIFIED.wkbToWkt(upper));
    }

    public void testJoinCaseInsensitive() {
        // Join values are matched case-insensitively
        var lower = process("LINESTRING(0 0, 10 0, 10 10)", 2.0, mapOf("join", "mitre"));
        var upper = process("LINESTRING(0 0, 10 0, 10 10)", 2.0, mapOf("join", "MITRE"));
        assertEquals(UNSPECIFIED.wkbToWkt(lower), UNSPECIFIED.wkbToWkt(upper));
    }
}
