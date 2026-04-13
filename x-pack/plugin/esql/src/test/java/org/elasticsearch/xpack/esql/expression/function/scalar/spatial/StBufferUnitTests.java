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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
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
        return new StBuffer(source, args.get(0), args.get(1));
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
        BytesRef wkb = UNSPECIFIED.wktToWkb(wkt);
        DataType distanceType = DataType.fromJava(distance.getClass());
        try (
            ExpressionEvaluator eval = evaluator(
                build(Source.EMPTY, List.of(new Literal(Source.EMPTY, wkb, GEO_POINT), new Literal(Source.EMPTY, distance, distanceType)))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(wkb, distance)))
        ) {
            var result = ((BytesRefVectorBlock) block).asVector().getBytesRef(0, new BytesRef());
            return block.isNull(0) ? null : result;
        }
    }

    public void testInvalidDistance() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> process("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", "invalid")
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
}
