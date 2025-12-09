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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
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

public class StSimplifyUnitTests extends ESTestCase {

    protected Expression build(Source source, List<Expression> args) {
        return new StSimplify(source, args.get(0), args.get(1));
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
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
    }

    protected final Page row(List<Object> values) {
        return maybeConvertBytesRefsToOrdinals(new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values)));
    }

    protected BytesRef process(String wkt, Object tolerance) {
        BytesRef wkb = UNSPECIFIED.wktToWkb(wkt);
        DataType toleranceType = DataType.fromJava(tolerance.getClass());
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                build(Source.EMPTY, List.of(new Literal(Source.EMPTY, wkb, GEO_POINT), new Literal(Source.EMPTY, tolerance, toleranceType)))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(wkb, tolerance)))
        ) {
            var result = ((BytesRefVectorBlock) block).asVector().getBytesRef(0, new BytesRef());
            return block.isNull(0) ? null : result;
        }
    }

    private final String polygonWkt = "POLYGON((0 0, 1 0.1, 2 0, 2 2, 1 1.9, 0 2, 0 0))";

    public void testInvalidTolerance() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process(polygonWkt, "invalid"));
        assertThat(ex.getMessage(), containsString("tolerance for st_simplify must be an integer or floating-point number"));
    }

    public void testINegativeTolerance() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process(polygonWkt, -1.0));
        assertThat(ex.getMessage(), containsString("tolerance must not be negative"));
    }

    // This should succeed
    public void testZeroTolerance() {
        process(polygonWkt, 0.0);
    }

    public void testValidTolerance() {
        // float
        process(polygonWkt, 1.0f);
        // double
        process(polygonWkt, 1.0d);
        // int
        process(polygonWkt, 1);
        // long
        process(polygonWkt, 1L);
    }

    public void testBowtie() {
        /* This is a bowtie

            *******
             *   *
              * *
               *
              * *
             *   *
            *******
        */
        var wkb = process("POLYGON((0 0, 2 2, 0 2, 2 0, 0 0))", 0.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        /* And it gets simplified to the lower triangle
               *
              * *
             *   *
            *     *
           *********
        */
        assertEquals("POLYGON ((0.0 0.0, 1.0 1.0, 2.0 0.0, 0.0 0.0))", result);
    }

    public void testTwoNonIntersectingPolygons() {
        /* Polygon with a hole outside the shell
                      * *
                      * *
             * * * *
             *     *
             *     *
             *     *
             * * * *
         */
        var wkb = process("POLYGON( (0 0, 4 0, 4 4, 0 4, 0 0), (5 5, 6 5, 6 6, 5 6, 5 5) )", 0.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        /* It gets simplified to the outer shell

             * * * *
             *     *
             *     *
             *     *
             * * * *
         */
        assertEquals("POLYGON ((0.0 0.0, 0.0 4.0, 4.0 4.0, 4.0 0.0, 0.0 0.0))", result);
    }

    public void testTwoTouchingPolygons() {
        /* Two polygons with a touching edge

             * * * *
             *     * *
             *     * *
             *     *
             * * * *

         */
        var wkb = process("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (4 2, 5 2, 5 3, 4 3, 4 2))", 0.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        /* It gets simplified to the biggest square

             * * * *
             *     *
             *     *
             *     *
             * * * *

         */
        assertEquals("POLYGON ((0.0 0.0, 0.0 4.0, 4.0 4.0, 4.0 3.0, 4.0 2.0, 4.0 0.0, 0.0 0.0))", result);
    }

    public void testLineWithRepeatedPoints() {
        var wkb = process("LINESTRING(0 0, 1 1, 1 1, 2 2)", 0.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertEquals("LINESTRING (0.0 0.0, 2.0 2.0)", result);
    }

    public void testEmptyPolygon() {
        var wkb = process("POLYGON EMPTY", 0.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        assertEquals("POLYGON EMPTY", result);
    }

    public void testBigTolerance() {
        var wkb = process("POLYGON( (0 0, 4 0, 4 4, 0 4, 0 0), (5 5, 6 5, 6 6, 5 6, 5 5) )", 10.0);
        var result = UNSPECIFIED.wkbToWkt(wkb);
        // It gets simplified to an empty polygon
        assertEquals("POLYGON EMPTY", result);
    }
}
