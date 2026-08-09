/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/**
 * Tests the structural invariants of the {@link SqlPredicate} sealed tree. Renderer behavior lives in
 * {@link SqlRendererTests}; this class focuses on the contracts the records' compact constructors must enforce so
 * downstream code (renderer, optimizer rewrites) can rely on them.
 */
public class SqlPredicateTests extends ESTestCase {

    public void testComparisonRejectsNullColumn() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.Comparison(null, CompOp.EQ, new SqlParam(1, DataType.INTEGER)));
    }

    public void testComparisonRejectsEmptyColumn() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.Comparison("", CompOp.EQ, new SqlParam(1, DataType.INTEGER)));
    }

    public void testComparisonRejectsNullOp() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.Comparison("c", null, new SqlParam(1, DataType.INTEGER)));
    }

    public void testComparisonRejectsNullParam() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.Comparison("c", CompOp.EQ, null));
    }

    public void testInListRejectsEmpty() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.InList("c", List.of()));
    }

    public void testInListIsDefensivelyCopied() {
        var src = new java.util.ArrayList<SqlParam>();
        src.add(new SqlParam(1, DataType.INTEGER));
        src.add(new SqlParam(2, DataType.INTEGER));
        SqlPredicate.InList in = new SqlPredicate.InList("c", src);
        // Mutating the source list must not affect the record's snapshot.
        src.add(new SqlParam(3, DataType.INTEGER));
        assertEquals(2, in.values().size());
    }

    public void testAndRequiresAtLeastTwoParts() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.And(List.of()));
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.And(List.of(new SqlPredicate.IsNull("c"))));
    }

    public void testOrRequiresAtLeastTwoParts() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.Or(List.of()));
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.Or(List.of(new SqlPredicate.IsNull("c"))));
    }

    public void testNotRejectsNull() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.Not(null));
    }

    public void testRangeRejectsNullBounds() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new SqlPredicate.Range("c", null, true, new SqlParam(1, DataType.INTEGER), true)
        );
    }

    public void testLikeRejectsNullPattern() {
        expectThrows(IllegalArgumentException.class, () -> new SqlPredicate.Like("c", null));
    }

    public void testCompOpCommute() {
        assertEquals(CompOp.EQ, CompOp.EQ.commute());
        assertEquals(CompOp.NEQ, CompOp.NEQ.commute());
        assertEquals(CompOp.GT, CompOp.LT.commute());
        assertEquals(CompOp.GTE, CompOp.LTE.commute());
        assertEquals(CompOp.LT, CompOp.GT.commute());
        assertEquals(CompOp.LTE, CompOp.GTE.commute());
    }

    public void testCompOpSymbols() {
        assertEquals("=", CompOp.EQ.symbol());
        assertEquals("<>", CompOp.NEQ.symbol());
        assertEquals("<", CompOp.LT.symbol());
        assertEquals("<=", CompOp.LTE.symbol());
        assertEquals(">", CompOp.GT.symbol());
        assertEquals(">=", CompOp.GTE.symbol());
    }

    public void testSqlParamRejectsNullType() {
        expectThrows(IllegalArgumentException.class, () -> new SqlParam("x", null));
    }
}
