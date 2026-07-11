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
 * Renderer-level unit tests: given a hand-built {@link SqlPredicate} tree, the produced SQL fragment and parameter
 * list must match expectation, with identifiers quoted via {@link JdbcDialect#quoteIdentifier(String)} and every
 * literal carried as a {@code ?} placeholder.
 * <p>
 * These tests deliberately do NOT exercise translation (covered separately in {@code JdbcFilterPushdownSupportTests});
 * they prove the renderer is dialect-agnostic and parameter-safe in isolation.
 */
public class SqlRendererTests extends ESTestCase {

    private final SqlRenderer renderer = new SqlRenderer(GenericDialect.INSTANCE);

    public void testRenderComparison() {
        SqlPredicate p = new SqlPredicate.Comparison("age", CompOp.GT, new SqlParam(18, DataType.INTEGER));
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("\"age\" > ?", r.sql());
        assertEquals(1, r.params().size());
        assertEquals(18, r.params().get(0).value());
        assertEquals(DataType.INTEGER, r.params().get(0).esqlType());
    }

    public void testRenderIsNullAndIsNotNull() {
        assertEquals("\"x\" IS NULL", renderer.render(new SqlPredicate.IsNull("x")).sql());
        assertEquals("\"x\" IS NOT NULL", renderer.render(new SqlPredicate.IsNotNull("x")).sql());
    }

    public void testRenderInList() {
        SqlPredicate p = new SqlPredicate.InList(
            "id",
            List.of(new SqlParam(1, DataType.INTEGER), new SqlParam(2, DataType.INTEGER), new SqlParam(3, DataType.INTEGER))
        );
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("\"id\" IN (?, ?, ?)", r.sql());
        assertEquals(3, r.params().size());
    }

    public void testRenderLikeWithEscape() {
        SqlPredicate p = new SqlPredicate.Like("name", "foo%bar");
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("\"name\" LIKE ? ESCAPE '\\'", r.sql());
        assertEquals(1, r.params().size());
        assertEquals("foo%bar", r.params().get(0).value());
    }

    public void testRenderRangeInclusiveUsesBetween() {
        SqlPredicate p = new SqlPredicate.Range("ts", new SqlParam(10L, DataType.LONG), true, new SqlParam(20L, DataType.LONG), true);
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("\"ts\" BETWEEN ? AND ?", r.sql());
        assertEquals(2, r.params().size());
    }

    public void testRenderRangeMixedInclusivityUsesPairOfComparisons() {
        SqlPredicate p = new SqlPredicate.Range("ts", new SqlParam(10L, DataType.LONG), true, new SqlParam(20L, DataType.LONG), false);
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("(\"ts\" >= ? AND \"ts\" < ?)", r.sql());
    }

    public void testRenderAndIsAggressivelyParenthesized() {
        SqlPredicate p = new SqlPredicate.And(
            List.of(new SqlPredicate.IsNotNull("a"), new SqlPredicate.Comparison("b", CompOp.EQ, new SqlParam(1, DataType.INTEGER)))
        );
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("(\"a\" IS NOT NULL AND \"b\" = ?)", r.sql());
    }

    public void testRenderOrIsAggressivelyParenthesized() {
        SqlPredicate p = new SqlPredicate.Or(
            List.of(
                new SqlPredicate.Comparison("a", CompOp.EQ, new SqlParam(1, DataType.INTEGER)),
                new SqlPredicate.Comparison("a", CompOp.EQ, new SqlParam(2, DataType.INTEGER))
            )
        );
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("(\"a\" = ? OR \"a\" = ?)", r.sql());
    }

    public void testRenderNotPrefixedAndParenthesized() {
        SqlPredicate p = new SqlPredicate.Not(new SqlPredicate.IsNull("c"));
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("NOT (\"c\" IS NULL)", r.sql());
    }

    public void testRenderNestedTree() {
        // (a IS NOT NULL AND (b = ? OR c > ?))
        SqlPredicate inner = new SqlPredicate.Or(
            List.of(
                new SqlPredicate.Comparison("b", CompOp.EQ, new SqlParam(1, DataType.INTEGER)),
                new SqlPredicate.Comparison("c", CompOp.GT, new SqlParam(2, DataType.INTEGER))
            )
        );
        SqlPredicate p = new SqlPredicate.And(List.of(new SqlPredicate.IsNotNull("a"), inner));
        SqlRenderer.Rendered r = renderer.render(p);
        assertEquals("(\"a\" IS NOT NULL AND (\"b\" = ? OR \"c\" > ?))", r.sql());
        assertEquals(2, r.params().size());
    }

    public void testRendererQuotesIdentifierWithSpecialChars() {
        // Identifier with a space -- still safe under GenericDialect quoting (double-quoting any embedded ").
        SqlPredicate p = new SqlPredicate.IsNotNull("my col");
        assertEquals("\"my col\" IS NOT NULL", renderer.render(p).sql());
    }

    public void testRendererRejectsNullPredicate() {
        expectThrows(IllegalArgumentException.class, () -> renderer.render(null));
    }
}
