/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.rules.ResolveHighlightIndexKey;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.logicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.soleHighlight;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.booksWithConflictingTitleAnalyzer;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * {@link PushDownAndCombineOrderBy} moves an EVAL below HIGHLIGHT so the sort can follow it. An EVAL that reads a generated
 * column, or defines a name HIGHLIGHT reads or writes, changes what is highlighted once moved, so the sort stays above.
 */
public class PushDownAndCombineOrderByTests extends ESTestCase {

    public void testSortPushedPastIndependentEval() {
        assertPushed(employees("EVAL negated = -emp_no | SORT negated"));
    }

    public void testEvalReadingGeneratedColumnKeepsSortAbove() {
        assertNotPushed(employees("EVAL len = LENGTH(highlight_first_name) | SORT emp_no"));
    }

    public void testEvalShadowingGeneratedColumnKeepsSortAbove() {
        assertNotPushed(employees("EVAL highlight_first_name = \"x\" | SORT emp_no"));
    }

    public void testEvalShadowingOnFieldKeepsSortAbove() {
        assertNotPushed(employees("EVAL first_name = \"x\" | SORT emp_no"));
    }

    public void testTopNPushedPastEvalWithIndexKey() {
        assertThat(soleHighlight(optimizedBooks("EVAL n = 1")).collect(TopN.class), hasSize(1));
    }

    public void testEvalShadowingIndexKeyKeepsTopNAbove() {
        assertThat(
            soleHighlight(optimizedBooks("EVAL `" + ResolveHighlightIndexKey.INDEX_KEY_NAME + "` = \"x\"")).collect(TopN.class),
            empty()
        );
    }

    /**
     * Runs the rule alone on the analyzed plan: in a full optimization, column pruning drops a HIGHLIGHT whose output is
     * shadowed.
     */
    private static LogicalPlan employees(String tail) {
        return EsqlTestUtils.analyzer()
            .addEmployees("test")
            .minimumTransportVersion(TransportVersion.current())
            .query("FROM test | HIGHLIGHT \"georgi\" ON first_name | " + tail + " | LIMIT 10");
    }

    /**
     * The indices disagree on {@code title}'s analyzer, so HIGHLIGHT reads each row's index key. The analyzer drops the key in
     * a projection right above HIGHLIGHT, and only the full optimizer brings the EVAL next to it.
     */
    private static LogicalPlan optimizedBooks(String eval) {
        LogicalPlan plan = booksWithConflictingTitleAnalyzer().query(
            "FROM books* | HIGHLIGHT \"ring\" ON title | " + eval + " | SORT book_no | LIMIT 10"
        );
        assertNotNull(soleHighlight(plan).indexKey());
        return new LogicalPlanOptimizer(logicalOptimizerContext(TEST_CFG, FoldContext.small(), TextEsField.TEXT_FIELD_ANALYZER)).optimize(
            plan
        );
    }

    private static void assertPushed(LogicalPlan plan) {
        as(soleHighlight(pushDown(plan)).child(), OrderBy.class);
    }

    private static void assertNotPushed(LogicalPlan plan) {
        assertThat(pushDown(plan), equalTo(plan));
    }

    private static LogicalPlan pushDown(LogicalPlan plan) {
        return new PushDownAndCombineOrderBy().apply(plan);
    }
}
