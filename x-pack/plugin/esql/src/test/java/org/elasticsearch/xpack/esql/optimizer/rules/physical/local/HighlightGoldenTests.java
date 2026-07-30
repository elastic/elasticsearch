/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

/**
 * Golden tests for the HIGHLIGHT command, asserting the logical and local physical plan shape,
 * including the generated {@code highlight_<field>} output column.
 */
public class HighlightGoldenTests extends GoldenTestCase {

    /**
     * HIGHLIGHT survives logical and local physical optimization, producing a {@code HighlightExec}
     * whose generated {@code highlight_<field>} column is appended to the output layout.
     */
    public void testBasicHighlight() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | HIGHLIGHT "elasticsearch" ON first_name
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }

    public void testMatchOperatorWhereIsPushedBelowHighlight() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | HIGHLIGHT "elasticsearch" ON first_name
            | WHERE first_name : "elasticsearch"
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }

    /**
     * TopN moves below HIGHLIGHT and is pushed into the source.
     */
    public void testTopNIsPushedBelowHighlight() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | WHERE first_name : "elasticsearch"
            | HIGHLIGHT "elasticsearch" ON first_name
            | SORT emp_no DESC
            | LIMIT 10
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }

    /**
     * HIGHLIGHT moves past the Eval and TopN created for a sort expression. The TopN remains local because Lucene cannot sort
     * on the expression.
     */
    public void testHighlightIsHoistedPastEvalAndTopN() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | WHERE first_name : "elasticsearch"
            | HIGHLIGHT "elasticsearch" ON first_name
            | SORT LENGTH(last_name) DESC
            | LIMIT 10
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }

    /**
     * TopN stays above HIGHLIGHT when it sorts on a generated highlight column.
     */
    public void testTopNOnGeneratedSnippetIsNotPushedBelowHighlight() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | WHERE first_name : "elasticsearch"
            | HIGHLIGHT "elasticsearch" ON first_name
            | SORT highlight_first_name ASC
            | LIMIT 10
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }

    /**
     * HIGHLIGHT moves past an Eval whose output name does not conflict with its input or output.
     */
    public void testHighlightIsHoistedPastEvalShadowingUnrelatedColumn() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | WHERE first_name : "elasticsearch"
            | HIGHLIGHT "elasticsearch" ON first_name
            | EVAL gender = CONCAT(last_name, "x")
            | SORT gender ASC
            | LIMIT 10
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }

    /**
     * HIGHLIGHT stays below an Eval that replaces the generated column.
     */
    public void testHighlightIsNotHoistedPastEvalShadowingGeneratedColumn() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | WHERE first_name : "elasticsearch"
            | HIGHLIGHT "elasticsearch" ON first_name
            | EVAL highlight_first_name = CONCAT(last_name, "x")
            | SORT highlight_first_name ASC
            | LIMIT 10
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }

    /**
     * HIGHLIGHT stays below an Eval that replaces its ON field.
     */
    public void testHighlightIsNotHoistedPastEvalShadowingOnField() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | WHERE first_name : "elasticsearch"
            | HIGHLIGHT "elasticsearch" ON first_name
            | EVAL first_name = CONCAT(last_name, "x")
            | SORT first_name ASC
            | LIMIT 10
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }
}
