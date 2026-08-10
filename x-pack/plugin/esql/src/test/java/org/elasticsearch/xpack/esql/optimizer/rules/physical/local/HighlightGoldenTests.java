/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;

import java.util.EnumSet;

/**
 * Golden tests for the HIGHLIGHT command, asserting the logical and local physical plan shape,
 * including the generated {@code highlight_<field>} output column.
 */
public class HighlightGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public HighlightGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

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
        builder(query).stages(STAGES).since(Highlight.ESQL_HIGHLIGHT).run();
    }

    public void testMatchOperatorWhereIsPushedBelowHighlight() {
        assumeTrue("requires HIGHLIGHT_V6 capability", EsqlCapabilities.Cap.HIGHLIGHT_V6.isEnabled());
        String query = """
            FROM employees
            | HIGHLIGHT "elasticsearch" ON first_name
            | WHERE first_name : "elasticsearch"
            """;
        builder(query).stages(STAGES).since(Highlight.ESQL_HIGHLIGHT).run();
    }

    /**
     * The logical optimizer moves the SORT and LIMIT below HIGHLIGHT, combining them into a TopN that HIGHLIGHT now sits above.
     * The local physical plan then pushes that TopN into the source, so highlighting runs on the surviving rows.
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
        builder(query).stages(STAGES).since(Highlight.ESQL_HIGHLIGHT).run();
    }

    /**
     * The TopN stays above HIGHLIGHT when it sorts on a generated highlight column, since that sort depends on the highlight output.
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
        builder(query).stages(STAGES).since(Highlight.ESQL_HIGHLIGHT).run();
    }
}
