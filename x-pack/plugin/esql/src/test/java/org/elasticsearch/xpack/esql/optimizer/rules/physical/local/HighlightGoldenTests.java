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
        assumeTrue("requires HIGHLIGHT_V3 capability", EsqlCapabilities.Cap.HIGHLIGHT_V3.isEnabled());
        String query = """
            FROM employees
            | HIGHLIGHT "elasticsearch" ON first_name
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }
}
