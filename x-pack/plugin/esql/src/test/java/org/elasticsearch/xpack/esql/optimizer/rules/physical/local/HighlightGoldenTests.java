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
 * Golden tests for the HIGHLIGHT command. Part 1 only covers grammar + plan-node shape;
 * once an executor lands these tests should grow to cover the prefix-column output.
 */
public class HighlightGoldenTests extends GoldenTestCase {

    /**
     * Part 1 plan shape: HIGHLIGHT survives through logical optimization
     * and local physical optimization without rewriting or pushdown.
     */
    public void testBasicHighlight() {
        assumeTrue("requires HIGHLIGHT_V0 capability", EsqlCapabilities.Cap.HIGHLIGHT_V0.isEnabled());
        String query = """
            FROM employees
            | HIGHLIGHT "elasticsearch" ON first_name
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION));
    }
}
