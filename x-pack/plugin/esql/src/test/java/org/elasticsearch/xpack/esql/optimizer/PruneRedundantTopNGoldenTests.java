/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneRedundantTopN;

import java.util.EnumSet;

/**
 * Golden tests for {@link PruneRedundantTopN}.
 */
public class PruneRedundantTopNGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public PruneRedundantTopNGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    /**
     * Two consecutive TopNs with the same sort order are merged into one.
     */
    public void testPruneRedundantTopN() {
        runGoldenTest("""
            FROM employees
            | KEEP first_name
            | SORT first_name
            | LIMIT 100
            | SORT first_name
            | LIMIT 10
            """, STAGES);
    }

    /**
     * Only consecutive TopNs are merged; here an {@code EVAL} sits between them, so both are kept.
     */
    public void testDoNotPruneRedundantTopNWithEvalInBetween() {
        runGoldenTest("""
            FROM employees
            | SORT first_name
            | LIMIT 10
            | DROP last_name
            | KEEP first_name
            | EVAL a = 12
            | SORT first_name
            | LIMIT 100
            """, STAGES);
    }

    /**
     * An aggregation between the two TopNs changes the semantics of the upstream TopN, so both are kept.
     */
    public void testDoNotPruneRedundantTopNWithAggregateInBetween() {
        runGoldenTest("""
            FROM employees
            | SORT first_name
            | LIMIT 10
            | STATS c = COUNT(*) BY first_name
            | SORT first_name
            | LIMIT 100
            """, STAGES);
    }
}
