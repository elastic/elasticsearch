/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class AddTopNPreFilterToAggregateGoldenTests extends GoldenTestCase {
    private static final String TOPN_PREFILTER_LONG = "topn_prefilter_long";
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AddTopNPreFilterToAggregateGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testLimitOnly() {
        builder("""
            FROM all_types
            | STATS count(*) BY long, keyword
            | LIMIT 10
            """).stages(STAGES).since(TOPN_PREFILTER_LONG).run();
    }

    public void testSortByLongDesc() {
        builder("""
            FROM all_types
            | STATS count(*) BY keyword, long
            | SORT long DESC
            | LIMIT 10
            """).stages(STAGES).since(TOPN_PREFILTER_LONG).run();
    }

    public void testSortByAliasedLong() {
        builder("""
            FROM all_types
            | STATS c = count(*) BY k = long, keyword
            | SORT k NULLS FIRST, keyword
            | LIMIT 10
            """).stages(STAGES).since(TOPN_PREFILTER_LONG).run();
    }

    public void testSortedByNotLongFirst() {
        runGoldenTest("""
            FROM all_types
            | STATS count(*) BY long, keyword
            | SORT keyword, long
            | LIMIT 10
            """, STAGES);
    }
}
