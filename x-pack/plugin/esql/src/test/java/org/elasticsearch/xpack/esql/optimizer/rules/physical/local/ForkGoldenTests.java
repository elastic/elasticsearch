/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushExpressionToFieldLoadGoldenTests.SEARCH_STATS;

public class ForkGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public ForkGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    // TODO: Add NODE_REDUCE and NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION stages
    // We need to extend golden tests to support more than a single data node plan.
    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.ANALYSIS,
        Stage.LOGICAL_OPTIMIZATION,
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.LOCAL_PHYSICAL_OPTIMIZATION
    );

    public void testSimple() {
        runGoldenTest("""
            FROM employees
            | FORK ( WHERE emp_no == 1 )
                   ( WHERE emp_no == 2 )
            """, STAGES);
    }

    public void testHybridSearch() {
        // SUM(_score) planning changed at ESQL_SUM_LONG_OVERFLOW_FIX; the older shape is unrelated to this FORK/FUSE test.
        builder("""
            FROM books METADATA _id, _index, _score
            | FORK ( WHERE title:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
                   ( WHERE author:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC
            """).stages(STAGES).searchStats(SEARCH_STATS).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).run();
    }

    public void testWithTopResultsAndStats() {
        runGoldenTest("""
            FROM books METADATA _score
            | WHERE author:"Tolkien"
            | FORK ( SORT _score DESC | LIMIT 3 )
                   ( STATS total = COUNT(*) )
            """, STAGES);
    }

    public void testForkPushdown() {
        runGoldenTest("""
            FROM employees
            | FORK ( WHERE false | WHERE emp_no == 1 )
                   ( WHERE emp_no == 2 | LIMIT 100)
                   ( WHERE emp_no == 2 | SORT salary DESC | LIMIT 100)
            | LIMIT 10
            """, STAGES);
    }
}
