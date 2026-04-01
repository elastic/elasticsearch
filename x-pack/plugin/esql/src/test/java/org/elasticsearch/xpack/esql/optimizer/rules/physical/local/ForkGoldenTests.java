/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class ForkGoldenTests extends GoldenTestCase {
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
        runGoldenTest("""
            FROM books METADATA _id, _index, _score
            | FORK ( WHERE title:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
                   ( WHERE author:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC
            """, STAGES);
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
