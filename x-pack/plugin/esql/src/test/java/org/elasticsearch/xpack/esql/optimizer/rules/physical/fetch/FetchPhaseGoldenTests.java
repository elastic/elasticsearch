/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.EnumSet;

/**
 * The plans of the fetch phase, next to the plans the same queries get without it. Each test writes two sets of files:
 * {@code fetch/} with the fetch phase and {@code eager/} without. The {@code physical_optimization} file is the plan
 * the coordinator ships, {@code local_reduce_planned_*} are the node reduce stage and the data driver plan a data node
 * splits it into, and {@code local_reduce_physical_optimization_data_driver} is the data driver plan after local
 * optimization.
 */
public class FetchPhaseGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public FetchPhaseGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.NODE_REDUCE,
        Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
    );

    /** The sort key crosses the exchange, the kept columns are fetched for the ten winners, the filter field stays local. */
    public void testTopN() {
        runBoth("""
            FROM employees
            | WHERE salary > 50000
            | SORT hire_date DESC
            | LIMIT 10
            | KEEP first_name, last_name, hire_date
            """, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    /** The same query when the sort cannot be pushed to Lucene: every data driver sorts, the node cut merges. */
    public void testTopNNotPushedToLucene() {
        runBoth("""
            FROM employees
            | WHERE salary > 50000
            | SORT hire_date DESC
            | LIMIT 10
            | KEEP first_name, last_name, hire_date
            """, unindexedStats());
    }

    /** The sort key crosses the exchange even though the query does not return it. */
    public void testSortKeyNotKept() {
        runBoth("""
            FROM employees
            | SORT hire_date
            | LIMIT 10
            | KEEP first_name, last_name
            """, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    /** Without a sort only the document reference crosses the exchange. */
    public void testLimit() {
        runBoth("""
            FROM employees
            | LIMIT 10
            | KEEP first_name, last_name
            """, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    /** Without KEEP every column is fetched, and a projection on top restores the original column order. */
    public void testNoKeep() {
        runBoth("""
            FROM employees
            | SORT emp_no
            | LIMIT 5
            """, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    /** A value computed before the cut crosses the exchange as a value. Its inputs never leave the data node. */
    public void testEvalBeforeTheCut() {
        runBoth("""
            FROM employees
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | SORT hire_date
            | LIMIT 10
            | KEEP full_name, salary
            """, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    /** WHERE and EVAL after the cut run on the coordinator over the fetched columns. */
    public void testWhereAndEvalAfterTheCut() {
        runBoth("""
            FROM employees
            | SORT hire_date
            | LIMIT 10
            | WHERE salary > 50000
            | EVAL monthly = salary / 12
            | KEEP first_name, monthly
            """, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    /** {@code _source} is the column the fetch phase saves the most on. */
    public void testSource() {
        runBoth("""
            FROM employees METADATA _source
            | SORT emp_no
            | LIMIT 3
            | KEEP emp_no, _source
            """, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    /** {@code _score} is computed by the query, not stored with the document, so it crosses the exchange. */
    public void testScore() {
        runBoth("""
            FROM employees METADATA _score
            | WHERE first_name == "Georgi"
            | SORT _score DESC
            | LIMIT 10
            | KEEP first_name, last_name, _score
            """, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    private void runBoth(String query, SearchStats searchStats) {
        builder(query).stages(STAGES)
            .searchStats(searchStats)
            .flags(EsqlFlags.withFetchPhase(true))
            .since(FetchPhasePolicy.PLANNER_MINIMUM)
            .nestedPath("fetch")
            .run();
        builder(query).stages(STAGES).searchStats(searchStats).flags(EsqlFlags.withFetchPhase(false)).nestedPath("eager").run();
    }

    // Prevents TopN pushdown.
    private static EsqlTestUtils.TestSearchStats unindexedStats() {
        return new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean isIndexed(FieldAttribute.FieldName field) {
                return false;
            }
        };
    }
}
