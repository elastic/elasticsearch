/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class LateMaterializationPlannerGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.NODE_REDUCE,
        Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
    );

    public void testBasicTopNLateMaterialization() throws Exception {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMultipleTopN() throws Exception {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            | SORT salary
            | LIMIT 10
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testSomeFieldsNeededBeforeLateMaterialization() throws Exception {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | WHERE salary > 10000
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMultipleFieldSortTopN() throws Exception {
        String query = """
            FROM employees
            | keep hire_date, emp_no, height
            | SORT hire_date, height
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testExpressionSortTopNKeepBeforeSort() throws Exception {
        String query = """
            FROM employees
            | keep hire_date, height
            | SORT sin(height) * 2
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testExpressionSortTopNKeepAfterSort() throws Exception {
        String query = """
            FROM employees
            | SORT sin(height) * 2
            | keep hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testPushedDownTopN() throws Exception {
        String query = """
            FROM employees
            | keep emp_no, height
            | SORT height
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES); // default stats are indexed.
    }

    public void testTopNWithMissingSortField() throws Exception {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, missingFieldStats("hire_date"));
    }

    public void testLookupJoinOnDataNode() throws Exception {
        String query = """
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE emp_no >= 10091 AND emp_no < 10094
            | SORT emp_no
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testTopNThenStats() throws Exception {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            | STATS avg_salary = AVG(salary), count = COUNT(*)
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMvExpandBeforeTopN() throws Exception {
        String query = """
            FROM employees
            | keep emp_no, job_positions, salary
            | MV_EXPAND job_positions
            | SORT salary
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
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

    // Returns false for exists() for the specified field, simulating a missing field on the data node.
    private static EsqlTestUtils.TestSearchStats missingFieldStats(String missingField) {
        return new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean exists(FieldAttribute.FieldName field) {
                return false;
            }

            @Override
            public boolean isIndexed(FieldAttribute.FieldName field) {
                return false;
            }
        };
    }
}
