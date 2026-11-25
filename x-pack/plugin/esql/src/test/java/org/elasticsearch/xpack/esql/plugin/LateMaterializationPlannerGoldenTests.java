/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.Objects;

public class LateMaterializationPlannerGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LateMaterializationPlannerGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.NODE_REDUCE,
        Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
    );

    public void testBasicTopNLateMaterialization() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMultipleTopN() {
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

    public void testSomeFieldsNeededBeforeLateMaterialization() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | WHERE salary > 10000
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    /**
     * No narrowing {@code KEEP}, so the top-level {@code Project} that {@code ProjectAwayColumns} adds lists the whole relation.
     * Every {@code FORK} branch has this shape. The test has no {@code FORK} because {@code GoldenTestCase} cannot plan its
     * multiple {@code ExchangeExec}s. The data driver must output only {@code [_doc, hire_date]}.
     */
    public void testNoKeepWithFilter() {
        String query = """
            FROM employees
            | WHERE salary > 10000
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    /**
     * The Lucene source operator computes {@code _score} and no block loader reads it, so the node-reduce driver cannot load it.
     * It must cross the exchange although it is not a sort key and nothing under the TopN reads it.
     */
    public void testScoreMustCrossTheExchange() {
        String query = """
            FROM books METADATA _score
            | WHERE title:"Tolkien"
            | SORT year
            | LIMIT 5
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    /**
     * {@code Fetchability} accepts {@code _index} and {@code _id}. The {@code EVAL} under the TopN reads them, so the data driver
     * loads them, but they must not cross the exchange: the node-reduce driver loads them again. Only the sort key and the
     * {@code EVAL} result cross.
     */
    public void testReloadableMetadataMustNotCrossTheExchange() {
        String query = """
            FROM employees METADATA _index, _id
            | EVAL id_len = LENGTH(CONCAT(_index, _id))
            | SORT hire_date
            | LIMIT 5
            | KEEP _index, _id, id_len
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    /**
     * For a TSDB index {@code ReplaceSourceAttributes} synthesizes a fresh {@code FieldAttribute} per
     * {@code EsQueryExec#TIME_SERIES_SOURCE_FIELDS}, so {@code _ts_slice_index} and {@code _ts_future_max_timestamp} appear in the
     * pipeline breaker's physical output without being the output of any {@code EsRelation}. They must not reach the data-side
     * {@code Project}: its child is the logical fragment, which does not output them, and the data node creates its own pair when
     * it maps the fragment. Otherwise the data node fails with
     * {@code missing references [_ts_slice_index, _ts_future_max_timestamp]}.
     */
    public void testTimeSeriesSourceAttributesMustNotBeProjected() {
        String query = """
            TS k8s METADATA _tsid
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMultipleFieldSortTopN() {
        String query = """
            FROM employees
            | keep hire_date, emp_no, height
            | SORT hire_date, height
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testExpressionSortTopNKeepBeforeSort() {
        String query = """
            FROM employees
            | keep hire_date, height
            | SORT sin(height) * 2
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testExpressionSortTopNKeepAfterSort() {
        String query = """
            FROM employees
            | SORT sin(height) * 2
            | keep hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testPushedDownTopN() {
        String query = """
            FROM employees
            | keep emp_no, height
            | SORT height
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES); // default stats are indexed.
    }

    public void testPushedDownTopNWithFilter() {
        String query = """
            FROM employees
            | WHERE salary > 10000
            | keep emp_no, height
            | SORT height
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES); // default stats are indexed.
    }

    public void testTopNWithMissingSortField() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, missingFieldStats("hire_date"));
    }

    public void testLookupJoinOnDataNode() {
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

    public void testTopNThenStats() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            | STATS max_salary = MAX(salary), count = COUNT(*)
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testNullifiedFieldWithLateMaterialization() {
        String query = """
            SET unmapped_fields="nullify";
            FROM employees
            | KEEP hire_date, salary, emp_no, does_not_exist
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testNullifiedFieldAsSort() {
        String query = """
            SET unmapped_fields="nullify";
            FROM employees
            | KEEP hire_date, salary, does_not_exist
            | SORT does_not_exist
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMvExpandBeforeTopN() {
        String query = """
            FROM employees
            | keep emp_no, job_positions, salary
            | MV_EXPAND job_positions
            | SORT salary
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testBasicTopNByLateMaterialization() {
        String query = """
            FROM employees
            | keep hire_date, salary, languages, emp_no
            | SORT hire_date
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMultipleTopNBy() {
        String query = """
            FROM employees
            | keep hire_date, salary, languages, gender, emp_no
            | SORT hire_date
            | LIMIT 5 BY languages
            | SORT salary
            | LIMIT 3 BY gender
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testTopNByWithFilter() {
        String query = """
            FROM employees
            | keep hire_date, salary, languages, emp_no
            | WHERE salary > 10000
            | SORT hire_date
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testTopNByWithMissingSortField() {
        String query = """
            FROM employees
            | keep hire_date, salary, languages, emp_no
            | SORT hire_date
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, missingFieldStats("hire_date"));
    }

    public void testBasicLimitByLateMaterialization() {
        String query = """
            FROM employees
            | keep salary, languages, emp_no
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testLimitByMultipleGroupings() {
        String query = """
            FROM employees
            | keep salary, languages, gender, emp_no
            | LIMIT 3 BY languages, gender
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testLimitByWithMissingGroupField() {
        String query = """
            FROM employees
            | keep salary, languages, emp_no
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, missingFieldStats("languages"));
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
                return Objects.equals(field.string(), missingField) == false;
            }

            @Override
            public boolean isIndexed(FieldAttribute.FieldName field) {
                return exists(field);
            }
        };
    }
}
