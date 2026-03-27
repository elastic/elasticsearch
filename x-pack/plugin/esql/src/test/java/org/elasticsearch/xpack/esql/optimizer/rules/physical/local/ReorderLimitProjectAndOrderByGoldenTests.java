/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class ReorderLimitProjectAndOrderByGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    /**
     * In this case the 2*salary AS language_name ends up in a Project and LimitBy references it, so LimitBy -> Project cannot be swapped
     */
    public void testLimitByAndProjectNotSwapped() {
        assumeTrue("LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_LIMIT_BY.isEnabled());
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | EVAL language_name = 2*salary
            | LOOKUP JOIN languages_lookup ON language_code
            | LIMIT 5 BY language_code
            """, STAGES);
    }

    /**
     * In this case we would end up with LimitBy -> Project -> OrderBy where 2*salary AS language_name gets
     * referenced by LimitBy, so the only way to fix the query is to swap LimitBy and OrderBy
     */
    public void testProjectAndOrderBySwapped() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | EVAL language_name = 2*salary
            | LOOKUP JOIN languages_lookup ON language_code
            | SORT salary
            | LIMIT 5 BY language_code
            """, STAGES);
    }

    private static final EsqlTestUtils.TestSearchStatsWithMinMax STATS = new EsqlTestUtils.TestSearchStatsWithMinMax(
        Map.of("date", dateTimeToLong("2023-10-20T12:15:03.360Z")),
        Map.of("date", dateTimeToLong("2023-10-23T13:55:01.543Z"))
    );
}
