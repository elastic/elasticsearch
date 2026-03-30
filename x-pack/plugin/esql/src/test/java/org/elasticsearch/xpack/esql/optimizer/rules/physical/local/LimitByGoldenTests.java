/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class LimitByGoldenTests extends GoldenTestCase {

    public void testLimitByWithoutSort() {
        runGoldenTest(
            """
                FROM employees
                | LIMIT 5 BY emp_no + 4, languages
                """,
            EnumSet.of(
                Stage.ANALYSIS,
                Stage.LOGICAL_OPTIMIZATION,
                Stage.PHYSICAL_OPTIMIZATION,
                Stage.LOCAL_PHYSICAL_OPTIMIZATION,
                Stage.NODE_REDUCE,
                Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
            ),
            STATS
        );
    }

    public void testSortLimitBy() {
        runGoldenTest(
            """
                FROM employees
                | SORT salary
                | LIMIT 5 BY emp_no + 4, languages
                """,
            EnumSet.of(
                Stage.ANALYSIS,
                Stage.LOGICAL_OPTIMIZATION,
                Stage.PHYSICAL_OPTIMIZATION,
                Stage.LOCAL_PHYSICAL_OPTIMIZATION,
                Stage.NODE_REDUCE,
                Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
            ),
            STATS
        );
    }

    private static final EsqlTestUtils.TestSearchStatsWithMinMax STATS = new EsqlTestUtils.TestSearchStatsWithMinMax(
        Map.of("date", dateTimeToLong("2023-10-20T12:15:03.360Z")),
        Map.of("date", dateTimeToLong("2023-10-23T13:55:01.543Z"))
    );
}
