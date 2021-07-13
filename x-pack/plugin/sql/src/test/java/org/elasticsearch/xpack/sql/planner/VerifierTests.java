/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.stats.Metrics;

import static org.elasticsearch.xpack.sql.SqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;

public class VerifierTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();
    private final IndexResolution indexResolution = IndexResolution.valid(
        new EsIndex("test", loadMapping("mapping-multi-field-with-nested.json"))
    );
    private final Analyzer analyzer = new Analyzer(
        TEST_CFG,
        new SqlFunctionRegistry(),
        indexResolution,
        new Verifier(new Metrics())
    );
    private final Planner planner = new Planner();

    private String error(String sql) {
        PlanningException e = expectThrows(
            PlanningException.class,
            () -> planner.plan(analyzer.analyze(parser.createStatement(sql), true), true)
        );
        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    public void testSubselectWithOrderByOnTopOfOrderByAndLimit() {
        assertEquals(
            "1:60: Cannot use ORDER BY on top of a subquery with ORDER BY and LIMIT",
            error("SELECT * FROM (SELECT * FROM test ORDER BY 1 ASC LIMIT 10) ORDER BY 2")
        );
        assertEquals(
                "1:72: Cannot use ORDER BY on top of a subquery with ORDER BY and LIMIT",
                error("SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 10) ORDER BY 1) ORDER BY 2")
        );
        assertEquals(
                "1:75: Cannot use ORDER BY on top of a subquery with ORDER BY and LIMIT",
                error("SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY 1 ASC) LIMIT 5) ORDER BY 1 DESC")
        );
        assertEquals(
                "1:152: Cannot use ORDER BY on top of a subquery with ORDER BY and LIMIT",
                error("SELECT * FROM (" +
                        "SELECT * FROM (" +
                            "SELECT * FROM (" +
                                "SELECT * FROM test ORDER BY int DESC" +
                            ") ORDER BY int ASC NULLS LAST) " +
                        "ORDER BY int DESC NULLS LAST LIMIT 12) " +
                      "ORDER BY int DESC NULLS FIRST")
        );
    }

    public void testSubselectWithOrderByOnTopOfGroupByOrderByAndLimit() {
        assertEquals(
            "1:96: Cannot use ORDER BY on top of a subquery with ORDER BY and LIMIT",
            error(
                "SELECT * FROM (SELECT max(int) AS max, bool FROM test GROUP BY bool ORDER BY max ASC LIMIT 10) ORDER BY max DESC"
            )
        );
        assertEquals(
            "1:112: Cannot use ORDER BY on top of a subquery with ORDER BY and LIMIT",
            error(
                "SELECT * FROM ("
                    + "SELECT * FROM ("
                    + "SELECT max(int) AS max, bool FROM test GROUP BY bool ORDER BY max ASC) "
                    + "LIMIT 10) "
                    + "ORDER BY max DESC"
            )
        );
        assertEquals(
                "1:186: Cannot use ORDER BY on top of a subquery with ORDER BY and LIMIT",
                error("SELECT * FROM (" +
                        "SELECT * FROM (" +
                            "SELECT * FROM (" +
                                "SELECT max(int) AS max, bool FROM test GROUP BY bool ORDER BY max DESC" +
                            ") ORDER BY max ASC NULLS LAST) " +
                        "ORDER BY max DESC NULLS LAST LIMIT 12) " +
                      "ORDER BY max DESC NULLS FIRST")
        );
    }
}
