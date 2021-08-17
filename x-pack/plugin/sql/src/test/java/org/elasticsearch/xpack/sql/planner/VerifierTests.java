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
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
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
        new Verifier(new Metrics(), TEST_CFG.version())
    );
    private final Planner planner = new Planner();

    private PhysicalPlan verify(String sql) {
        return planner.plan(analyzer.analyze(parser.createStatement(sql), true), true);
    }

    private String error(String sql) {
        PlanningException e = expectThrows(
            PlanningException.class,
            () -> verify(sql)
        );
        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    private String innerLimitMsg(int line, int column) {
        return line
            + ":"
            + column
            + ": LIMIT or TOP cannot be used in a subquery if outer query contains GROUP BY, ORDER BY, PIVOT or WHERE";
    }

    public void testSubselectWithOrderByOnTopOfOrderByAndLimit() {
        assertEquals(
            innerLimitMsg(1, 50),
            error("SELECT * FROM (SELECT * FROM test ORDER BY 1 ASC LIMIT 10) ORDER BY 2")
        );
        assertEquals(
                innerLimitMsg(1, 50),
                error("SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 10) ORDER BY 1) ORDER BY 2")
        );
        assertEquals(
                innerLimitMsg(1, 66),
                error("SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY 1 ASC) LIMIT 5) ORDER BY 1 DESC")
        );
        assertEquals(
                innerLimitMsg(1, 142),
                error("SELECT * FROM (" +
                        "SELECT * FROM (" +
                            "SELECT * FROM (" +
                                "SELECT * FROM test ORDER BY int DESC" +
                            ") ORDER BY int ASC NULLS LAST) " +
                        "ORDER BY int DESC NULLS LAST LIMIT 12) " +
                      "ORDER BY int DESC NULLS FIRST")
        );
        assertEquals(
            innerLimitMsg(1, 50),
            error("SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 10) ORDER BY 1 LIMIT 20) ORDER BY 2")
        );
    }

    public void testSubselectWithOrderByOnTopOfGroupByOrderByAndLimit() {
        assertEquals(
            innerLimitMsg(1, 86),
            error(
                "SELECT * FROM (SELECT max(int) AS max, bool FROM test GROUP BY bool ORDER BY max ASC LIMIT 10) ORDER BY max DESC"
            )
        );
        assertEquals(
            innerLimitMsg(1, 102),
            error(
                "SELECT * FROM ("
                    + "SELECT * FROM ("
                    + "SELECT max(int) AS max, bool FROM test GROUP BY bool ORDER BY max ASC) "
                    + "LIMIT 10) "
                    + "ORDER BY max DESC"
            )
        );
        assertEquals(
                innerLimitMsg(1, 176),
                error("SELECT * FROM (" +
                        "SELECT * FROM (" +
                            "SELECT * FROM (" +
                                "SELECT max(int) AS max, bool FROM test GROUP BY bool ORDER BY max DESC" +
                            ") ORDER BY max ASC NULLS LAST) " +
                        "ORDER BY max DESC NULLS LAST LIMIT 12) " +
                      "ORDER BY max DESC NULLS FIRST")
        );
    }

    public void testInnerLimitWithWhere() {
        assertEquals(innerLimitMsg(1, 35),
            error("SELECT * FROM (SELECT * FROM test LIMIT 10) WHERE int = 1"));
        assertEquals(innerLimitMsg(1, 50),
            error("SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 10)) WHERE int = 1"));
        assertEquals(innerLimitMsg(1, 51),
            error("SELECT * FROM (SELECT * FROM (SELECT * FROM test) LIMIT 10) WHERE int = 1"));
    }

    public void testInnerLimitWithGroupBy() {
        assertEquals(innerLimitMsg(1, 37),
            error("SELECT int FROM (SELECT * FROM test LIMIT 10) GROUP BY int"));
        assertEquals(innerLimitMsg(1, 52),
            error("SELECT int FROM (SELECT * FROM (SELECT * FROM test LIMIT 10)) GROUP BY int"));
        assertEquals(innerLimitMsg(1, 53),
            error("SELECT int FROM (SELECT * FROM (SELECT * FROM test) LIMIT 10) GROUP BY int"));
    }

    public void testInnerLimitWithPivot() {
        assertEquals(innerLimitMsg(1, 52),
            error("SELECT * FROM (SELECT int, bool, keyword FROM test LIMIT 10) PIVOT (AVG(int) FOR bool IN (true, false))"));
    }

    public void testTopWithOrderBySucceeds() {
        PhysicalPlan plan = verify("SELECT TOP 5 * FROM test ORDER BY int");
        assertEquals(EsQueryExec.class, plan.getClass());
    }

    public void testInnerTop() {
        assertEquals(innerLimitMsg(1, 23),
            error("SELECT * FROM (SELECT TOP 10 * FROM test) WHERE int = 1"));
        assertEquals(innerLimitMsg(1, 23),
            error("SELECT * FROM (SELECT TOP 10 * FROM test) ORDER BY int"));
        assertEquals(innerLimitMsg(1, 23),
            error("SELECT * FROM (SELECT TOP 10 int, bool, keyword FROM test) PIVOT (AVG(int) FOR bool IN (true, false))"));
    }
}
