/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

public class PostOptimizerVerifierTests extends ESTestCase {

    private SqlParser parser;
    private Analyzer analyzer;
    private Optimizer optimizer;
    private Planner planner;
    private IndexResolution indexResolution;

    @Before
    public void init() {
        parser = new SqlParser();

        Map<String, EsField> mapping = SqlTypesTests.loadMapping("mapping-multi-field-variation.json");
        EsIndex test = new EsIndex("test", mapping);
        indexResolution = IndexResolution.valid(test);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, new SqlFunctionRegistry(), indexResolution, new Verifier(new Metrics()));
        optimizer = new Optimizer();
        planner = new Planner();
    }

    @After
    public void destroy() {
        parser = null;
        analyzer = null;
    }

    private PhysicalPlan plan(String sql) {
        return planner.plan(optimizer.optimize(analyzer.analyze(parser.createStatement(sql), true)), true);
    }

    private String error(String sql) {
        return error(indexResolution, sql);
    }

    private String error(IndexResolution getIndexResult, String sql) {
        PlanningException e = expectThrows(PlanningException.class, () -> plan(sql));
        assertTrue(e.getMessage().startsWith("Found "));
        String header = "Found 1 problem(s)\nline ";
        return e.getMessage().substring(header.length());
    }

    public void testPivotInnerAgg() {
        assertEquals("1:59: Aggregation [SUM_OF_SQUARES(int)] not supported (yet) by PIVOT",
                error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(SUM_OF_SQUARES(int) FOR keyword IN ('bla'))"));
    }

    public void testPivotNestedInnerAgg() {
        assertEquals("1:65: Aggregation [SUM_OF_SQUARES(int)] not supported (yet) by PIVOT",
                error("SELECT * FROM (SELECT int, keyword, bool FROM test) " + "PIVOT(ROUND(SUM_OF_SQUARES(int)) FOR keyword IN ('bla'))"));
    }
}
