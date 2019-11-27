/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.util.TestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.type.EsField;
import org.junit.Before;

import java.util.Map;

//@TestLogging(value = "org.elasticsearch.xpack.sql:TRACE", reason = "debug")
public class QueryFolderTests extends ESTestCase {

    private EqlParser parser;
    private Analyzer analyzer;
    private Optimizer optimizer = new Optimizer();
    private Planner planner = new Planner();

    @Before
    public void init() {
        Map<String, EsField> mapping = TestUtils.loadMapping("mapping-default.json");
        EsIndex test = new EsIndex("test", mapping);
        parser = new EqlParser(test);

        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(TestUtils.TEST_CFG, new FunctionRegistry(), getIndexResult,
                new Verifier(new Metrics()));
    }

    private LogicalPlan l(String sql) {
        return optimizer.optimize(analyzer.analyze(parser.createStatement(sql), true));
    }

    private PhysicalPlan p(String sql) {
        return planner.plan(l(sql), true);
    }

    public void testBasicFilter() {
        String q = "process WHERE process_name == 'value'";
        System.out.println(l(q));
        System.out.println(p(q));
    }

    public void testFilterIn() {
        String q = "process WHERE process_name IN ('ipconfig.exe', 'netstat.exe', 'systeminfo.exe', 'route.exe')";
        System.out.println(l(q));
        System.out.println(p(q));
    }

    public void testSubfieldSelection() {
        String q = "process WHERE hostname == 'exactValue'";
        System.out.println(l(q));
        System.out.println(p(q));
    }

    public void testFolding() {
        PhysicalPlan p = p("process WHERE pid == 100 / (2 + 3) AND TRUE");
        System.out.println(p);
    }

    //
    // Validations
    //
    public void testIncorrectField() {
        Exception ex = expectThrows(Exception.class, () -> p("process WHERE processname == 'value'"));
        assertTrue(ex.getMessage().contains("Unknown column [processname], did you mean "));
    }

    public void testIncorrectType() {
        Exception ex = expectThrows(Exception.class, () -> p("process WHERE pid > 123 + 'value' "));
        assertTrue(ex.getMessage().contains("must be [numeric], found"));
    }
}