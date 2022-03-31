/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.planner;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;
import org.elasticsearch.xpack.sql.util.DateUtils;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.sql.SqlTestUtils.TEST_CFG;

public class QueryTranslatorSpecTests extends ESTestCase {

    private static final List<String> TEST_FILENAMES = List.of("querytranslator_tests.txt", "querytranslator_subqueries_tests.txt");

    private static class TestContext {
        private final SqlParser parser;
        private final Analyzer analyzer;
        private final Optimizer optimizer;
        private final Planner planner;

        TestContext(String mappingFile) {
            parser = new SqlParser();
            Map<String, EsField> mapping = SqlTypesTests.loadMapping(mappingFile);
            EsIndex test = new EsIndex("test", mapping);
            IndexResolution getIndexResult = IndexResolution.valid(test);
            analyzer = new Analyzer(TEST_CFG, new SqlFunctionRegistry(), getIndexResult, new Verifier(new Metrics()));
            optimizer = new Optimizer();
            planner = new Planner();
        }

        LogicalPlan plan(String sql) {
            return analyzer.analyze(parser.createStatement(sql, DateUtils.UTC), true);
        }

        PhysicalPlan optimizeAndPlan(String sql) {
            return optimizeAndPlan(plan(sql));
        }

        PhysicalPlan optimizeAndPlan(LogicalPlan plan) {
            return planner.plan(optimizer.optimize(plan), true);
        }
    }

    private static TestContext testContext;

    @BeforeClass
    public static void init() {
        testContext = new TestContext("mapping-multi-field-variation.json");
    }

    private final String filename;
    private final String name;
    private final String query;
    private final List<Matcher<String>> matchers;

    public QueryTranslatorSpecTests(String filename, String name, String query, List<Matcher<String>> matchers) {
        this.filename = filename;
        this.name = name;
        this.query = query;
        this.matchers = matchers;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = "%1$s/%2$s")
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for (String filename : TEST_FILENAMES) {
            params.addAll(TestUtils.readSpec(QueryTranslatorSpecTests.class, filename));
        }
        return params;
    }

    public void test() {
        assumeFalse("Test is ignored", name.endsWith("-Ignore"));

        PhysicalPlan p = testContext.optimizeAndPlan(query);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        final String query = eqe.queryContainer().toString().replaceAll("\\s+", "");
        matchers.forEach(m -> assertThat(query, m));
    }
}
