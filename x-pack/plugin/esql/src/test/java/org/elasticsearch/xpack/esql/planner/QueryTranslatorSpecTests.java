/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

public class QueryTranslatorSpecTests extends ESTestCase {

    private static final List<String> TEST_FILENAMES = List.of("querytranslator_tests.txt");
    private static TestPlannerOptimizer plannerOptimizer;

    private static Analyzer makeAnalyzer(String mappingFileName) {
        var mapping = loadMapping(mappingFileName);
        EsIndex test = new EsIndex("test", mapping, Set.of("test"));
        IndexResolution getIndexResult = IndexResolution.valid(test);

        return new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResult, new EnrichResolution()),
            new Verifier(new Metrics())
        );
    }

    @BeforeClass
    public static void init() {
        plannerOptimizer = new TestPlannerOptimizer(EsqlTestUtils.TEST_CFG, makeAnalyzer("mapping-all-types.json"));
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

        PhysicalPlan optimized = plannerOptimizer.plan(query);
        EsQueryExec eqe = (EsQueryExec) optimized.collectLeaves().get(0);
        final String query = eqe.query().toString().replaceAll("\\s+", "");
        matchers.forEach(m -> assertThat(query, m));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
