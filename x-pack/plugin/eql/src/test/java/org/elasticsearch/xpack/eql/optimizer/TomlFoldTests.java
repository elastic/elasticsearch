/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.eql.EqlTestUtils.TEST_CFG_CASE_INSENSITIVE;
import static org.elasticsearch.xpack.eql.EqlTestUtils.TEST_CFG_CASE_SENSITIVE;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class TomlFoldTests extends ESTestCase {

    protected static final String PARAM_FORMATTING = "%1$s.test -> %2$s";

    private static EqlParser parser = new EqlParser();
    private static final EqlFunctionRegistry functionRegistry = new EqlFunctionRegistry();
    private static Verifier verifier = new Verifier(new Metrics());
    private static Analyzer caseSensitiveAnalyzer = new Analyzer(TEST_CFG_CASE_SENSITIVE, functionRegistry, verifier);
    private static Analyzer caseInsensitiveAnalyzer = new Analyzer(TEST_CFG_CASE_INSENSITIVE, functionRegistry, verifier);

    private final int num;
    private final EqlFoldSpec spec;

    public TomlFoldTests(int num, EqlFoldSpec spec) {
        this.num = num;
        this.spec = spec;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {
        List<EqlFoldSpec> foldSpecs = EqlFoldSpecLoader.load("/test_folding.toml");
        foldSpecs.addAll(EqlFoldSpecLoader.load("/test_string_functions.toml"));
        List<EqlFoldSpec> unsupportedSpecs = EqlFoldSpecLoader.load("/test_unsupported.toml");

        HashSet<EqlFoldSpec> filteredSpecs = new HashSet<>(foldSpecs);
        filteredSpecs.removeAll(unsupportedSpecs);
        return asArray(filteredSpecs);
    }

    private static List<Object[]> asArray(Collection<EqlFoldSpec> specs) {
        AtomicInteger counter = new AtomicInteger();
        return specs.stream().map(spec -> new Object[] {
            counter.incrementAndGet(), spec
        }).collect(toList());
    }

    public void test() {
        // run both tests if case sensitivity doesn't matter
        if (spec.caseSensitive() == null) {
            testCaseSensitive(spec);
            testCaseInsensitive(spec);
        }
        // run only the case sensitive test
        else if (spec.caseSensitive()) {
            testCaseSensitive(spec);
        }
        // run only the case insensitive test
        else {
            testCaseInsensitive(spec);
        }
    }

    private void testCaseSensitive(EqlFoldSpec spec) {
        testWithAnalyzer(caseSensitiveAnalyzer, spec);
    }

    private void testCaseInsensitive(EqlFoldSpec spec) {
        testWithAnalyzer(caseInsensitiveAnalyzer, spec);
    }

    private void testWithAnalyzer(Analyzer analyzer, EqlFoldSpec spec) {
        Expression expr = parser.createExpression(spec.expression());
        LogicalPlan logicalPlan = new Project(EMPTY, new LocalRelation(EMPTY, emptyList()),
            singletonList(new Alias(Source.EMPTY, "test", expr)));
        LogicalPlan analyzed = analyzer.analyze(logicalPlan);

        assertTrue(analyzed instanceof Project);
        List<?> projections = ((Project) analyzed).projections();
        assertEquals(1, projections.size());
        assertTrue(projections.get(0) instanceof Alias);
        Alias a = (Alias) projections.get(0);

        assertTrue(a.child().foldable());
        Object folded = a.child().fold();

        // upgrade to a long, because the parser typically downgrades Long -> Integer when possible
        if (folded instanceof Integer) {
            folded  = ((Integer) folded).longValue();
        }

        assertEquals(spec.expected(), folded);
    }
}
