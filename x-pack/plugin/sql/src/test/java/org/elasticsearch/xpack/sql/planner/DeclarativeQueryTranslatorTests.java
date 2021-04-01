/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.planner;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.SqlTestUtils;
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
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;

public class DeclarativeQueryTranslatorTests extends ESTestCase {

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
            analyzer = new Analyzer(SqlTestUtils.TEST_CFG, new SqlFunctionRegistry(), getIndexResult, new Verifier(new Metrics()));
            optimizer = new Optimizer();
            planner = new Planner();
        }

        LogicalPlan plan(String sql, ZoneId zoneId) {
            return analyzer.analyze(parser.createStatement(sql, zoneId), true);
        }

        LogicalPlan plan(String sql) {
            return plan(sql, DateUtils.UTC);
        }

        PhysicalPlan optimizeAndPlan(String sql) {
            return optimizeAndPlan(plan(sql));
        }

        PhysicalPlan optimizeAndPlan(LogicalPlan plan) {
            return planner.plan(optimizer.optimize(plan),true);
        }
    }

    private static TestContext testContext;

    @BeforeClass
    public static void init() {
        testContext = new TestContext("mapping-multi-field-variation.json");
    }

    private final String name;
    private final String query;
    private final List<Matcher<String>> matchers;

    public DeclarativeQueryTranslatorTests(String name, String query, List<Matcher<String>> matchers) {
        this.name = name;
        this.query = query;
        this.matchers = matchers;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() throws Exception {
        return readSpec("querytranslator_tests.txt");
    }

    public void test() {
        assumeFalse("Test is ignored", name.endsWith("-Ignore"));
        
        PhysicalPlan p = testContext.optimizeAndPlan(query);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        final String query = eqe.queryContainer().toString().replaceAll("\\s+", "");
        matchers.forEach(m -> assertThat(query, m));
    }

    static Iterable<Object[]> readSpec(String url) throws Exception {
        ArrayList<Object[]> arr = new ArrayList<>();
        Map<String, Integer> testNames = new LinkedHashMap<>();

        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                DeclarativeQueryTranslatorTests.class.getResourceAsStream(url), StandardCharsets.UTF_8))) {
            int lineNumber = 0;
            String line;
            String name = null;
            String query = null;
            ArrayList<Matcher<String>> matchers = new ArrayList<>(8);

            StringBuilder sb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();

                if (line.isEmpty() || line.startsWith("//")) {
                    continue;
                }

                if (name == null) {
                    name = line;
                    Integer previousName = testNames.put(name, lineNumber);
                    if (previousName != null) {
                        throw new IllegalArgumentException("Duplicate test name '" + line + "' at line " + lineNumber
                            + " (previously seen at line " + previousName + ")");
                    }
                }

                else if (query == null) {
                    sb.append(line);
                    if (line.endsWith(";")) {
                        sb.setLength(sb.length() - 1);
                        query = sb.toString();
                        sb.setLength(0);
                    }
                }

                else {
                    boolean done = false;
                    if (line.endsWith(";")) {
                        line = line.substring(0, line.length() - 1);
                        done = true;
                    }
                    // no expectation
                    if (line.equals("null") == false && line.isEmpty() == false) {
                        String[] matcherAndExpectation = line.split("[ \\t]+", 2);
                        if (matcherAndExpectation.length != 2) {
                            throw new IllegalArgumentException("wrongly formatted assert, " +
                                "should be '<matcher> <expectation>', but on line " 
                                + url + ":" + lineNumber + " it was: " + line);
                        }
                        String matcherType = matcherAndExpectation[0];
                        String expectation = matcherAndExpectation[1];
                        switch (matcherType.toUpperCase(Locale.ROOT)) {
                            case "CONTAINS":
                                matchers.add(containsString(expectation));
                                break;
                            case "CONTAINSREGEX":
                                matchers.add(StringContainsRegex.containsRegex(expectation));
                                break;
                            default:
                                throw new IllegalArgumentException("unsupported matcher on line "
                                    + url + ":" + lineNumber + ": " + matcherType);
                        }
                    }

                    if (done) {
                        // Add and zero out for the next spec
                        addSpec(arr, name, query, matchers);
                        name = null;
                        query = null;
                        matchers.clear();
                    }
                }
            }

            if (name != null) {
                throw new IllegalStateException("Read a test [" + name + "] without a body at the end of [" + url + "]");
            }
        }
        return arr;
    }

    private static void addSpec(ArrayList<Object[]> arr, String name, String query, 
        List<Matcher<String>> matchers) {
        if ((Strings.isNullOrEmpty(name) == false) && (Strings.isNullOrEmpty(query) == false)) {
            arr.add(new Object[] { name, query, matchers });
        }
    }
    
    private static class StringContainsRegex extends TypeSafeDiagnosingMatcher<String> {

        private final Pattern pattern;

        protected StringContainsRegex(Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a string containing the pattern ").appendValue(pattern);
        }

        @Override
        protected boolean matchesSafely(String actual, Description mismatchDescription) {
            if (pattern.matcher(actual).find() == false) {
                mismatchDescription.appendText("the string was ").appendValue(actual);
                return false;
            }
            return true;
        }

        public static Matcher<String> containsRegex(String regex) {
            return new StringContainsRegex(Pattern.compile(regex));
        }
    }
    
}
