/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.eql.EqlTestUtils.TEST_CFG;

public class OptimizerTests extends ESTestCase {


    private static final String INDEX_NAME = "test";
    private EqlParser parser = new EqlParser();
    private IndexResolution index = loadIndexResolution("mapping-default.json");

    private static Map<String, EsField> loadEqlMapping(String name) {
        return TypesTests.loadMapping(name);
    }

    private IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(new EsIndex(INDEX_NAME, loadEqlMapping(name)));
    }

    private LogicalPlan accept(IndexResolution resolution, String eql) {
        PreAnalyzer preAnalyzer = new PreAnalyzer();
        Analyzer analyzer = new Analyzer(TEST_CFG, new EqlFunctionRegistry(), new Verifier());
        Optimizer optimizer = new Optimizer();
        return optimizer.optimize(analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql), resolution)));
    }

    private LogicalPlan accept(String eql) {
        return accept(index, eql);
    }

    public void testIsNull() {
        List<String> tests = Arrays.asList(
            "foo where command_line == null",
            "foo where null == command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = accept(q);
            assertTrue(plan instanceof Project);
            plan = ((Project) plan).child();
            assertTrue(plan instanceof OrderBy);
            plan = ((OrderBy) plan).child();
            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof IsNull);

            IsNull check = (IsNull) condition.right();
            assertEquals(((FieldAttribute) check.field()).name(), "command_line");
        }
    }
    public void testIsNotNull() {
        List<String> tests = Arrays.asList(
            "foo where command_line != null",
            "foo where null != command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = accept(q);
            assertTrue(plan instanceof Project);
            plan = ((Project) plan).child();
            assertTrue(plan instanceof OrderBy);
            plan = ((OrderBy) plan).child();
            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof IsNotNull);

            IsNotNull check = (IsNotNull) condition.right();
            assertEquals(((FieldAttribute) check.field()).name(), "command_line");
        }
    }

    public void testEqualsWildcard() {
        List<String> tests = Arrays.asList(
            "foo where command_line == '* bar *'",
            "foo where '* bar *' == command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = accept(q);
            assertTrue(plan instanceof Project);
            plan = ((Project) plan).child();
            assertTrue(plan instanceof OrderBy);
            plan = ((OrderBy) plan).child();
            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof Like);

            Like like = (Like) condition.right();
            assertEquals(((FieldAttribute) like.field()).name(), "command_line");
            assertEquals(like.pattern().asJavaRegex(), "^.* bar .*$");
            assertEquals(like.pattern().asLuceneWildcard(), "* bar *");
            assertEquals(like.pattern().asIndexNameWildcard(), "* bar *");
        }
    }

    public void testNotEqualsWildcard() {
        List<String> tests = Arrays.asList(
            "foo where command_line != '* baz *'",
            "foo where '* baz *' != command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = accept(q);
            assertTrue(plan instanceof Project);
            plan = ((Project) plan).child();
            assertTrue(plan instanceof OrderBy);
            plan = ((OrderBy) plan).child();
            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof Not);

            Not not = (Not) condition.right();
            Like like = (Like) not.field();
            assertEquals(((FieldAttribute) like.field()).name(), "command_line");
            assertEquals(like.pattern().asJavaRegex(), "^.* baz .*$");
            assertEquals(like.pattern().asLuceneWildcard(), "* baz *");
            assertEquals(like.pattern().asIndexNameWildcard(), "* baz *");
        }
    }

    public void testWildcardEscapes() {
        LogicalPlan plan = accept("foo where command_line == '* %bar_ * \\\\ \\n \\r \\t'");
        assertTrue(plan instanceof Project);
        plan = ((Project) plan).child();
        assertTrue(plan instanceof OrderBy);
        plan = ((OrderBy) plan).child();
        assertTrue(plan instanceof Filter);

        Filter filter = (Filter) plan;
        And condition = (And) filter.condition();
        assertTrue(condition.right() instanceof Like);

        Like like = (Like) condition.right();
        assertEquals(((FieldAttribute) like.field()).name(), "command_line");
        assertEquals(like.pattern().asJavaRegex(), "^.* %bar_ .* \\\\ \n \r \t$");
        assertEquals(like.pattern().asLuceneWildcard(), "* %bar_ * \\\\ \n \r \t");
        assertEquals(like.pattern().asIndexNameWildcard(), "* %bar_ * \\ \n \r \t");
    }
}
