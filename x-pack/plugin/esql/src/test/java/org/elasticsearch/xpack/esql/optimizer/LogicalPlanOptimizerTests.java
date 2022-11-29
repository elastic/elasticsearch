/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.FoldNull;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.BeforeClass;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class LogicalPlanOptimizerTests extends ESTestCase {

    private static EsqlParser parser;
    private static Analyzer analyzer;
    private static LogicalPlanOptimizer logicalOptimizer;
    private static Map<String, EsField> mapping;

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();

        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        logicalOptimizer = new LogicalPlanOptimizer();

        analyzer = new Analyzer(getIndexResult, new EsqlFunctionRegistry(), new Verifier(), TEST_CFG);
    }

    public void testCombineProjections() {
        var plan = plan("""
            from test
            | project emp_no, *name, salary
            | project last_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("last_name"));
        var relation = as(project.child(), EsRelation.class);
    }

    public void testCombineProjectionWithFilterInBetween() {
        var plan = plan("""
            from test
            | project *name, salary
            | where salary > 10
            | project last_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("last_name"));
    }

    public void testCombineProjectionWhilePreservingAlias() {
        var plan = plan("""
            from test
            | project x = first_name, salary
            | where salary > 10
            | project y = x
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("y"));
        var p = project.projections().get(0);
        var alias = as(p, Alias.class);
        assertThat(Expressions.name(alias.child()), containsString("first_name"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-internal/issues/378")
    public void testCombineProjectionWithAggregation() {
        var plan = plan("""
            from test
            | stats avg(salary) by last_name, first_name
            """);

        var agg = as(plan, Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("last_name"));
        assertThat(Expressions.names(agg.groupings()), contains("last_name", "first_name"));
    }

    public void testCombineLimits() {
        var limitValues = new int[] { randomIntBetween(10, 99), randomIntBetween(100, 1000) };
        var firstLimit = randomBoolean() ? 0 : 1;
        var secondLimit = firstLimit == 0 ? 1 : 0;
        var oneLimit = new Limit(EMPTY, L(limitValues[firstLimit]), emptySource());
        var anotherLimit = new Limit(EMPTY, L(limitValues[secondLimit]), oneLimit);
        assertEquals(
            new Limit(EMPTY, L(Math.min(limitValues[0], limitValues[1])), emptySource()),
            new LogicalPlanOptimizer.CombineLimits().rule(anotherLimit)
        );
    }

    public void testMultipleCombineLimits() {
        var numberOfLimits = randomIntBetween(3, 10);
        var minimum = randomIntBetween(10, 99);
        var limitWithMinimum = randomIntBetween(0, numberOfLimits - 1);

        var plan = emptySource();
        for (int i = 0; i < numberOfLimits; i++) {
            var value = i == limitWithMinimum ? minimum : randomIntBetween(100, 1000);
            plan = new Limit(EMPTY, L(value), plan);
        }
        assertEquals(new Limit(EMPTY, L(minimum), emptySource()), new LogicalPlanOptimizer().optimize(plan));
    }

    public void testBasicNullFolding() {
        FoldNull rule = new FoldNull();
        assertNullLiteral(rule.rule(new Add(EMPTY, L(randomInt()), Literal.NULL)));
        assertNullLiteral(rule.rule(new Round(EMPTY, Literal.NULL, null)));
        assertNullLiteral(rule.rule(new Length(EMPTY, Literal.NULL)));
    }

    private LogicalPlan plan(String query) {
        return logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query)));
    }

    private void assertNullLiteral(Expression expression) {
        assertEquals(Literal.class, expression.getClass());
        assertNull(expression.fold());
    }
}
