/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.Having;
import org.elasticsearch.xpack.sql.stats.Metrics;

import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class AnalyzerTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();
    private final Analyzer analyzer = new Analyzer(
        SqlTestUtils.TEST_CFG,
        new SqlFunctionRegistry(),
        IndexResolution.valid(new EsIndex("test", loadMapping("mapping-basic.json"))),
        new Verifier(new Metrics())
    );

    private LogicalPlan analyze(String sql) {
        return analyzer.analyze(parser.createStatement(sql), false);
    }

    public void testResolveRecursiveFilterRefs() {
        LogicalPlan plan = analyze("SELECT emp_no * 10 emp_no, emp_no + 1 e FROM test WHERE e > 0");

        assertThat(plan, instanceOf(Project.class));
        Project p = (Project) plan;
        assertThat(p.child(), instanceOf(Filter.class));
        Filter f = (Filter) p.child();
        assertThat(f.condition(), instanceOf(GreaterThan.class));
        GreaterThan gt = (GreaterThan) f.condition();
        assertThat(gt.left().toString(), is("emp_no + 1"));
    }

    public void testResolveAlternatingRecursiveFilterRefs() {
        // Queries like the following used to cause a StackOverflowError in ResolveFilterRefs.
        // see https://github.com/elastic/elasticsearch/issues/81577
        LogicalPlan plan = analyze(
            "SELECT MAX(emp_no) emp_no, MAX(emp_no) emp_no, AVG(emp_no) e FROM test WHERE e > 0 GROUP BY emp_no HAVING e > 0"
        );

        assertThat(plan, instanceOf(Having.class));
        Having h = (Having) plan;
        assertThat(h.condition(), instanceOf(GreaterThan.class));
        GreaterThan gt = (GreaterThan) h.condition();
        assertThat(gt.left().toString(), startsWith("e{r}"));
        assertThat(h.child(), instanceOf(Aggregate.class));
        Aggregate a = (Aggregate) h.child();
        assertThat(a.child(), instanceOf(Filter.class));
        Filter f = (Filter) a.child();
        assertThat(f.condition(), instanceOf(GreaterThan.class));
        GreaterThan gtF = (GreaterThan) f.condition();
        // having the aggregation function in the where clause will cause an error later on
        assertThat(gtF.left().toString(), is("AVG(emp_no)"));
    }

    public void testResolveAlternatingRecursiveFilterRefs_WithCountInSubSelect() {
        // Queries like the following used to cause a StackOverflowError in Analyzer.
        // see https://github.com/elastic/elasticsearch/issues/81577
        // The query itself is not supported (using aggregates in a sub-select) but it shouldn't bring down ES
        LogicalPlan plan = analyze(
            "SELECT salary, ks AS salary FROM (SELECT COUNT(salary/1000) AS salary, salary/1000 ks FROM test GROUP BY salary/1000) "
                + "WHERE ks > 30 AND salary > 3"
        );
        // passing the analysis step should succeed
        LogicalPlan optimizedPlan = new Optimizer().optimize(plan);
        assertThat(optimizedPlan, instanceOf(Project.class));
        Project p = (Project) optimizedPlan;
        assertThat(p.child(), instanceOf(Filter.class));
        Filter f = (Filter) p.child();
        assertThat(f.condition(), instanceOf(GreaterThan.class));
        GreaterThan gt = (GreaterThan) f.condition();
        assertThat(gt.left(), instanceOf(Count.class));
        assertThat(gt.right(), instanceOf(Literal.class));
        assertThat(f.child(), instanceOf(Aggregate.class));
        Aggregate a = (Aggregate) f.child();
        assertThat(a.groupings().size(), is(1));
        assertThat(a.groupings().get(0), instanceOf(Div.class));
        assertThat(a.child(), instanceOf(Filter.class));
        Filter af = (Filter) a.child();
        assertThat(af.condition(), instanceOf(GreaterThan.class));
        gt = (GreaterThan) af.condition();
        assertThat(gt.left(), instanceOf(Div.class));
        assertThat(gt.right(), instanceOf(Literal.class));
    }

    public void testResolveAlternatingRecursiveFilterRefs_WithAvgInSubSelect() {
        // Queries like the following used to cause a StackOverflowError in Analyzer.
        // see https://github.com/elastic/elasticsearch/issues/81577
        // The query itself is not supported (using aggregates in a sub-select) but it shouldn't bring down ES
        LogicalPlan plan = analyze(
            "SELECT salary AS salary, salary AS s FROM (SELECT ROUND(AVG(salary)) AS salary FROM test_emp GROUP BY gender) "
                + "WHERE s > 48000 OR salary > 46000"
        );
        // passing the analysis step should succeed
        LogicalPlan optimizedPlan = new Optimizer().optimize(plan);
        assertThat(optimizedPlan, instanceOf(Project.class));
        Project p = (Project) optimizedPlan;
        assertThat(p.child(), instanceOf(Filter.class));
        Filter f = (Filter) p.child();
        assertThat(f.condition(), instanceOf(GreaterThan.class));
        GreaterThan gt = (GreaterThan) f.condition();
        assertThat(gt.left(), instanceOf(Round.class));
        assertThat(gt.right(), instanceOf(Literal.class));
        assertThat(f.child(), instanceOf(Aggregate.class));
        Aggregate a = (Aggregate) f.child();
        assertThat(a.groupings().size(), is(1));
        assertThat(a.groupings().get(0), instanceOf(FieldAttribute.class));
        assertThat(a.child(), instanceOf(EsRelation.class));
        // the first filter (s > 48000) is removed by the optimizer
    }
}
