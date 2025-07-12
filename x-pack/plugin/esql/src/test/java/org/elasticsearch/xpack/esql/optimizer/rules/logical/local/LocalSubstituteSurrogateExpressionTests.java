/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;

public class LocalSubstituteSurrogateExpressionTests extends LocalLogicalPlanOptimizerTests {

    public void testSubstituteDateTruncInEvalWithRoundTo() {
        var plan = plan("""
              from test
              | sort hire_date
              | eval x = date_trunc(1 day, hire_date)
              | keep emp_no, hire_date, x
              | limit 5
            """);

        // create a SearchStats with min and max millis
        Map<String, Object> minValue = Map.of("hire_date", 1697804103360L); // 2023-10-20T12:15:03.360Z
        Map<String, Object> maxValue = Map.of("hire_date", 1698069301543L); // 2023-10-23T13:55:01.543Z
        SearchStats searchStats = new EsqlTestUtils.TestSearchStatsWithMinMax(minValue, maxValue);

        LogicalPlan localPlan = localPlan(plan, searchStats);
        Project project = as(localPlan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval eval = as(topN.child(), Eval.class);
        List<Alias> fields = eval.fields();
        assertEquals(1, fields.size());
        Alias a = fields.get(0);
        assertEquals("x", a.name());
        RoundTo roundTo = as(a.child(), RoundTo.class);
        FieldAttribute fa = as(roundTo.field(), FieldAttribute.class);
        assertEquals("hire_date", fa.name());
        assertEquals(DATETIME, fa.dataType());
        assertEquals(4, roundTo.points().size()); // 4 days
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

    public void testSubstituteDateTruncInAggWithRoundTo() {
        var plan = plan("""
              from test
              | stats count(*) by x = date_trunc(1 day, hire_date)
            """);

        // create a SearchStats with min and max millis
        Map<String, Object> minValue = Map.of("hire_date", 1697804103360L); // 2023-10-20T12:15:03.360Z
        Map<String, Object> maxValue = Map.of("hire_date", 1698069301543L); // 2023-10-23T13:55:01.543Z
        SearchStats searchStats = new EsqlTestUtils.TestSearchStatsWithMinMax(minValue, maxValue);

        LogicalPlan localPlan = localPlan(plan, searchStats);
        Limit limit = as(localPlan, Limit.class);
        Aggregate aggregate = as(limit.child(), Aggregate.class);
        Eval eval = as(aggregate.child(), Eval.class);
        List<Alias> fields = eval.fields();
        assertEquals(1, fields.size());
        Alias a = fields.get(0);
        assertEquals("x", a.name());
        RoundTo roundTo = as(a.child(), RoundTo.class);
        FieldAttribute fa = as(roundTo.field(), FieldAttribute.class);
        assertEquals("hire_date", fa.name());
        assertEquals(DATETIME, fa.dataType());
        assertEquals(4, roundTo.points().size()); // 4 days
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

    public void testSubstituteBucketInAggWithRoundTo() {
        var plan = plan("""
              from test
              | stats count(*) by x = bucket(hire_date, 1 day)
            """);
        // create a SearchStats with min and max millis
        Map<String, Object> minValue = Map.of("hire_date", 1697804103360L); // 2023-10-20T12:15:03.360Z
        Map<String, Object> maxValue = Map.of("hire_date", 1698069301543L); // 2023-10-23T13:55:01.543Z
        SearchStats searchStats = new EsqlTestUtils.TestSearchStatsWithMinMax(minValue, maxValue);

        LogicalPlan localPlan = localPlan(plan, searchStats);
        Limit limit = as(localPlan, Limit.class);
        Aggregate aggregate = as(limit.child(), Aggregate.class);
        Eval eval = as(aggregate.child(), Eval.class);
        List<Alias> fields = eval.fields();
        assertEquals(1, fields.size());
        Alias a = fields.get(0);
        assertEquals("x", a.name());
        RoundTo roundTo = as(a.child(), RoundTo.class);
        FieldAttribute fa = as(roundTo.field(), FieldAttribute.class);
        assertEquals("hire_date", fa.name());
        assertEquals(DATETIME, fa.dataType());
        assertEquals(4, roundTo.points().size()); // 4 days
        EsRelation relation = as(eval.child(), EsRelation.class);
    }
}
