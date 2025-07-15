/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class LocalSubstituteSurrogateExpressionTests extends LocalLogicalPlanOptimizerTests {

    // Key is the predicate,
    // Value is the number of items in the round_to function, if the number of item is 0, that means the min/max in predicates do not
    // overlap with SearchStats, so the substitution does not happen.
    private static final Map<String, Integer> predicatesForDateTruncBucket = new HashMap<>(
        Map.ofEntries(
            Map.entry("", 4),
            Map.entry(" | where hire_date == \"2023-10-22\" ", 1),
            Map.entry(" | where hire_date == \"2023-10-19\" ", 0),
            Map.entry(" | where hire_date >= \"2023-10-20\" ", 4),
            Map.entry(" | where hire_date >= \"2023-10-22\" ", 2),
            Map.entry(" | where hire_date >  \"2023-10-24\" ", 0),
            Map.entry(" | where hire_date <  \"2023-10-24\" ", 4),
            Map.entry(" | where hire_date <= \"2023-10-22\" ", 3),
            Map.entry(" | where hire_date <= \"2023-10-19\" ", 0),
            Map.entry(" | where hire_date >= \"2023-10-20\" and hire_date <= \"2023-10-24\" ", 4),
            Map.entry(" | where hire_date >= \"2023-10-21\" and hire_date <= \"2023-10-23\" ", 3),
            Map.entry(" | where hire_date >= \"2023-10-24\" and hire_date <= \"2023-10-31\" ", 0)
        )
    );

    // The date range of SearchStats is from 2023-10-20 to 2023-10-23.
    private static final SearchStats searchStats = searchStats();

    public void testSubstituteDateTruncInEvalWithRoundTo() {
        for (Map.Entry<String, Integer> predicate : predicatesForDateTruncBucket.entrySet()) {
            String predicateString = predicate.getKey();
            int roundToPointsSize = predicate.getValue();
            String query = LoggerMessageFormat.format(null, """
                from test
                | sort hire_date
                | eval x = date_trunc(1 day, hire_date)
                | keep emp_no, hire_date, x
                {}
                | limit 5
                """, predicateString);
            LogicalPlan localPlan = localPlan(query, searchStats);
            Project project = as(localPlan, Project.class);
            TopN topN = as(project.child(), TopN.class);
            Eval eval = as(topN.child(), Eval.class);
            List<Alias> fields = eval.fields();
            assertEquals(1, fields.size());
            Alias a = fields.get(0);
            assertEquals("x", a.name());
            verifySubstitution(a, roundToPointsSize);
            LogicalPlan subPlan = predicateString.isEmpty() ? eval : eval.child();
            EsRelation relation = as(subPlan.children().get(0), EsRelation.class);
        }
    }

    public void testSubstituteDateTruncInAggWithRoundTo() {
        for (Map.Entry<String, Integer> predicate : predicatesForDateTruncBucket.entrySet()) {
            String predicateString = predicate.getKey();
            int roundToPointsSize = predicate.getValue();
            String query = LoggerMessageFormat.format(null, """
                from test
                {}
                | stats count(*) by x = date_trunc(1 day, hire_date)
                """, predicateString);
            LogicalPlan localPlan = localPlan(query, searchStats);
            Limit limit = as(localPlan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            Eval eval = as(aggregate.child(), Eval.class);
            List<Alias> fields = eval.fields();
            assertEquals(1, fields.size());
            Alias a = fields.get(0);
            assertEquals("x", a.name());
            verifySubstitution(a, roundToPointsSize);
            LogicalPlan subPlan = predicateString.isEmpty() ? eval : eval.child();
            EsRelation relation = as(subPlan.children().get(0), EsRelation.class);
        }
    }

    public void testSubstituteBucketInAggWithRoundTo() {
        for (Map.Entry<String, Integer> predicate : predicatesForDateTruncBucket.entrySet()) {
            String predicateString = predicate.getKey();
            int roundToPointsSize = predicate.getValue();
            String query = LoggerMessageFormat.format(null, """
                from test
                {}
                | stats count(*) by x = bucket(hire_date, 1 day)
                """, predicateString);
            LogicalPlan localPlan = localPlan(query, searchStats);
            Limit limit = as(localPlan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            Eval eval = as(aggregate.child(), Eval.class);
            List<Alias> fields = eval.fields();
            assertEquals(1, fields.size());
            Alias a = fields.get(0);
            assertEquals("x", a.name());
            verifySubstitution(a, roundToPointsSize);
            LogicalPlan subPlan = predicateString.isEmpty() ? eval : eval.child();
            EsRelation relation = as(subPlan.children().get(0), EsRelation.class);
        }
    }

    private void verifySubstitution(Alias a, int roundToPointsSize) {
        FieldAttribute fa = null;
        Expression e = a.child();
        if (roundToPointsSize > 0) {
            RoundTo roundTo = as(e, RoundTo.class);
            fa = as(roundTo.field(), FieldAttribute.class);
            assertEquals(roundToPointsSize, roundTo.points().size());
        } else {
            if (e instanceof DateTrunc dateTrunc) {
                fa = as(dateTrunc.field(), FieldAttribute.class);
            } else if (e instanceof Bucket bucket) {
                fa = as(bucket.field(), FieldAttribute.class);
            } else {
                fail(e.getClass() + " is not supported");
            }
        }
        assertEquals("hire_date", fa.name());
        assertEquals(DATETIME, fa.dataType());
    }

    private LogicalPlan localPlan(String query, SearchStats searchStats) {
        var plan = plan(query);
        return localPlan(plan, searchStats);
    }

    private static SearchStats searchStats() {
        // create a SearchStats with min and max millis
        Map<String, Object> minValue = Map.of("hire_date", 1697804103360L); // 2023-10-20T12:15:03.360Z
        Map<String, Object> maxValue = Map.of("hire_date", 1698069301543L); // 2023-10-23T13:55:01.543Z
        return new EsqlTestUtils.TestSearchStatsWithMinMax(minValue, maxValue);
    }
}
