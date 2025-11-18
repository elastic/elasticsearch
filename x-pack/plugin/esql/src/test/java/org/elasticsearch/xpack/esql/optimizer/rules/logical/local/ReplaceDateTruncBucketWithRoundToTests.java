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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
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
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class ReplaceDateTruncBucketWithRoundToTests extends LocalLogicalPlanOptimizerTests {

    // Key is the predicate,
    // Value is the number of items in the round_to function, if the number of item is 0, that means the min/max in predicates do not
    // overlap with SearchStats, so the substitution does not happen.
    private static final Map<String, Integer> predicatesWithDateTruncBucket = new HashMap<>(
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

    private static final Map<String, Integer> evalRenamePredicatesWithDateTruncBucket = new HashMap<>(
        Map.ofEntries(
            // ReplaceAliasingEvalWithProject replaces x with hire_date so that the DateTrunc can be transformed to RoundTo
            Map.entry(" | eval x = hire_date ", 4),
            // DateTrunc cannot be transformed to RoundTo if it references an expression
            Map.entry(" | eval x = hire_date + 1 year ", -1),
            // PushDownEval replaces the reference(x) in DateTrunc with the corresponding field hire_date
            Map.entry(" | rename hire_date as x ", 4),
            Map.entry(" | rename hire_date as a, a as x ", 4),
            Map.entry(" | rename hire_date as x, x as hire_date ", 4),
            Map.entry(" | eval a = hire_date | rename a as x ", 4),
            Map.entry(" | eval x = hire_date | where x >= \"2023-10-22\" ", 2),
            Map.entry(" | rename hire_date as x | where x >= \"2023-10-20\" ", 4),
            Map.entry(" | rename hire_date as a, a as x | where x <= \"2023-10-22\" ", 3),
            Map.entry(" | rename hire_date as x, x as hire_date | where hire_date >= \"2023-10-21\" and hire_date <= \"2023-10-23\" ", 3),
            Map.entry(" | eval a = hire_date | rename a as x | where x <= \"2023-10-22\" ", 3)
        )
    );

    // The date range of SearchStats is from 2023-10-20 to 2023-10-23.
    private static final SearchStats searchStats = searchStats();

    public void testSubstituteDateTruncInEvalWithRoundTo() {
        for (Map.Entry<String, Integer> predicate : predicatesWithDateTruncBucket.entrySet()) {
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
            Configuration configuration = TEST_CFG;
            LogicalPlan localPlan = localPlan(plan(query), configuration, searchStats);
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
        for (Map.Entry<String, Integer> predicate : predicatesWithDateTruncBucket.entrySet()) {
            String predicateString = predicate.getKey();
            int roundToPointsSize = predicate.getValue();
            String query = LoggerMessageFormat.format(null, """
                from test
                {}
                | stats count(*) by x = date_trunc(1 day, hire_date)
                """, predicateString);
            Configuration configuration = TEST_CFG;
            LogicalPlan localPlan = localPlan(plan(query), configuration, searchStats);
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
        for (Map.Entry<String, Integer> predicate : predicatesWithDateTruncBucket.entrySet()) {
            String predicateString = predicate.getKey();
            int roundToPointsSize = predicate.getValue();
            String query = LoggerMessageFormat.format(null, """
                from test
                {}
                | stats count(*) by x = bucket(hire_date, 1 day)
                """, predicateString);
            Configuration configuration = TEST_CFG;
            LogicalPlan localPlan = localPlan(plan(query), configuration, searchStats);
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

    public void testSubstituteDateTruncInEvalWithRoundToWithEvalRename() {
        for (Map.Entry<String, Integer> predicate : evalRenamePredicatesWithDateTruncBucket.entrySet()) {
            String predicateString = predicate.getKey();
            int roundToPointsSize = predicate.getValue();
            boolean hasWhere = predicateString.contains("where");
            boolean renameBack = predicateString.contains("rename hire_date as x, x as hire_date");
            boolean dateTruncOnExpression = predicateString.contains("hire_date + 1 year");
            String fieldName = renameBack ? "hire_date" : "x";
            String query = LoggerMessageFormat.format(null, """
                from test
                | sort hire_date
                {}
                | eval y = date_trunc(1 day, {})
                | keep emp_no, {}, y
                | limit 5
                """, predicateString, fieldName, fieldName);
            Configuration configuration = TEST_CFG;
            LogicalPlan localPlan = localPlan(plan(query), configuration, searchStats);
            Project project = as(localPlan, Project.class);
            TopN topN = as(project.child(), TopN.class);
            Eval eval = as(topN.child(), Eval.class);
            List<Alias> fields = eval.fields();
            assertEquals(dateTruncOnExpression ? 2 : 1, fields.size());
            Alias a = fields.get(dateTruncOnExpression ? 1 : 0);
            assertEquals("y", a.name());
            verifySubstitution(a, roundToPointsSize);
            LogicalPlan subPlan = hasWhere ? eval.child() : eval;
            EsRelation relation = as(subPlan.children().get(0), EsRelation.class);
        }
    }

    public void testSubstituteBucketInAggWithRoundToWithEvalRename() {
        for (Map.Entry<String, Integer> predicate : evalRenamePredicatesWithDateTruncBucket.entrySet()) {
            String predicateString = predicate.getKey();
            int roundToPointsSize = predicate.getValue();
            boolean hasWhere = predicateString.contains("where");
            boolean renameBack = predicateString.contains("rename hire_date as x, x as hire_date");
            boolean dateTruncOnExpression = predicateString.contains("hire_date + 1 year");
            String fieldName = renameBack ? "hire_date" : "x";
            String query = LoggerMessageFormat.format(null, """
                from test
                {}
                | stats count(*) by y = bucket({}, 1 day)
                """, predicateString, fieldName);
            Configuration configuration = TEST_CFG;
            LogicalPlan localPlan = localPlan(plan(query), configuration, searchStats);
            Limit limit = as(localPlan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            Eval eval = as(aggregate.child(), Eval.class);
            List<Alias> fields = eval.fields();
            assertEquals(dateTruncOnExpression ? 2 : 1, fields.size());
            Alias a = fields.get(dateTruncOnExpression ? 1 : 0);
            assertEquals("y", a.name());
            verifySubstitution(a, roundToPointsSize);
            LogicalPlan subPlan = hasWhere ? eval.child() : eval;
            EsRelation relation = as(subPlan.children().get(0), EsRelation.class);
        }
    }

    public void testSubstituteDateTruncInAggWithRoundToWithEvalRename() {
        for (Map.Entry<String, Integer> predicate : evalRenamePredicatesWithDateTruncBucket.entrySet()) {
            String predicateString = predicate.getKey();
            int roundToPointsSize = predicate.getValue();
            boolean hasWhere = predicateString.contains("where");
            boolean renameBack = predicateString.contains("rename hire_date as x, x as hire_date");
            boolean dateTruncOnExpression = predicateString.contains("hire_date + 1 year");
            String fieldName = renameBack ? "hire_date" : "x";
            String query = LoggerMessageFormat.format(null, """
                from test
                {}
                | stats count(*) by y = date_trunc(1 day, {})
                """, predicateString, fieldName);
            Configuration configuration = TEST_CFG;
            LogicalPlan localPlan = localPlan(plan(query), configuration, searchStats);
            Limit limit = as(localPlan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            Eval eval = as(aggregate.child(), Eval.class);
            List<Alias> fields = eval.fields();
            assertEquals(dateTruncOnExpression ? 2 : 1, fields.size());
            Alias a = fields.get(dateTruncOnExpression ? 1 : 0);
            assertEquals("y", a.name());
            verifySubstitution(a, roundToPointsSize);
            LogicalPlan subPlan = hasWhere ? eval.child() : eval;
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
        } else if (roundToPointsSize == 0) {
            if (e instanceof DateTrunc dateTrunc) {
                fa = as(dateTrunc.field(), FieldAttribute.class);
            } else if (e instanceof Bucket bucket) {
                fa = as(bucket.field(), FieldAttribute.class);
            } else {
                fail(e.getClass() + " is not supported");
            }
        } else {
            if (e instanceof DateTrunc dateTrunc) {
                assertTrue(dateTrunc.field() instanceof ReferenceAttribute);
            } else if (e instanceof Bucket bucket) {
                assertTrue(bucket.field() instanceof ReferenceAttribute);
            } else {
                fail(e.getClass() + " is not supported");
            }
        }
        if (roundToPointsSize >= 0) {
            assertEquals("hire_date", fa.name());
            assertEquals(DATETIME, fa.dataType());
        }
    }

    private static SearchStats searchStats() {
        // create a SearchStats with min and max millis
        Map<String, Object> minValue = Map.of("hire_date", 1697804103360L); // 2023-10-20T12:15:03.360Z
        Map<String, Object> maxValue = Map.of("hire_date", 1698069301543L); // 2023-10-23T13:55:01.543Z
        return new EsqlTestUtils.TestSearchStatsWithMinMax(minValue, maxValue);
    }
}
