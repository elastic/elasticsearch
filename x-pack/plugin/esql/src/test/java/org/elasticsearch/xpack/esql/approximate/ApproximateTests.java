/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximate;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanPreOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class ApproximateTests extends ESTestCase {

    private static final EsqlParser parser = EsqlParser.INSTANCE;
    private static final LogicalPlanPreOptimizer logicalPlanPreOptimizer = new LogicalPlanPreOptimizer(
        new LogicalPreOptimizerContext(FoldContext.small(), mock(InferenceService.class), TransportVersion.current())
    );

    public void testVerify_validQuery() throws Exception {
        verifyQuery("FROM index | EVAL x=1 | WHERE y<0.1 | SORT z | SAMPLE 0.1 | STATS COUNT() BY y | SORT z | MV_EXPAND x");
    }

    public void testVerify_noStats() {
        assertError("FROM index | EVAL x = 1 | SORT y", equalTo("line 1:1: query without [STATS] cannot be approximated"));
    }

    public void testVerify_incompatibleCommand() {
        assertError(
            "FROM index | FORK (EVAL x=1) (EVAL y=1) | STATS COUNT()",
            equalTo("line 1:14: query with [FORK (EVAL x=1) (EVAL y=1)] cannot be approximated")
        );
        assertError(
            "FROM index | STATS COUNT() | FORK (EVAL x=1) (EVAL y=1)",
            equalTo("line 1:30: query with [FORK (EVAL x=1) (EVAL y=1)] cannot be approximated")
        );

        assertError(
            "FROM index | INLINE STATS COUNT() | STATS COUNT()",
            equalTo("line 1:14: query with [INLINE STATS COUNT()] cannot be approximated")
        );
        assertError(
            "FROM index | STATS COUNT() | INLINE STATS COUNT()",
            equalTo("line 1:30: query with [INLINE STATS COUNT()] cannot be approximated")
        );

        assertError(
            "FROM index | LOOKUP JOIN index2 ON id | STATS COUNT()",
            equalTo("line 1:14: query with [LOOKUP JOIN index2 ON id] cannot be approximated")
        );
        assertError(
            "FROM index | STATS COUNT() | LOOKUP JOIN index2 ON id",
            equalTo("line 1:30: query with [LOOKUP JOIN index2 ON id] cannot be approximated")
        );
    }

    public void testVerify_nonSwappableCommand() {
        assertError(
            "FROM index | MV_EXPAND x | STATS COUNT()",
            equalTo("line 1:14: query with [MV_EXPAND x] before [STATS] cannot be approximated")
        );
        assertError(
            "FROM index | CHANGE_POINT x ON t | STATS COUNT()",
            equalTo("line 1:14: query with [CHANGE_POINT x ON t] before [STATS] cannot be approximated")
        );
        assertError(
            "FROM index | LIMIT 100 | STATS COUNT()",
            equalTo("line 1:14: query with [LIMIT 100] before [STATS] cannot be approximated")
        );
    }

    private void assertError(String esql, Matcher<String> matcher) {
        Exception e = assertThrows(VerificationException.class, () -> verifyQuery(esql));
        assertThat(e.getMessage().substring("Found 1 problem\n".length()), matcher);
    }

    private void verifyQuery(String esql) throws Exception {
        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        LogicalPlan plan = parser.createStatement(esql, new QueryParams()).plan();
        plan.setAnalyzed();
        logicalPlanPreOptimizer.preOptimize(plan, ActionListener.wrap(resultHolder::set, exceptionHolder::set));
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }
        new Approximate(resultHolder.get());
    }
}
