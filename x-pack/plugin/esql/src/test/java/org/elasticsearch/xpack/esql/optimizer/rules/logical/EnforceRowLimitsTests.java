/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class EnforceRowLimitsTests extends ESTestCase {

    private final EnforceRowLimits rule = new EnforceRowLimits();

    @Override
    protected boolean enableWarningsCheck() {
        // We manually manage ThreadContext and warnings in these tests
        return false;
    }

    private EsRelation createEsRelation() {
        return new EsRelation(EMPTY, "test", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
    }

    private Completion createCompletion(LogicalPlan child, Expression rowLimit) {
        Source source = EMPTY;
        Expression prompt = Literal.keyword(source, "test prompt");
        Attribute targetField = new ReferenceAttribute(source, "completion", DataType.KEYWORD);
        Expression inferenceId = Literal.keyword(source, "test-inference-id");
        return new Completion(source, child, inferenceId, prompt, targetField, rowLimit);
    }

    /**
     * Tests: | EsRelation | COMPLETION(rowLimit=100) → | EsRelation | LIMIT 100 | COMPLETION
     */
    public void testEnforceLimitOnCompletionWithoutLimit() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation child = createEsRelation();
            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(child, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify limit is applied to child
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit limit = as(newChild, Limit.class);
            assertThat(((Literal) limit.limit()).value(), equalTo(100));

            // Verify warning
            List<String> warningHeaders = threadContext.getResponseHeaders().get("Warning");
            assertNotNull("Expected warning header", warningHeaders);
            assertThat(warningHeaders.size(), equalTo(1));

            String warningValue = HeaderWarning.extractWarningValueFromWarningHeader(warningHeaders.get(0), false);
            assertThat(warningValue, containsString("No limit defined, adding default limit of [100]"));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | LIMIT 50 | COMPLETION(rowLimit=100) → | LIMIT 50 | COMPLETION (no change)
     */
    public void testPreservesLowerLimit() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation relation = createEsRelation();
            Limit existingLimit = new Limit(EMPTY, Literal.integer(EMPTY, 50), relation);
            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(existingLimit, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify lower limit is preserved
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit limit = as(newChild, Limit.class);
            assertThat(((Literal) limit.limit()).value(), equalTo(50));

            // Should NOT have warning when limit is preserved (no change made)
            List<String> warningHeaders = threadContext.getResponseHeaders().get("Warning");
            if (warningHeaders != null) {
                for (String warningHeader : warningHeaders) {
                    String warningValue = HeaderWarning.extractWarningValueFromWarningHeader(warningHeader, false);
                    assertThat(warningValue, not(containsString("No limit defined")));
                    assertThat(warningValue, not(containsString("Limit adjusted")));
                }
            }
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | LIMIT 500 | COMPLETION(rowLimit=100) → | LIMIT 100 | COMPLETION
     */
    public void testReducesHigherLimit() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation relation = createEsRelation();
            Limit existingLimit = new Limit(EMPTY, Literal.integer(EMPTY, 500), relation);
            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(existingLimit, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify limit is reduced to rowLimit
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit limit = as(newChild, Limit.class);
            assertThat(((Literal) limit.limit()).value(), equalTo(100));

            // Verify warning about limit adjustment
            List<String> warningHeaders = threadContext.getResponseHeaders().get("Warning");
            assertNotNull("Expected warning header", warningHeaders);
            assertThat(warningHeaders.size(), equalTo(1));

            String warningValue = HeaderWarning.extractWarningValueFromWarningHeader(warningHeaders.get(0), false);
            assertThat(warningValue, containsString("Limit adjusted to [100] to enforce row limit"));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | EVAL | COMPLETION(rowLimit=100) → | EVAL | LIMIT 100 | COMPLETION
     * Streaming plans push the limit down to their child.
     */
    public void testEnforcesLimitOnStreamingPlan() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation relation = createEsRelation();
            Eval eval = new Eval(EMPTY, relation, List.of());
            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(eval, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify limit is applied to streaming plan (Eval)
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Eval.class));
            Eval newEval = as(newChild, Eval.class);
            assertThat(newEval.child(), instanceOf(Limit.class));
            Limit limit = as(newEval.child(), Limit.class);
            assertThat(((Literal) limit.limit()).value(), equalTo(100));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | LIMIT 100 | COMPLETION(rowLimit=100) → | LIMIT 100 | COMPLETION (no change)
     */
    public void testWithExactLimit() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation relation = createEsRelation();
            Limit existingLimit = new Limit(EMPTY, Literal.integer(EMPTY, 100), relation);
            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(existingLimit, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify exact limit is preserved
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit limit = as(newChild, Limit.class);
            assertThat(((Literal) limit.limit()).value(), equalTo(100));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | EsRelation | COMPLETION(rowLimit=250) → | EsRelation | LIMIT 250 | COMPLETION
     */
    public void testWithCustomRowLimit() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation child = createEsRelation();
            Expression rowLimit = Literal.integer(EMPTY, 250);
            Completion completion = createCompletion(child, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify custom limit is applied
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit limit = as(newChild, Limit.class);
            assertThat(((Literal) limit.limit()).value(), equalTo(250));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | STATS | COMPLETION(rowLimit=100) → | STATS | LIMIT 100 | COMPLETION
     */
    public void testEnforceLimitWithStats() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation relation = createEsRelation();
            // Create a STATS plan: STATS count = count(*) BY field
            Attribute countAttr = new ReferenceAttribute(EMPTY, "count", DataType.LONG);
            Attribute fieldAttr = new ReferenceAttribute(EMPTY, "field", DataType.KEYWORD);
            List<Expression> groupings = List.of(fieldAttr);
            List<NamedExpression> aggregates = List.of(countAttr);
            Aggregate stats = new Aggregate(EMPTY, relation, groupings, aggregates);

            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(stats, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify limit is added AFTER stats (between stats and completion)
            // Structure should be: Completion -> Limit(100) -> Aggregate -> EsRelation
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit limit = as(newChild, Limit.class);
            assertThat(((Literal) limit.limit()).value(), equalTo(100));

            assertThat(limit.child(), instanceOf(Aggregate.class));
            Aggregate newStats = as(limit.child(), Aggregate.class);
            assertThat(newStats.child(), instanceOf(EsRelation.class));

            // Verify warning
            List<String> warningHeaders = threadContext.getResponseHeaders().get("Warning");
            assertNotNull("Expected warning header", warningHeaders);
            assertThat(warningHeaders.size(), equalTo(1));

            String warningValue = HeaderWarning.extractWarningValueFromWarningHeader(warningHeaders.get(0), false);
            assertThat(warningValue, containsString("No limit defined, adding default limit of [100]"));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | LIMIT 500 | STATS | COMPLETION(rowLimit=100) → | LIMIT 500 | STATS | LIMIT 100 | COMPLETION
     */
    public void testReducesLimitWithStats() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation relation = createEsRelation();
            Limit existingLimit = new Limit(EMPTY, Literal.integer(EMPTY, 500), relation);

            // Create a STATS plan with existing limit
            Attribute countAttr = new ReferenceAttribute(EMPTY, "count", DataType.LONG);
            Attribute fieldAttr = new ReferenceAttribute(EMPTY, "field", DataType.KEYWORD);
            List<Expression> groupings = List.of(fieldAttr);
            List<NamedExpression> aggregates = List.of(countAttr);
            Aggregate stats = new Aggregate(EMPTY, existingLimit, groupings, aggregates);

            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(stats, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify new limit is added AFTER stats while preserving the existing limit BEFORE stats
            // Structure should be: Completion -> Limit(100) -> Aggregate -> Limit(500) -> EsRelation
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit newLimit = as(newChild, Limit.class);
            assertThat(((Literal) newLimit.limit()).value(), equalTo(100));

            assertThat(newLimit.child(), instanceOf(Aggregate.class));
            Aggregate newStats = as(newLimit.child(), Aggregate.class);
            assertThat(newStats.child(), instanceOf(Limit.class));
            Limit existingLimitResult = as(newStats.child(), Limit.class);
            assertThat(((Literal) existingLimitResult.limit()).value(), equalTo(500));

            // Verify warning about adding limit
            List<String> warningHeaders = threadContext.getResponseHeaders().get("Warning");
            assertNotNull("Expected warning header", warningHeaders);
            assertThat(warningHeaders.size(), equalTo(1));

            String warningValue = HeaderWarning.extractWarningValueFromWarningHeader(warningHeaders.get(0), false);
            assertThat(warningValue, containsString("No limit defined, adding default limit of [100]"));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | MV_EXPAND | COMPLETION(rowLimit=100) → | MV_EXPAND | LIMIT 100 | COMPLETION
     */
    public void testEnforceLimitWithMvExpand() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation relation = createEsRelation();
            // Create a MV_EXPAND plan
            Attribute targetAttr = new ReferenceAttribute(EMPTY, "tags", DataType.KEYWORD);
            Attribute expandedAttr = new ReferenceAttribute(EMPTY, "tags", DataType.KEYWORD);
            MvExpand mvExpand = new MvExpand(EMPTY, relation, targetAttr, expandedAttr);

            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(mvExpand, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify limit is added AFTER mvexpand (between mvexpand and completion)
            // Structure should be: Completion -> Limit(100) -> MvExpand -> EsRelation
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit limit = as(newChild, Limit.class);
            assertThat(((Literal) limit.limit()).value(), equalTo(100));

            assertThat(limit.child(), instanceOf(MvExpand.class));
            MvExpand newMvExpand = as(limit.child(), MvExpand.class);
            assertThat(newMvExpand.child(), instanceOf(EsRelation.class));

            // Verify warning
            List<String> warningHeaders = threadContext.getResponseHeaders().get("Warning");
            assertNotNull("Expected warning header", warningHeaders);
            assertThat(warningHeaders.size(), equalTo(1));

            String warningValue = HeaderWarning.extractWarningValueFromWarningHeader(warningHeaders.get(0), false);
            assertThat(warningValue, containsString("No limit defined, adding default limit of [100]"));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Tests: | LIMIT 50 | MV_EXPAND | COMPLETION(rowLimit=100) → | LIMIT 50 | MV_EXPAND | LIMIT 100 | COMPLETION
     */
    public void testPreservesLowerLimitWithMvExpand() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            EsRelation relation = createEsRelation();
            Limit existingLimit = new Limit(EMPTY, Literal.integer(EMPTY, 50), relation);

            // Create a MV_EXPAND plan with existing limit
            Attribute targetAttr = new ReferenceAttribute(EMPTY, "tags", DataType.KEYWORD);
            Attribute expandedAttr = new ReferenceAttribute(EMPTY, "tags", DataType.KEYWORD);
            MvExpand mvExpand = new MvExpand(EMPTY, existingLimit, targetAttr, expandedAttr);

            Expression rowLimit = Literal.integer(EMPTY, 100);
            Completion completion = createCompletion(mvExpand, rowLimit);

            LogicalPlan result = rule.rule(completion);
            Completion resultCompletion = as(result, Completion.class);

            // Verify new limit is added AFTER mvexpand while preserving the existing limit BEFORE mvexpand
            // Structure should be: Completion -> Limit(100) -> MvExpand -> Limit(50) -> EsRelation
            LogicalPlan newChild = resultCompletion.child();
            assertThat(newChild, instanceOf(Limit.class));
            Limit newLimit = as(newChild, Limit.class);
            assertThat(((Literal) newLimit.limit()).value(), equalTo(100));

            assertThat(newLimit.child(), instanceOf(MvExpand.class));
            MvExpand newMvExpand = as(newLimit.child(), MvExpand.class);
            assertThat(newMvExpand.child(), instanceOf(Limit.class));
            Limit existingLimitResult = as(newMvExpand.child(), Limit.class);
            assertThat(((Literal) existingLimitResult.limit()).value(), equalTo(50));

            // Should have warning since a new limit is being added
            List<String> warningHeaders = threadContext.getResponseHeaders().get("Warning");
            assertNotNull("Expected warning header", warningHeaders);
            assertThat(warningHeaders.size(), equalTo(1));

            String warningValue = HeaderWarning.extractWarningValueFromWarningHeader(warningHeaders.get(0), false);
            assertThat(warningValue, containsString("No limit defined, adding default limit of [100]"));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }
}
