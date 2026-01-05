/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomMinimumVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class LogicalPlanPreOptimizerTests extends ESTestCase {

    public void testPlanIsMarkedAsPreOptimized() throws Exception {
        for (int round = 0; round < 100; round++) {
            // We want to make sure that the pre-optimizer woks for a wide range of plans
            preOptimizedPlan(randomPlan());
        }
    }

    public void testPreOptimizeFailsIfPlanIsNotAnalyzed() throws Exception {
        LogicalPlan plan = EsqlTestUtils.relation();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();

        preOptimizer().preOptimize(plan, ActionListener.wrap(r -> fail("Should have failed"), exceptionHolder::set));
        assertBusy(() -> {
            assertThat(exceptionHolder.get(), notNullValue());
            IllegalStateException e = as(exceptionHolder.get(), IllegalStateException.class);
            assertThat(e.getMessage(), equalTo("Expected analyzed plan"));
        });
    }

    public LogicalPlan preOptimizedPlan(LogicalPlan plan) throws Exception {
        // set plan as analyzed
        plan.setPreOptimized();

        SetOnce<LogicalPlan> resultHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();

        preOptimizer().preOptimize(plan, ActionListener.wrap(resultHolder::set, exceptionHolder::set));

        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        assertThat(resultHolder.get(), notNullValue());
        assertThat(resultHolder.get().preOptimized(), equalTo(true));

        return resultHolder.get();
    }

    private LogicalPlanPreOptimizer preOptimizer() {
        var inferenceService = mock(org.elasticsearch.xpack.esql.inference.InferenceService.class);
        LogicalPreOptimizerContext preOptimizerContext = new LogicalPreOptimizerContext(
            FoldContext.small(),
            inferenceService,
            randomMinimumVersion()
        );
        return new LogicalPlanPreOptimizer(preOptimizerContext);
    }

    private LogicalPlan randomPlan() {
        LogicalPlan plan = EsqlTestUtils.relation();
        int numCommands = between(0, 100);

        for (int i = 0; i < numCommands; i++) {
            plan = switch (randomInt(3)) {
                case 0 -> new Eval(Source.EMPTY, plan, List.of(new Alias(Source.EMPTY, randomIdentifier(), randomExpression())));
                case 1 -> new Limit(Source.EMPTY, of(randomInt()), plan);
                case 2 -> new Filter(Source.EMPTY, plan, randomCondition());
                default -> new Project(Source.EMPTY, plan, List.of(new Alias(Source.EMPTY, randomIdentifier(), fieldAttribute())));
            };
        }
        return plan;
    }

    private Expression randomExpression() {
        return switch (randomInt(3)) {
            case 0 -> of(randomInt());
            case 1 -> of(randomIdentifier());
            case 2 -> new Add(Source.EMPTY, of(randomInt()), of(randomDouble()));
            default -> new Concat(Source.EMPTY, of(randomIdentifier()), randomList(1, 10, () -> of(randomIdentifier())));
        };
    }

    private Expression randomCondition() {
        if (randomBoolean()) {
            return EsqlTestUtils.equalsOf(randomExpression(), randomExpression());
        }

        return EsqlTestUtils.greaterThanOf(randomExpression(), randomExpression());
    }
}
