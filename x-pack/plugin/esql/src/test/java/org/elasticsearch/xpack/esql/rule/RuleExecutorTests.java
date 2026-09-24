/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.CombineEvals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.CombineProjections;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneEmptyPlans;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;

public class RuleExecutorTests extends ESTestCase {

    /**
     * Each pass of the batch runs these rules in order on the {@link #plan()}:
     * <pre>
     * start of pass              Project[x, y]
     *                            \_Eval[x = 1, y = 2]
     *
     * SplitEvals          --&gt;    Project[x, y]
     *                            \_Eval[y = 2]
     *                              \_Eval[x = 1]
     *
     * CombineEvals        --&gt;    Project[x, y]
     *                            \_Eval[x = 1, y = 2]
     *
     * PruneEmptyPlans     --&gt;    (no change, the plan has no empty relation)
     *
     * SplitProjects       --&gt;    Project[x, y]
     *                            \_Project[x, y]
     *                              \_Eval[x = 1, y = 2]
     *
     * CombineProjections  --&gt;    Project[x, y]
     *                            \_Eval[x = 1, y = 2]
     * </pre>
     * The pass ends on the plan it started with, but four rules changed it along the way, so the executor sees a changed plan
     * and runs another identical pass until the limit of 100 passes. Only the four plan-changing rules are reported, and the last
     * ten of them end with the tail of the final pass: SplitProjects, CombineProjections.
     */
    public void testLimitReachedReportsLastAppliedRules() {
        TestExecutor executor = new TestExecutor(
            new RuleExecutor.Batch<>(
                "never converges",
                new SplitEvals(),
                new CombineEvals(),
                new PruneEmptyPlans(),
                new SplitProjects(),
                new CombineProjections()
            )
        );

        RuleExecutionException e = expectThrows(RuleExecutionException.class, () -> executor.execute(plan()));
        assertThat(
            e.getMessage(),
            equalTo(
                "Rule execution limit [100] reached. Last Rules: ["
                    + "SplitProjects, logical.CombineProjections, "
                    + "SplitEvals, logical.CombineEvals, "
                    + "SplitProjects, logical.CombineProjections, "
                    + "SplitEvals, logical.CombineEvals, "
                    + "SplitProjects, logical.CombineProjections]"
            )
        );
    }

    /**
     * The same SplitEvals / CombineEvals loop as {@link #testLimitReachedReportsLastAppliedRules}, stopped after two passes, so only
     * four rule applications happened and all of them are reported.
     */
    public void testLimitReachedReportsFewerThanTenAppliedRules() {
        TestExecutor executor = new TestExecutor(
            new RuleExecutor.Batch<>(
                "never converges",
                new RuleExecutor.Limiter(2),
                new SplitEvals(),
                new PruneEmptyPlans(),
                new CombineEvals()
            )
        );

        RuleExecutionException e = expectThrows(RuleExecutionException.class, () -> executor.execute(plan()));
        assertThat(
            e.getMessage(),
            equalTo(
                "Rule execution limit [2] reached. Last Rules: ["
                    + "SplitEvals, logical.CombineEvals, "
                    + "SplitEvals, logical.CombineEvals]"
            )
        );
    }

    /**
     * Without the splitting rules nothing undoes CombineEvals: the first pass merges the pre-split {@code Eval}s back into the
     * {@link #plan()}, the second pass changes nothing, and the batch stops.
     */
    public void testConvergingBatchDoesNotThrow() {
        LogicalPlan plan = plan();
        TestExecutor executor = new TestExecutor(new RuleExecutor.Batch<>("converges", new CombineEvals(), new CombineProjections()));
        assertThat(executor.execute(new SplitEvals().apply(plan)), equalTo(plan));
    }

    private static LogicalPlan plan() {
        Eval eval = new Eval(EMPTY, relation(), List.of(alias("x", ONE), alias("y", TWO)));
        return new Project(EMPTY, eval, eval.output());
    }

    private static class TestExecutor extends RuleExecutor<LogicalPlan> {
        private final Batch<LogicalPlan> batch;

        TestExecutor(Batch<LogicalPlan> batch) {
            this.batch = batch;
        }

        @Override
        protected Iterable<Batch<LogicalPlan>> batches() {
            return List.of(batch);
        }
    }

    /**
     * Splits the first field of a multi-field {@link Eval} into its own {@link Eval} below the rest, the inverse of
     * {@link CombineEvals}.
     */
    private static class SplitEvals extends Rule<LogicalPlan, LogicalPlan> {
        SplitEvals() {
            super("SplitEvals");
        }

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.transformDown(Eval.class, eval -> {
                List<Alias> fields = eval.fields();
                if (fields.size() < 2) {
                    return eval;
                }
                Eval first = new Eval(eval.source(), eval.child(), fields.subList(0, 1));
                return new Eval(eval.source(), first, fields.subList(1, fields.size()));
            });
        }
    }

    /**
     * Wraps a root {@link Project} in an identity {@link Project}, the inverse of {@link CombineProjections}. Only the root is
     * wrapped, and not a {@link Project} that already sits on another one, so a single application adds exactly one layer.
     */
    private static class SplitProjects extends Rule<LogicalPlan, LogicalPlan> {
        SplitProjects() {
            super("SplitProjects");
        }

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            if (plan instanceof Project project && project.child() instanceof Project == false) {
                return new Project(project.source(), project, project.output());
            }
            return plan;
        }
    }
}
