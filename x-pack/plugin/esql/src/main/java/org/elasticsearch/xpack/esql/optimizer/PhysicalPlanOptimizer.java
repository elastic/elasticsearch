/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

@Experimental
public class PhysicalPlanOptimizer extends RuleExecutor<PhysicalPlan> {

    private static Setting<Boolean> ADD_TASK_PARALLELISM_ABOVE_QUERY = Setting.boolSetting("add_task_parallelism_above_query", false);

    private final EsqlConfiguration configuration;

    public PhysicalPlanOptimizer(EsqlConfiguration configuration) {
        this.configuration = configuration;
    }

    public PhysicalPlan optimize(PhysicalPlan plan) {
        plan = execute(plan);
        // ensure we always have single node at the end
        if (plan.singleNode() == false) {
            return new ExchangeExec(plan.source(), plan, ExchangeExec.Type.GATHER, ExchangeExec.Partitioning.SINGLE_DISTRIBUTION);
        }
        return plan;
    }

    @Override
    protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
        List<Batch> batches = new ArrayList<>();
        batches.add(new Batch("Create topN", Limiter.ONCE, new CreateTopN()));
        batches.add(new Batch("Split nodes", Limiter.ONCE, new SplitAggregate(), new SplitTopN()));
        batches.add(new Batch("Add exchange", Limiter.ONCE, new AddExchangeOnSingleNodeSplit()));
        // TODO: Needs another project at the end - depends on https://github.com/elastic/elasticsearch-internal/issues/293
        Batch fieldExtract = new Batch("Lazy field loading", Limiter.ONCE, new AddFieldExtraction());
        batches.add(fieldExtract);

        // TODO: add rule to prune _doc_id, _segment_id, _shard_id at the top
        // Batch addProject = new Batch("Add project", new AddProjectWhenInternalFieldNoLongerNeeded());
        if (ADD_TASK_PARALLELISM_ABOVE_QUERY.get(configuration.pragmas())) {
            batches.add(new Batch("Add task parallelization above query", new AddTaskParallelismAboveQuery()));
        }

        return batches;
    }

    static class AddFieldExtraction extends OptimizerRule<UnaryExec> {

        // start from the source upwards
        AddFieldExtraction() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected PhysicalPlan rule(UnaryExec plan) {
            // Exchange simply breaks down things so ignore it
            if (plan instanceof ExchangeExec || plan.child() instanceof ExchangeExec) {
                return plan;
            }

            // 1. add the extractors before each node that requires extra columns
            var lastNodeWithExtraction = new Holder<PhysicalPlan>();

            var missing = new LinkedHashSet<Attribute>();
            var input = plan.inputSet();

            // collect field attributes used inside the expressions
            plan.forEachExpression(FieldAttribute.class, f -> {
                if (input.contains(f) == false) {
                    missing.add(f);
                }
            });

            // ignore exchanges
            if (missing.isEmpty() == false) {
                plan = plan.replaceChild(new FieldExtractExec(plan.source(), plan.child(), missing));
            }

            return plan;
        }
    }

    private static class SplitAggregate extends OptimizerRule<AggregateExec> {

        @Override
        protected PhysicalPlan rule(AggregateExec aggregateExec) {
            if (aggregateExec.getMode() == AggregateExec.Mode.SINGLE) {
                return new AggregateExec(
                    aggregateExec.source(),
                    new AggregateExec(
                        aggregateExec.source(),
                        aggregateExec.child(),
                        aggregateExec.groupings(),
                        aggregateExec.aggregates(),
                        AggregateExec.Mode.PARTIAL
                    ),
                    aggregateExec.groupings(),
                    aggregateExec.aggregates(),
                    AggregateExec.Mode.FINAL
                );
            }
            return aggregateExec;
        }
    }

    private static class SplitTopN extends OptimizerRule<TopNExec> {

        @Override
        protected PhysicalPlan rule(TopNExec topNExec) {
            if (topNExec.getMode() == TopNExec.Mode.SINGLE) {
                return new TopNExec(
                    topNExec.source(),
                    new TopNExec(topNExec.source(), topNExec.child(), topNExec.order(), topNExec.getLimit(), TopNExec.Mode.PARTIAL),
                    topNExec.order(),
                    topNExec.getLimit(),
                    TopNExec.Mode.FINAL
                );
            }
            return topNExec;
        }
    }

    private static class AddExchangeOnSingleNodeSplit extends OptimizerRule<UnaryExec> {

        @Override
        protected PhysicalPlan rule(UnaryExec parent) {
            if (parent.singleNode() && parent.child().singleNode() == false) {
                if (parent instanceof ExchangeExec exchangeExec
                    && exchangeExec.getType() == ExchangeExec.Type.GATHER
                    && exchangeExec.getPartitioning() == ExchangeExec.Partitioning.SINGLE_DISTRIBUTION) {
                    return parent;
                }
                return parent.replaceChild(
                    new ExchangeExec(
                        parent.source(),
                        parent.child(),
                        ExchangeExec.Type.GATHER,
                        ExchangeExec.Partitioning.SINGLE_DISTRIBUTION
                    )
                );
            }
            return parent;
        }
    }

    private static class CreateTopN extends OptimizerRule<LimitExec> {

        @Override
        protected PhysicalPlan rule(LimitExec limitExec) {
            if (limitExec.child()instanceof OrderExec orderExec) {
                return new TopNExec(limitExec.source(), orderExec.child(), orderExec.order(), limitExec.limit());
            }
            return limitExec;
        }
    }

    private static class AddTaskParallelismAboveQuery extends OptimizerRule<UnaryExec> {

        @Override
        protected PhysicalPlan rule(UnaryExec plan) {
            if (plan instanceof ExchangeExec == false && plan.child()instanceof EsQueryExec esQueryExec) {
                return plan.replaceChild(
                    new ExchangeExec(
                        esQueryExec.source(),
                        esQueryExec,
                        ExchangeExec.Type.REPARTITION,
                        ExchangeExec.Partitioning.FIXED_ARBITRARY_DISTRIBUTION
                    )
                );
            }
            return plan;
        }
    }

    public abstract static class OptimizerRule<SubPlan extends PhysicalPlan> extends Rule<SubPlan, PhysicalPlan> {

        private final OptimizerRules.TransformDirection direction;

        public OptimizerRule() {
            this(OptimizerRules.TransformDirection.DOWN);
        }

        protected OptimizerRule(OptimizerRules.TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return direction == OptimizerRules.TransformDirection.DOWN
                ? plan.transformDown(typeToken(), this::rule)
                : plan.transformUp(typeToken(), this::rule);
        }

        @Override
        protected abstract PhysicalPlan rule(SubPlan plan);
    }

    public abstract static class OptimizerExpressionRule<E extends Expression> extends Rule<PhysicalPlan, PhysicalPlan> {

        private final OptimizerRules.TransformDirection direction;
        // overriding type token which returns the correct class but does an uncheck cast to LogicalPlan due to its generic bound
        // a proper solution is to wrap the Expression rule into a Plan rule but that would affect the rule declaration
        // so instead this is hacked here
        private final Class<E> expressionTypeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        public OptimizerExpressionRule(OptimizerRules.TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return direction == OptimizerRules.TransformDirection.DOWN
                ? plan.transformExpressionsDown(expressionTypeToken, this::rule)
                : plan.transformExpressionsUp(expressionTypeToken, this::rule);
        }

        @Override
        protected PhysicalPlan rule(PhysicalPlan plan) {
            return plan;
        }

        protected abstract Expression rule(E e);

        public Class<E> expressionToken() {
            return expressionTypeToken;
        }
    }
}
