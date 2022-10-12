/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.List;

@Experimental
public class Optimizer extends RuleExecutor<PhysicalPlan> {

    private static Setting<Boolean> ADD_TASK_PARALLELISM_ABOVE_QUERY = Setting.boolSetting("add_task_parallelism_above_query", false);

    private final EsqlConfiguration configuration;

    public Optimizer(EsqlConfiguration configuration) {
        this.configuration = configuration;
    }

    public PhysicalPlan optimize(PhysicalPlan verified) {
        PhysicalPlan plan = execute(verified);
        // ensure we always have single node at the end
        if (plan.singleNode() == false) {
            return new ExchangeExec(plan.source(), plan, ExchangeExec.Type.GATHER, ExchangeExec.Partitioning.SINGLE_DISTRIBUTION);
        }
        return plan;
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

    @Override
    protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
        List<Batch> batches = new ArrayList<>();
        batches.add(new Batch("Create topN", new CreateTopN()));
        batches.add(new Batch("Split nodes", new SplitAggregate(), new SplitTopN()));
        batches.add(new Batch("Add exchange", new AddExchangeOnSingleNodeSplit()));
        batches.add(
            new Batch(
                "Move FieldExtract upwards",
                new FieldExtractPastEval(),
                new FieldExtractPastAggregate(),
                new EmptyFieldExtractRemoval()
            )
        );
        // TODO: add rule to prune _doc_id, _segment_id, _shard_id at the top
        // Batch addProject = new Batch("Add project", new AddProjectWhenInternalFieldNoLongerNeeded());
        if (ADD_TASK_PARALLELISM_ABOVE_QUERY.get(configuration.pragmas())) {
            batches.add(new Batch("Add task parallelization above query", new AddTaskParallelismAboveQuery()));
        }
        return batches;
    }

    private static class FieldExtractPastEval extends OptimizerRule<EvalExec> {

        @Override
        protected PhysicalPlan rule(EvalExec eval) {
            if (eval.child()instanceof FieldExtractExec fieldExtractExec) {
                // If you have an ExtractFieldNode below an EvalNode,
                // only extract the things that the eval needs, and extract the rest above eval
                return possiblySplitExtractFieldNode(eval, eval.fields(), fieldExtractExec, true);
            }
            return eval;
        }
    }

    private static class FieldExtractPastAggregate extends OptimizerRule<AggregateExec> {

        @Override
        protected PhysicalPlan rule(AggregateExec aggregateExec) {
            if (aggregateExec.child()instanceof FieldExtractExec fieldExtractExec) {
                // If you have an ExtractFieldNode below an Aggregate,
                // only extract the things that the aggregate needs, and extract the rest above eval
                return possiblySplitExtractFieldNode(aggregateExec, aggregateExec.aggregates(), fieldExtractExec, false);
            }
            return aggregateExec;
        }
    }

    private static UnaryExec possiblySplitExtractFieldNode(
        UnaryExec parent,
        List<? extends NamedExpression> namedExpressions,
        FieldExtractExec fieldExtractExec,
        boolean preserveUnused
    ) {
        List<Attribute> attributesToKeep = new ArrayList<>();
        List<Attribute> attributesToMoveUp = new ArrayList<>();
        outer: for (Attribute fieldExtractAttribute : fieldExtractExec.getAttrs()) {
            if (namedExpressions.stream().anyMatch(ne -> ne.anyMatch(e -> e.semanticEquals(fieldExtractAttribute)))) {
                attributesToKeep.add(fieldExtractAttribute);
            } else {
                if (preserveUnused) {
                    attributesToMoveUp.add(fieldExtractAttribute);
                }
            }
        }
        if (attributesToKeep.size() == fieldExtractExec.getAttrs().size()) {
            return parent;
        }
        return new FieldExtractExec(
            fieldExtractExec.source(),
            parent.replaceChild(
                new FieldExtractExec(
                    fieldExtractExec.source(),
                    fieldExtractExec.child(),
                    fieldExtractExec.index(),
                    attributesToKeep,
                    fieldExtractExec.getEsQueryAttrs()
                )
            ),
            fieldExtractExec.index(),
            attributesToMoveUp,
            fieldExtractExec.getEsQueryAttrs()
        );
    }

    private static class EmptyFieldExtractRemoval extends OptimizerRule<FieldExtractExec> {

        @Override
        protected PhysicalPlan rule(FieldExtractExec fieldExtractExec) {
            if (fieldExtractExec.getAttrs().isEmpty()) {
                return fieldExtractExec.child();
            }
            return fieldExtractExec;
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
}
