/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.planner.QlTranslatorHandler;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitAnd;

@Experimental
public class PhysicalPlanOptimizer extends RuleExecutor<PhysicalPlan> {

    private static Setting<Boolean> ADD_TASK_PARALLELISM_ABOVE_QUERY = Setting.boolSetting("add_task_parallelism_above_query", false);
    private static final QlTranslatorHandler TRANSLATOR_HANDLER = new QlTranslatorHandler();

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

        // keep filters pushing before field extraction insertion
        batches.add(new Batch("Push filters to source", Limiter.ONCE, new PushFiltersToSource()));
        batches.add(new Batch("Lazy field extraction", Limiter.ONCE, new InsertFieldExtraction()));

        batches.add(new Batch("Split nodes", Limiter.ONCE, new SplitAggregate(), new SplitTopN(), new SplitLimit()));
        batches.add(new Batch("Add exchange", Limiter.ONCE, new AddExchangeOnSingleNodeSplit()));

        if (ADD_TASK_PARALLELISM_ABOVE_QUERY.get(configuration.pragmas())) {
            batches.add(new Batch("Add task parallelization above query", new AddTaskParallelismAboveQuery()));
        }

        return batches;
    }

    //
    // Materialize the concrete fields that need to be extracted from the storage until the last possible moment
    // 0. field extraction is one per EsQueryExec
    // 1. add the materialization right before usage
    // 2. prune meta fields once all fields were loaded
    static class InsertFieldExtraction extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            // 1. add the extractors before each node that requires extra columns
            var lastNodeWithExtraction = new Holder<UnaryExec>();

            // start bottom -> up

            // TODO: look into supporting nary nodes
            plan = plan.transformUp(UnaryExec.class, p -> {
                var missing = new LinkedHashSet<Attribute>();
                var input = p.inputSet();

                // collect field attributes used inside expressions
                p.forEachExpression(FieldAttribute.class, f -> {
                    if (input.contains(f) == false) {
                        missing.add(f);
                    }
                });

                // add extractor
                if (missing.isEmpty() == false) {
                    // collect source attributes
                    var extractor = new FieldExtractExec(p.source(), p.child(), missing);
                    p = p.replaceChild(extractor);
                    lastNodeWithExtraction.set(p);
                }

                // any existing agg / projection projects away the source attributes
                if (p instanceof AggregateExec || p instanceof ProjectExec) {
                    lastNodeWithExtraction.set(null);
                }
                return p;
            });

            // 2. check the last field extractor that was introduced and project the source attributes away
            var pruneNode = lastNodeWithExtraction.get();

            if (pruneNode != null) {
                plan = plan.transformUp(pruneNode.getClass(), p -> {
                    PhysicalPlan pl = p;
                    // instance equality should work
                    if (pruneNode == p) {
                        var withoutSourceAttribute = new ArrayList<>(p.output());
                        withoutSourceAttribute.removeIf(EsQueryExec::isSourceAttribute);
                        pl = new ProjectExec(p.source(), p, withoutSourceAttribute);
                    }
                    return pl;
                });
            }

            return plan;
        }

        @Override
        protected PhysicalPlan rule(PhysicalPlan plan) {
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

    private static class SplitLimit extends OptimizerRule<LimitExec> {

        @Override
        protected PhysicalPlan rule(LimitExec limitExec) {
            if (limitExec.child().singleNode() == false && limitExec.mode() == LimitExec.Mode.SINGLE) {
                return new LimitExec(
                    limitExec.source(),
                    new LimitExec(limitExec.source(), limitExec.child(), limitExec.limit(), LimitExec.Mode.PARTIAL),
                    limitExec.limit(),
                    LimitExec.Mode.FINAL
                );
            }
            return limitExec;
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

    private static class PushFiltersToSource extends OptimizerRule<FilterExec> {
        @Override
        protected PhysicalPlan rule(FilterExec filterExec) {
            PhysicalPlan plan = filterExec;
            if (filterExec.child()instanceof EsQueryExec queryExec) {
                List<Expression> pushable = new ArrayList<>();
                List<Expression> nonPushable = new ArrayList<>();
                for (Expression exp : splitAnd(filterExec.condition())) {
                    (canPushToSource(exp) ? pushable : nonPushable).add(exp);
                }
                if (pushable.size() > 0) { // update the executable with pushable conditions
                    QueryBuilder planQuery = TRANSLATOR_HANDLER.asQuery(Predicates.combineAnd(pushable)).asBuilder();
                    QueryBuilder query = planQuery;
                    QueryBuilder filterQuery = queryExec.query();
                    if (filterQuery != null) {
                        query = boolQuery().must(filterQuery).must(planQuery);
                    }
                    queryExec = new EsQueryExec(queryExec.source(), queryExec.index(), query);
                    if (nonPushable.size() > 0) { // update filter with remaining non-pushable conditions
                        plan = new FilterExec(filterExec.source(), queryExec, Predicates.combineAnd(nonPushable));
                    } else { // prune Filter entirely
                        plan = queryExec;
                    }
                } // else: nothing changes
            }

            return plan;
        }

        private static boolean canPushToSource(Expression exp) {
            if (exp instanceof BinaryComparison bc) {
                return bc.left() instanceof FieldAttribute && bc.right().foldable();
            } else if (exp instanceof BinaryLogic bl) {
                return canPushToSource(bl.left()) && canPushToSource(bl.right());
            }
            return false;
        }
    }

}
