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
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalPlanExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
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
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitAnd;

@Experimental
public class PhysicalPlanOptimizer extends RuleExecutor<PhysicalPlan> {

    static Setting<Boolean> ADD_TASK_PARALLELISM_ABOVE_QUERY = Setting.boolSetting("add_task_parallelism_above_query", false);
    private static final QlTranslatorHandler TRANSLATOR_HANDLER = new QlTranslatorHandler();

    private final EsqlConfiguration configuration;

    public PhysicalPlanOptimizer(EsqlConfiguration configuration) {
        this.configuration = configuration;
    }

    public PhysicalPlan optimize(PhysicalPlan plan) {
        return execute(plan);
    }

    @Override
    protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
        List<Batch> batches = new ArrayList<>();
        // keep filters pushing before field extraction insertion
        batches.add(new Batch("Global plan", Limiter.ONCE, new PushFiltersToSource()));
        batches.add(new Batch("Data flow", Limiter.ONCE, new AddExchangeOnSingleNodeSplit()));

        if (ADD_TASK_PARALLELISM_ABOVE_QUERY.get(configuration.pragmas())) {
            batches.add(new Batch("Add task parallelization above query", Limiter.ONCE, new AddTaskParallelismAboveQuery()));
        }

        batches.add(new Batch("Gather data flow", Limiter.ONCE, new EnsureSingleGatheringNode()));

        // local optimizations
        batches.add(
            new Batch(
                "Local Plan",
                Limiter.ONCE,
                new MarkLocalPlan(),
                new LocalToGlobalLimit(),
                new InsertFieldExtraction(),
                new LocalOptimizations(),
                new RemoveLocalPlanMarker()
            )
        );

        return batches;
    }

    private static class MarkLocalPlan extends Rule<PhysicalPlan, PhysicalPlan> {

        public PhysicalPlan apply(PhysicalPlan plan) {
            var found = new Holder<Boolean>(Boolean.FALSE);
            plan = plan.transformDown(ExchangeExec.class, e -> {
                PhysicalPlan p = e;
                if (found.get() == false) {
                    found.set(Boolean.TRUE);
                    p = new LocalPlanExec(e.source(), e);
                }
                return p;
            });
            if (found.get() == Boolean.FALSE) {
                plan = new LocalPlanExec(plan.source(), plan);
            }
            return plan;
        }

        @Override
        protected PhysicalPlan rule(PhysicalPlan plan) {
            return plan;
        }
    }

    private static class RemoveLocalPlanMarker extends OptimizerRule<LocalPlanExec> {

        @Override
        protected PhysicalPlan rule(LocalPlanExec plan) {
            return plan.child();
        }
    }

    /**
     * Copy any limit in the local plan (before the exchange) after it so after gathering the data,
     * the limit still applies.
     */
    private static class LocalToGlobalLimit extends Rule<PhysicalPlan, PhysicalPlan> {

        public PhysicalPlan apply(PhysicalPlan plan) {
            PhysicalPlan pl = plan;
            if (plan instanceof UnaryExec unary && unary.child()instanceof ExchangeExec exchange) {
                var localLimit = findLocalLimit(exchange);
                if (localLimit != null) {
                    pl = new LimitExec(localLimit.source(), plan, localLimit.limit());
                }
            }
            return pl;
        }

        @Override
        protected PhysicalPlan rule(PhysicalPlan plan) {
            return plan;
        }

        private LimitExec findLocalLimit(UnaryExec localPlan) {
            for (var plan = localPlan.child();;) {
                if (plan instanceof LimitExec localLimit) {
                    return localLimit;
                }
                // possible to go deeper
                if (plan instanceof ProjectExec || plan instanceof EvalExec) {
                    plan = ((UnaryExec) plan).child();
                } else {
                    // no limit specified
                    return null;
                }
            }
        }
    }

    // Execute local rules (only once) - should be a separate step
    static class LocalOptimizations extends OptimizerRule<LocalPlanExec> {

        private final class LocalRules extends RuleExecutor<PhysicalPlan> {

            @Override
            protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
                return emptyList();
            }

            @Override
            public PhysicalPlan execute(PhysicalPlan plan) {
                return super.execute(plan);
            }
        }

        private final LocalRules localRules = new LocalRules();

        @Override
        // use the rule method to apply the local optimizations
        protected PhysicalPlan rule(LocalPlanExec plan) {
            return localRules.execute(plan);
        }
    }

    //
    // Materialize the concrete fields that need to be extracted from the storage until the last possible moment
    // 0. collect all fields necessary going down the tree
    // 1. once the local plan is found (segment-level), start adding field extractors
    // 2. add the materialization right before usage inside the local plan
    // 3. optionally prune meta fields once all fields were loaded (not needed if a project already exists)
    // 4. materialize any missing fields needed further up the chain
    static class InsertFieldExtraction extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            var globalMissing = new LinkedHashSet<Attribute>();
            var keepCollecting = new Holder<>(Boolean.TRUE);

            // collect all field extraction
            plan = plan.transformDown(UnaryExec.class, p -> {
                PhysicalPlan pl = p;
                if (p instanceof LocalPlanExec localPlan) {
                    // stop collecting
                    keepCollecting.set(Boolean.FALSE);
                    pl = insertExtract(localPlan, globalMissing);
                }
                // keep collecting global attributes
                else if (keepCollecting.get()) {
                    var input = p.inputSet();
                    p.forEachExpression(FieldAttribute.class, f -> {
                        if (input.contains(f) == false) {
                            globalMissing.add(f);
                        }
                    });
                }
                return pl;
            });
            return plan;
        }

        private PhysicalPlan insertExtract(LocalPlanExec localPlan, Set<Attribute> missingUpstream) {
            PhysicalPlan plan = localPlan;
            // 1. add the extractors before each node that requires extra columns
            var isProjectionNeeded = new Holder<Boolean>(Boolean.TRUE);
            var lastFieldExtractorParent = new Holder<UnaryExec>();

            // apply the plan locally, adding a field extractor right before data is loaded
            plan = plan.transformUp(UnaryExec.class, p -> {
                var missing = missingAttributes(p);

                // don't extract grouping fields the hash aggregator will do the extraction by itself
                if (p instanceof AggregateExec agg) {
                    missing.removeAll(Expressions.references(agg.groupings()));
                }

                // add extractor
                if (missing.isEmpty() == false) {
                    // collect source attributes and add the extractor
                    var extractor = new FieldExtractExec(p.source(), p.child(), missing);
                    p = p.replaceChild(extractor);
                    lastFieldExtractorParent.set(p);
                }

                // any existing agg / projection projects away the source attributes
                if (p instanceof AggregateExec || p instanceof ProjectExec) {
                    isProjectionNeeded.set(Boolean.FALSE);
                }
                return p;
            });

            // 2. check if there's a need to add any non-extracted attributes from the local plan to the last field extractor
            // optionally project away the source attributes if no other projection is found locally
            var lastParent = lastFieldExtractorParent.get();
            if (lastParent != null) {
                missingUpstream.removeAll(lastParent.inputSet());
                if (missingUpstream.size() > 0) {
                    plan = plan.transformDown(UnaryExec.class, p -> {
                        PhysicalPlan pl = p;
                        if (p == lastParent) {
                            var extractor = (FieldExtractExec) p.child();
                            var combined = new AttributeSet(extractor.attributesToExtract()).combine(new AttributeSet(missingUpstream));
                            PhysicalPlan child = new FieldExtractExec(p.source(), extractor.child(), combined);
                            // prune away the source attributes is necessary
                            if (isProjectionNeeded.get()) {
                                var withoutSourceAttribute = new ArrayList<>(combined);
                                withoutSourceAttribute.removeIf(EsQueryExec::isSourceAttribute);
                                child = new ProjectExec(p.source(), child, withoutSourceAttribute);
                            }
                            pl = p.replaceChild(child);
                        }
                        return pl;
                    });
                }
            }

            return plan;
        }

        private static Set<Attribute> missingAttributes(PhysicalPlan p) {
            var missing = new LinkedHashSet<Attribute>();
            var input = p.inputSet();

            // collect field attributes used inside expressions
            p.forEachExpression(FieldAttribute.class, f -> {
                if (input.contains(f) == false) {
                    missing.add(f);
                }
            });
            return missing;
        }

        @Override
        protected PhysicalPlan rule(PhysicalPlan physicalPlan) {
            return physicalPlan;
        }
    }

    private static class AddExchangeOnSingleNodeSplit extends OptimizerRule<UnaryExec> {

        @Override
        protected PhysicalPlan rule(UnaryExec parent) {
            if (parent.singleNode() && parent.child().singleNode() == false) {
                if (parent instanceof ExchangeExec exchangeExec
                    // TODO: this check might not be needed
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

    private static class AddTaskParallelismAboveQuery extends OptimizerRule<EsQueryExec> {

        protected AddTaskParallelismAboveQuery() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected PhysicalPlan rule(EsQueryExec plan) {
            return new ExchangeExec(
                plan.source(),
                plan,
                ExchangeExec.Type.REPARTITION,
                ExchangeExec.Partitioning.FIXED_ARBITRARY_DISTRIBUTION
            );
        }
    }

    private static class EnsureSingleGatheringNode extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            // ensure we always have single node at the end
            if (plan.singleNode() == false) {
                plan = new ExchangeExec(plan.source(), plan, ExchangeExec.Type.GATHER, ExchangeExec.Partitioning.SINGLE_DISTRIBUTION);
            }
            return plan;
        }

        @Override
        protected PhysicalPlan rule(PhysicalPlan plan) {
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
