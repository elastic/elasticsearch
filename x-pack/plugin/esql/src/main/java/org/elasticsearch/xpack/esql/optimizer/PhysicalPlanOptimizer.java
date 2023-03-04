/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalPlanExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.planner.QlTranslatorHandler;
import org.elasticsearch.xpack.ql.rule.ParameterizedRule;
import org.elasticsearch.xpack.ql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection.UP;

@Experimental
public class PhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, PhysicalOptimizerContext> {

    private static final QlTranslatorHandler TRANSLATOR_HANDLER = new QlTranslatorHandler();

    private static final Iterable<RuleExecutor.Batch<PhysicalPlan>> rules = initializeRules(true);

    public PhysicalPlanOptimizer(PhysicalOptimizerContext context) {
        super(context);
    }

    public PhysicalPlan optimize(PhysicalPlan plan) {
        return execute(plan);
    }

    static Iterable<RuleExecutor.Batch<PhysicalPlan>> initializeRules(boolean isOptimizedForEsSource) {
        // keep filters pushing before field extraction insertion
        var exchange = new Batch<>("Data flow", Limiter.ONCE, new AddExchangeOnSingleNodeSplit());
        var reducer = new Batch<>("Gather data flow", Limiter.ONCE, new EnsureSingleGatheringNode());

        // local planning - add marker
        var localPlanningStart = new Batch<>("Local Plan Start", Limiter.ONCE, new MarkLocalPlan(), new LocalToGlobalLimitAndTopNExec());

        // local rules
        List<Rule<?, PhysicalPlan>> esSourceRules = new ArrayList<>(3);
        esSourceRules.add(new ReplaceAttributeSourceWithDocId());

        if (isOptimizedForEsSource) {
            esSourceRules.add(new PushLimitToSource());
            esSourceRules.add(new PushFiltersToSource());
        }

        @SuppressWarnings("unchecked")
        Batch<PhysicalPlan> localPlanning = new Batch<>("Local planning", esSourceRules.toArray(Rule[]::new));

        // local planning - clean-up
        var localPlanningStop = new Batch<>("Local Plan Stop", Limiter.ONCE, new InsertFieldExtraction(), new RemoveLocalPlanMarker());

        return asList(exchange, reducer, localPlanningStart, localPlanning, localPlanningStop);
    }

    @Override
    protected Iterable<RuleExecutor.Batch<PhysicalPlan>> batches() {
        return rules;
    }

    private static class ReplaceAttributeSourceWithDocId extends OptimizerRule<EsSourceExec> {

        ReplaceAttributeSourceWithDocId() {
            super(UP);
        }

        @Override
        protected PhysicalPlan rule(EsSourceExec plan) {
            return new EsQueryExec(plan.source(), plan.index(), plan.query());
        }
    }

    private static class MarkLocalPlan extends Rule<PhysicalPlan, PhysicalPlan> {

        public PhysicalPlan apply(PhysicalPlan plan) {
            var found = new Holder<>(Boolean.FALSE);
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
    }

    private static class RemoveLocalPlanMarker extends OptimizerRule<LocalPlanExec> {

        @Override
        protected PhysicalPlan rule(LocalPlanExec plan) {
            return plan.child();
        }
    }

    /**
     * Copy any limit/sort/topN in the local plan (before the exchange) after it so after gathering the data,
     * the limit still applies.
     */
    private static class LocalToGlobalLimitAndTopNExec extends OptimizerRule<ExchangeExec> {

        private LocalToGlobalLimitAndTopNExec() {
            super(UP);
        }

        @Override
        protected PhysicalPlan rule(ExchangeExec exchange) {
            return maybeAddGlobalLimitOrTopN(exchange);
        }

        /**
         * This method copies any Limit/Sort/TopN in the local plan (before the exchange) after it,
         * ensuring that all the inputs are available at that point
         * eg. if between the exchange and the TopN there is a <code>project</code> that filters out
         * some inputs needed by the topN (i.e. the sorting fields), this method also modifies
         * the existing <code>project</code> to make these inputs available to the global TopN, and then adds
         * another <code>project</code> at the end of the plan, to ensure that the original semantics
         * are preserved.
         *
         * In detail:
         * <ol>
         *     <li>Traverse the plan down starting from the exchange, looking for the first Limit/Sort/TopN</li>
         *     <li>If a Limit is found, copy it after the Exchange to make it global limit</li>
         *     <li>If a TopN is found, copy it after the Exchange and ensure that it has all the inputs needed:
         *         <ol>
         *            <li>Starting from the TopN, traverse the plan backwards and check that all the nodes propagate
         *            the inputs needed by the TopN</li>
         *            <li>If a Project node filters out some of the inputs needed by the TopN,
         *            replace it with another one that includes those inputs</li>
         *            <li>Copy the TopN after the exchange, to make it global</li>
         *            <li>If the outputs of the new global TopN are different from the outputs of the original Exchange,
         *            add another Project that filters out the unneeded outputs and preserves the original semantics</li>
         *         </ol>
         *     </li>
         * </ol>
         */
        private PhysicalPlan maybeAddGlobalLimitOrTopN(ExchangeExec exchange) {
            List<UnaryExec> visitedNodes = new ArrayList<>();
            visitedNodes.add(exchange);
            AttributeSet exchangeOutputSet = exchange.outputSet();
            // step 1: traverse the plan and find Limit/TopN
            for (var plan = exchange.child();;) {
                if (plan instanceof LimitExec limit) {
                    // Step 2: just add a global Limit
                    return limit.replaceChild(exchange);
                }
                if (plan instanceof TopNExec topN) {
                    // Step 3: copy the TopN after the Exchange and ensure that it has all the inputs needed
                    Set<Attribute> requiredAttributes = Expressions.references(topN.order()).combine(topN.inputSet());
                    if (exchangeOutputSet.containsAll(requiredAttributes)) {
                        return topN.replaceChild(exchange);
                    }

                    PhysicalPlan subPlan = topN;
                    // Step 3.1: Traverse the plan backwards to check inputs available
                    for (int i = visitedNodes.size() - 1; i >= 0; i--) {
                        UnaryExec node = visitedNodes.get(i);
                        if (node instanceof ProjectExec proj && node.outputSet().containsAll(requiredAttributes) == false) {
                            // Step 3.2: a Project is filtering out some inputs needed by the global TopN,
                            // replace it with another one that preserves these inputs
                            List<NamedExpression> newProjections = new ArrayList<>(proj.projections());
                            for (Attribute attr : requiredAttributes) {
                                if (newProjections.contains(attr) == false) {
                                    newProjections.add(attr);
                                }
                            }
                            node = new ProjectExec(proj.source(), proj.child(), newProjections);
                        }
                        subPlan = node.replaceChild(subPlan);
                    }

                    // Step 3.3: add the global TopN right after the exchange
                    topN = topN.replaceChild(subPlan);
                    if (exchangeOutputSet.containsAll(topN.output())) {
                        return topN;
                    } else {
                        // Step 3.4: the output propagation is leaking at the end of the plan,
                        // add one more Project to preserve the original query semantics
                        return new ProjectExec(topN.source(), topN, new ArrayList<>(exchangeOutputSet));
                    }
                }
                if (plan instanceof ProjectExec || plan instanceof EvalExec) {
                    visitedNodes.add((UnaryExec) plan);
                    // go deeper with step 1
                    plan = ((UnaryExec) plan).child();
                } else {
                    // no limit specified, return the original plan
                    return exchange;
                }
            }
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
                    globalMissing.addAll(missingAttributes(p));
                }
                return pl;
            });
            return plan;
        }

        private PhysicalPlan insertExtract(LocalPlanExec localPlan, Set<Attribute> missingUpstream) {
            PhysicalPlan plan = localPlan;
            // 1. add the extractors before each node that requires extra columns
            var isProjectionNeeded = new Holder<>(Boolean.TRUE);
            var lastFieldExtractorParent = new Holder<UnaryExec>();

            // apply the plan locally, adding a field extractor right before data is loaded
            plan = plan.transformUp(UnaryExec.class, p -> {
                var missing = missingAttributes(p);

                /*
                 * If there is a single grouping then we'll try to use ords. Either way
                 * it loads the field lazily. If we have more than one field we need to
                 * make sure the fields are loaded for the standard hash aggregator.
                 */
                if (p instanceof AggregateExec agg && agg.groupings().size() == 1) {
                    var leaves = new LinkedList<>();
                    agg.aggregates()
                        .stream()
                        .filter(a -> agg.groupings().contains(a) == false)
                        .forEach(a -> leaves.addAll(a.collectLeaves()));
                    var remove = agg.groupings().stream().filter(g -> leaves.contains(g) == false).toList();
                    missing.removeAll(Expressions.references(remove));
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
    }

    private static class AddExchangeOnSingleNodeSplit extends OptimizerRule<UnaryExec> {

        @Override
        protected PhysicalPlan rule(UnaryExec parent) {
            if (parent instanceof ExchangeExec == false && parent.singleNode() && parent.child().singleNode() == false) {
                return parent.replaceChild(new ExchangeExec(parent.source(), parent.child()));
            }
            return parent;
        }
    }

    private static class EnsureSingleGatheringNode extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            // ensure we always have single node at the end
            if (plan.singleNode() == false) {
                plan = new ExchangeExec(plan.source(), plan);
            }
            return plan;
        }
    }

    public abstract static class ParameterizedOptimizerRule<SubPlan extends PhysicalPlan, P> extends ParameterizedRule<
        SubPlan,
        PhysicalPlan,
        P> {

        private final OptimizerRules.TransformDirection direction;

        public ParameterizedOptimizerRule() {
            this(OptimizerRules.TransformDirection.DOWN);
        }

        protected ParameterizedOptimizerRule(OptimizerRules.TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan, P context) {
            return direction == OptimizerRules.TransformDirection.DOWN
                ? plan.transformDown(typeToken(), t -> rule(t, context))
                : plan.transformUp(typeToken(), t -> rule(t, context));
        }

        protected abstract PhysicalPlan rule(SubPlan plan, P context);
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
                    queryExec = new EsQueryExec(queryExec.source(), queryExec.index(), queryExec.output(), query, queryExec.limit());
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

    private static class PushLimitToSource extends OptimizerRule<LimitExec> {
        @Override
        protected PhysicalPlan rule(LimitExec limitExec) {
            PhysicalPlan plan = limitExec;
            PhysicalPlan child = limitExec.child();
            if (child instanceof EsQueryExec queryExec) { // add_task_parallelism_above_query: false
                plan = queryExec.withLimit(limitExec.limit());
            } else if (child instanceof ExchangeExec exchangeExec && exchangeExec.child()instanceof EsQueryExec queryExec) {
                plan = exchangeExec.replaceChild(queryExec.withLimit(limitExec.limit()));
            }
            return plan;
        }
    }
}
