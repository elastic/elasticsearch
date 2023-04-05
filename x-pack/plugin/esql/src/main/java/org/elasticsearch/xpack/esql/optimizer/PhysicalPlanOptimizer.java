/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec.Mode;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.FieldSort;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalPlanExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
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
        var gather = new Batch<>("Exchange", Limiter.ONCE, new InsertGatherExchange(), new AddExplicitProject());

        // local planning - add marker
        var localPlanningStart = new Batch<>("Local Plan Start", Limiter.ONCE, new MarkLocalPlan());

        // local rules
        List<Rule<?, PhysicalPlan>> esSourceRules = new ArrayList<>(4);
        esSourceRules.add(new ReplaceAttributeSourceWithDocId());

        if (isOptimizedForEsSource) {
            esSourceRules.add(new PushTopNToSource());
            esSourceRules.add(new PushLimitToSource());
            esSourceRules.add(new PushFiltersToSource());
        }

        // execute the rules multiple times to improve the chances of things being pushed down
        @SuppressWarnings("unchecked")
        var localPlanning = new Batch<PhysicalPlan>("Push to ES", esSourceRules.toArray(Rule[]::new));
        // add the field extraction in just one pass
        // add it at the end after all the other rules have ran
        var fieldExtraction = new Batch<>("Field extraction", Limiter.ONCE, new InsertFieldExtraction());

        // the distributed plan must be executed after the field extraction
        var distribution = new Batch<>("Distributed", Limiter.ONCE, new Distributed());

        // local planning - clean-up
        var localPlanningStop = new Batch<>("Local Plan Stop", Limiter.ONCE, new RemoveLocalPlanMarker());

        // return asList(exchange, parallelism, reducer, localPlanningStart, localPlanning, localPlanningStop);
        return asList(gather, localPlanningStart, localPlanning, fieldExtraction, distribution, localPlanningStop);
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
            var found = new Holder<>(FALSE);
            plan = plan.transformDown(ExchangeExec.class, e -> {
                PhysicalPlan p = e;
                if (found.get() == false) {
                    found.set(TRUE);
                    p = new LocalPlanExec(e.source(), e);
                }
                return p;
            });
            if (found.get() == FALSE) {
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
     * Dedicate rule for adding an exchange into the plan that acts as a very basic state machine:
     * 1. Starts bottom-up and if the source is an EsQueryExec goes into gather mode
     * 2. In gather mode, it looks for the first encounter of limit, sort or aggregate right after the node.
     * In addition, for TopN/Limit/Sort it copies the node on top of the gather.
     */
    private static class InsertGatherExchange extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            var needsGather = new Holder<>(FALSE);

            plan = plan.transformUp(p -> {
                // move to gather nodes only for EsQueryExec
                if (needsGather.get() == FALSE && p instanceof EsSourceExec) {
                    needsGather.set(TRUE);
                }
                // in gather, check presence of copying nodes and if found, apply it on top of the node.
                // Copy the node as well for Order, TopN and Limit
                if (needsGather.get() == TRUE) {
                    // no need to add project when dealing with an aggregate
                    if (p instanceof AggregateExec agg) {
                        if (agg.getMode() == Mode.PARTIAL) {
                            p = addGatherExchange(p);
                        }
                        needsGather.set(FALSE);
                    } else {
                        // found a project, no need to add a manual one
                        if (p instanceof LimitExec || p instanceof OrderExec || p instanceof TopNExec) {
                            // add the exchange but also clone the node
                            PhysicalPlan localCopy = p;
                            p = ((UnaryExec) p).replaceChild(addGatherExchange(localCopy));
                            needsGather.set(FALSE);

                        }
                    }
                }
                return p;
            });

            return plan;
        }

        private static ExchangeExec addGatherExchange(PhysicalPlan p) {
            return new ExchangeExec(p.source(), p, ExchangeExec.Mode.LOCAL);
        }
    }

    /**
     * Adds an explicit project to filter out the amount of attributes sent from the local plan to the coordinator.
     * This is done here to localize the project close to the data source and simplify the upcoming field
     * extraction.
     */
    private static class AddExplicitProject extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            var projectAll = new Holder<>(TRUE);
            var keepCollecting = new Holder<>(TRUE);
            var fieldAttributes = new LinkedHashSet<Attribute>();
            var aliases = new HashMap<Attribute, Expression>();

            return plan.transformDown(UnaryExec.class, p -> {
                // no need for project all
                if (p instanceof ProjectExec || p instanceof AggregateExec) {
                    projectAll.set(FALSE);
                }
                if (keepCollecting.get()) {
                    if (p instanceof DissectExec dissect) {
                        fieldAttributes.removeAll(dissect.extractedFields());
                    } else {
                        p.forEachExpression(NamedExpression.class, ne -> {
                            var attr = ne.toAttribute();
                            // filter out aliases declared before the exchange
                            if (ne instanceof Alias as) {
                                aliases.put(attr, as.child());
                                fieldAttributes.remove(attr);
                            } else {
                                if (aliases.containsKey(attr) == false) {
                                    fieldAttributes.add(attr);
                                }
                            }
                        });
                    }
                }
                if (p instanceof ExchangeExec exec) {
                    keepCollecting.set(FALSE);
                    // no need for projection when dealing with aggs
                    if (exec.child() instanceof AggregateExec) {
                        fieldAttributes.clear();
                    }
                    var selectAll = projectAll.get();
                    if (fieldAttributes.isEmpty() == false || selectAll) {
                        var output = selectAll ? exec.child().output() : new ArrayList<>(fieldAttributes);
                        p = exec.replaceChild(new ProjectExec(exec.source(), exec.child(), output));
                    }
                }
                return p;
            });
        }
    }

    //
    // Materialize the concrete fields that need to be extracted from the storage until the last possible moment
    // 0. collect all fields necessary going down the tree
    // 1. once the local plan is found (segment-level), start adding field extractors
    // 2. add the materialization right before usage inside the local plan
    // 3. materialize any missing fields needed further up the chain
    // 4. add project (shouldn't be necessary due to AddExplicitProject) in order to drop off _doc
    static class InsertFieldExtraction extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            var globalMissing = new LinkedHashSet<Attribute>();
            var keepCollecting = new Holder<>(TRUE);

            // collect coordinator field extraction - top to data-node
            plan = plan.transformDown(UnaryExec.class, p -> {
                PhysicalPlan pl = p;
                if (p instanceof LocalPlanExec localPlan) {
                    // stop collecting
                    keepCollecting.set(FALSE);
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
            var lastFieldExtractorParent = new Holder<UnaryExec>();
            var needsProjection = new Holder<>(TRUE);

            // apply the plan locally, adding a field extractor right before data is loaded
            // by going bottom-up
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
                    var extractor = new FieldExtractExec(p.source(), p.child(), List.copyOf(missing));
                    p = p.replaceChild(extractor);
                    lastFieldExtractorParent.set(p);
                }

                if (p instanceof ProjectExec || p instanceof AggregateExec) {
                    needsProjection.set(FALSE);
                }

                return p;
            });

            // 2. check if there's a need to add any non-extracted attributes from the local plan to the last field extractor
            // optionally project away the source attributes if no other projection is found locally
            if (missingUpstream.size() > 0) {
                var lastParent = lastFieldExtractorParent.get();
                var missingSet = new AttributeSet(missingUpstream);
                // no field extract present -- add it right before the exchange
                if (lastParent == null) {
                    var exchange = localPlan.child();
                    plan = plan.transformDown(UnaryExec.class, p -> {
                        if (p == exchange) {
                            var fieldExtract = new FieldExtractExec(exchange.source(), p.child(), List.copyOf(missingSet));
                            p = p.replaceChild(projectAwayDocId(needsProjection.get(), fieldExtract));
                        }
                        return p;
                    });
                }
                // field extractor present, enrich it
                else {
                    missingUpstream.removeAll(lastParent.inputSet());
                    if (missingUpstream.size() > 0) {
                        plan = plan.transformDown(UnaryExec.class, p -> {
                            PhysicalPlan pl = p;
                            if (p == lastParent) {
                                var extractor = (FieldExtractExec) p.child();
                                var combined = new AttributeSet(extractor.attributesToExtract()).combine(new AttributeSet(missingUpstream));
                                var fieldExtractor = new FieldExtractExec(p.source(), extractor.child(), List.copyOf(combined));
                                pl = p.replaceChild(projectAwayDocId(needsProjection.get(), fieldExtractor));
                            }
                            return pl;
                        });
                    }
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

        private static PhysicalPlan projectAwayDocId(Boolean needsProjection, FieldExtractExec fieldExtract) {
            PhysicalPlan plan = fieldExtract;
            if (needsProjection == TRUE) {
                var list = fieldExtract.output();
                list.remove(fieldExtract.sourceAttribute());
                plan = new ProjectExec(fieldExtract.source(), fieldExtract, list);
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
            if (filterExec.child() instanceof EsQueryExec queryExec) {
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
                    queryExec = new EsQueryExec(
                        queryExec.source(),
                        queryExec.index(),
                        queryExec.output(),
                        query,
                        queryExec.limit(),
                        queryExec.sorts()
                    );
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
            } else if (child instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
                plan = exchangeExec.replaceChild(queryExec.withLimit(limitExec.limit()));
            }
            return plan;
        }
    }

    private static class PushTopNToSource extends OptimizerRule<TopNExec> {
        @Override
        protected PhysicalPlan rule(TopNExec topNExec) {
            PhysicalPlan plan = topNExec;
            PhysicalPlan child = topNExec.child();

            boolean canPushDownTopN = child instanceof EsQueryExec
                || (child instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec);
            if (canPushDownTopN && canPushDownOrders(topNExec.order()) && ((Integer) topNExec.limit().fold()) <= LuceneOperator.PAGE_SIZE) {
                var sorts = buildFieldSorts(topNExec.order());
                var limit = topNExec.limit();

                if (child instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
                    plan = exchangeExec.replaceChild(queryExec.withSorts(sorts).withLimit(limit));
                } else {
                    plan = ((EsQueryExec) child).withSorts(sorts).withLimit(limit);
                }
            }
            return plan;
        }

        private boolean canPushDownOrders(List<Order> orders) {
            // allow only FieldAttributes (no expressions) for sorting
            return false == Expressions.match(orders, s -> ((Order) s).child() instanceof FieldAttribute == false);
        }

        private List<FieldSort> buildFieldSorts(List<Order> orders) {
            List<FieldSort> sorts = new ArrayList<>(orders.size());
            for (Order o : orders) {
                sorts.add(new FieldSort(((FieldAttribute) o.child()), o.direction(), o.nullsPosition()));
            }
            return sorts;
        }
    }

    /**
     * Splits the given physical into two parts: the downstream below the remote exchange, to be executed on data nodes
     * and the upstream above the remote exchange, to be executed on the coordinator node.
     * TODO: We should have limit, topN on data nodes before returning the result.
     */
    private static class Distributed extends Rule<PhysicalPlan, PhysicalPlan> {

        private static boolean startWithLuceneIndex(PhysicalPlan plan) {
            var foundLucene = new Holder<>(FALSE);
            plan.forEachUp(p -> {
                if (p instanceof EsQueryExec) {
                    foundLucene.set(TRUE);
                }
            });
            return foundLucene.get();
        }

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            if (startWithLuceneIndex(plan) == false) {
                return plan;
            }
            var delimiter = new Holder<PhysicalPlan>();
            var foundLimit = new Holder<>(FALSE);
            plan.forEachUp(p -> {
                if (p instanceof TopNExec || p instanceof LimitExec || p instanceof OrderExec) {
                    foundLimit.set(TRUE);
                }
                // aggregation partial from limit must be executed after the final topN
                if (p instanceof EsQueryExec
                    || p instanceof FieldExtractExec
                    || (p instanceof AggregateExec agg && agg.getMode() == Mode.PARTIAL && foundLimit.get() == FALSE)) {
                    delimiter.set(p);
                }
                // execute as much as possible on data nodes to minimize network traffic and achieve higher concurrent execution
                if (p instanceof ExchangeExec e && delimiter.get() != null) {
                    assert e.mode() == ExchangeExec.Mode.LOCAL;
                    delimiter.set(e);
                }
            });
            plan = plan.transformDown(PhysicalPlan.class, p -> {
                if (p == delimiter.get()) {
                    delimiter.set(null);
                    if (p instanceof ExchangeExec e) {
                        p = addRemoteExchange(e.child());
                    } else {
                        p = addRemoteExchange(p);
                    }
                }
                return p;
            });
            return plan;
        }

        private static ExchangeExec addRemoteExchange(PhysicalPlan p) {
            var remoteSink = new ExchangeExec(p.source(), p, ExchangeExec.Mode.REMOTE_SINK);
            return new ExchangeExec(p.source(), remoteSink, ExchangeExec.Mode.REMOTE_SOURCE);
        }
    }
}
