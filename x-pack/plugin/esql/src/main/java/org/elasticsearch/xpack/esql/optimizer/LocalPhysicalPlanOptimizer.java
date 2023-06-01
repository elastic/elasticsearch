/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules.OptimizerRule;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.PhysicalVerificationException;
import org.elasticsearch.xpack.esql.planner.PhysicalVerifier;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.QlTranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.util.Holder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection.UP;

public class LocalPhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, LocalPhysicalOptimizerContext> {
    private static final QlTranslatorHandler TRANSLATOR_HANDLER = new EsqlTranslatorHandler();

    private final PhysicalVerifier verifier = new PhysicalVerifier();

    public LocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context) {
        super(context);
    }

    public PhysicalPlan localOptimize(PhysicalPlan plan) {
        return verify(execute(plan));
    }

    PhysicalPlan verify(PhysicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new PhysicalVerificationException(failures);
        }
        return plan;
    }

    static List<Batch<PhysicalPlan>> rules(boolean optimizeForEsSource) {
        List<Rule<?, PhysicalPlan>> esSourceRules = new ArrayList<>(4);
        esSourceRules.add(new ReplaceAttributeSourceWithDocId());

        if (optimizeForEsSource) {
            esSourceRules.add(new PushTopNToSource());
            esSourceRules.add(new PushLimitToSource());
            esSourceRules.add(new PushFiltersToSource());
        }

        // execute the rules multiple times to improve the chances of things being pushed down
        @SuppressWarnings("unchecked")
        var pushdown = new Batch<PhysicalPlan>("Push to ES", esSourceRules.toArray(Rule[]::new));
        // add the field extraction in just one pass
        // add it at the end after all the other rules have ran
        var fieldExtraction = new Batch<>("Field extraction", Limiter.ONCE, new InsertFieldExtraction());
        return asList(pushdown, fieldExtraction);
    }

    @Override
    protected List<Batch<PhysicalPlan>> batches() {
        return rules(true);
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

    // Materialize the concrete fields that need to be extracted from the storage until the last possible moment.
    // Expects the local plan to already have a projection containing the fields needed upstream.
    //
    // 1. add the materialization right before usage inside the local plan
    // 2. materialize any missing fields needed further up the chain
    /**
     * @see org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer.ProjectAwayColumns
     */
    static class InsertFieldExtraction extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            var lastFieldExtractorParent = new Holder<UnaryExec>();
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
                    // TODO: this seems out of place
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

                return p;
            });

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
                        query = boolQuery().filter(filterQuery).filter(planQuery);
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
            } else if (exp instanceof RegexMatch<?> rm) {
                return rm.field() instanceof FieldAttribute;
            } else if (exp instanceof In in) {
                return in.value() instanceof FieldAttribute && Expressions.foldable(in.list());
            } else if (exp instanceof Not not) {
                return canPushToSource(not.field());
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

        private List<EsQueryExec.FieldSort> buildFieldSorts(List<Order> orders) {
            List<EsQueryExec.FieldSort> sorts = new ArrayList<>(orders.size());
            for (Order o : orders) {
                sorts.add(new EsQueryExec.FieldSort(((FieldAttribute) o.child()), o.direction(), o.nullsPosition()));
            }
            return sorts;
        }
    }

    private static final class EsqlTranslatorHandler extends QlTranslatorHandler {
        @Override
        public Query wrapFunctionQuery(ScalarFunction sf, Expression field, Supplier<Query> querySupplier) {
            if (field instanceof FieldAttribute fa) {
                return ExpressionTranslator.wrapIfNested(new SingleValueQuery(querySupplier.get(), fa.name()), field);
            }
            throw new IllegalStateException("Should always be field attributes");
        }
    }
}
