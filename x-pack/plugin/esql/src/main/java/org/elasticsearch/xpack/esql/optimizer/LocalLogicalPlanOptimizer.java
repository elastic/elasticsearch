/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.rules.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.PropagateEmptyRelation;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.cleanup;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.operators;
import static org.elasticsearch.xpack.esql.optimizer.rules.OptimizerRules.TransformDirection.UP;

/**
 * <p>This class is part of the planner. Data node level logical optimizations.  At this point we have access to
 * {@link org.elasticsearch.xpack.esql.stats.SearchStats} which provides access to metadata about the index. </p>
 *
 * <p>NB: This class also reapplies all the rules from {@link LogicalPlanOptimizer#operators()} and {@link LogicalPlanOptimizer#cleanup()}
 * </p>
 */
public class LocalLogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LocalLogicalOptimizerContext> {

    public LocalLogicalPlanOptimizer(LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        super(localLogicalOptimizerContext);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        var local = new Batch<>(
            "Local rewrite",
            Limiter.ONCE,
            new ReplaceTopNWithLimitAndSort(),
            new ReplaceMissingFieldWithNull(),
            new InferIsNotNull(),
            new InferNonNullAggConstraint()
        );

        var rules = new ArrayList<Batch<LogicalPlan>>();
        rules.add(local);
        // TODO: if the local rules haven't touched the tree, the rest of the rules can be skipped
        rules.addAll(asList(operators(), cleanup()));
        replaceRules(rules);
        return rules;
    }

    private List<Batch<LogicalPlan>> replaceRules(List<Batch<LogicalPlan>> listOfRules) {
        for (Batch<LogicalPlan> batch : listOfRules) {
            var rules = batch.rules();
            for (int i = 0; i < rules.length; i++) {
                if (rules[i] instanceof PropagateEmptyRelation) {
                    rules[i] = new LocalPropagateEmptyRelation();
                }
            }
        }
        return listOfRules;
    }

    public LogicalPlan localOptimize(LogicalPlan plan) {
        return execute(plan);
    }

    /**
     * Break TopN back into Limit + OrderBy to allow the order rules to kick in.
     */
    public static class ReplaceTopNWithLimitAndSort extends OptimizerRules.OptimizerRule<TopN> {
        public ReplaceTopNWithLimitAndSort() {
            super(UP);
        }

        @Override
        protected LogicalPlan rule(TopN plan) {
            return new Limit(plan.source(), plan.limit(), new OrderBy(plan.source(), plan.child(), plan.order()));
        }
    }

    /**
     * Look for any fields used in the plan that are missing locally and replace them with null.
     * This should minimize the plan execution, in the best scenario skipping its execution all together.
     */
    private static class ReplaceMissingFieldWithNull extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

        @Override
        public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
            return plan.transformUp(p -> missingToNull(p, localLogicalOptimizerContext.searchStats()));
        }

        private LogicalPlan missingToNull(LogicalPlan plan, SearchStats stats) {
            if (plan instanceof EsRelation || plan instanceof LocalRelation) {
                return plan;
            }

            if (plan instanceof Aggregate a) {
                // don't do anything (for now)
                return a;
            }
            // keep the aliased name
            else if (plan instanceof Project project) {
                var projections = project.projections();
                List<NamedExpression> newProjections = new ArrayList<>(projections.size());
                Map<DataType, Alias> nullLiteral = Maps.newLinkedHashMapWithExpectedSize(DataType.types().size());

                for (NamedExpression projection : projections) {
                    // Do not use the attribute name, this can deviate from the field name for union types.
                    if (projection instanceof FieldAttribute f && stats.exists(f.fieldName()) == false) {
                        DataType dt = f.dataType();
                        Alias nullAlias = nullLiteral.get(f.dataType());
                        // save the first field as null (per datatype)
                        if (nullAlias == null) {
                            Alias alias = new Alias(f.source(), f.name(), Literal.of(f, null), f.id());
                            nullLiteral.put(dt, alias);
                            projection = alias.toAttribute();
                        }
                        // otherwise point to it
                        else {
                            // since avoids creating field copies
                            projection = new Alias(f.source(), f.name(), nullAlias.toAttribute(), f.id());
                        }
                    }

                    newProjections.add(projection);
                }
                // add the first found field as null
                if (nullLiteral.size() > 0) {
                    plan = new Eval(project.source(), project.child(), new ArrayList<>(nullLiteral.values()));
                    plan = new Project(project.source(), plan, newProjections);
                }
            } else if (plan instanceof Eval
                || plan instanceof Filter
                || plan instanceof OrderBy
                || plan instanceof RegexExtract
                || plan instanceof TopN) {
                    plan = plan.transformExpressionsOnlyUp(
                        FieldAttribute.class,
                        // Do not use the attribute name, this can deviate from the field name for union types.
                        f -> stats.exists(f.fieldName()) ? f : Literal.of(f, null)
                    );
                }

            return plan;
        }
    }

    /**
     * Simplify IsNotNull targets by resolving the underlying expression to its root fields with unknown
     * nullability.
     * e.g.
     * (x + 1) / 2 IS NOT NULL --> x IS NOT NULL AND (x+1) / 2 IS NOT NULL
     * SUBSTRING(x, 3) > 4 IS NOT NULL --> x IS NOT NULL AND SUBSTRING(x, 3) > 4 IS NOT NULL
     * When dealing with multiple fields, a conjunction/disjunction based on the predicate:
     * (x + y) / 4 IS NOT NULL --> x IS NOT NULL AND y IS NOT NULL AND (x + y) / 4 IS NOT NULL
     * This handles the case of fields nested inside functions or expressions in order to avoid:
     * - having to evaluate the whole expression
     * - not pushing down the filter due to expression evaluation
     * IS NULL cannot be simplified since it leads to a disjunction which prevents the filter to be
     * pushed down:
     * (x + 1) IS NULL --> x IS NULL OR x + 1 IS NULL
     * and x IS NULL cannot be pushed down
     * <br/>
     * Implementation-wise this rule goes bottom-up, keeping an alias up to date to the current plan
     * and then looks for replacing the target.
     */
    static class InferIsNotNull extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            // the alias map is shared across the whole plan
            AttributeMap<Expression> aliases = new AttributeMap<>();
            // traverse bottom-up to pick up the aliases as we go
            plan = plan.transformUp(p -> inspectPlan(p, aliases));
            return plan;
        }

        private LogicalPlan inspectPlan(LogicalPlan plan, AttributeMap<Expression> aliases) {
            // inspect just this plan properties
            plan.forEachExpression(Alias.class, a -> aliases.put(a.toAttribute(), a.child()));
            // now go about finding isNull/isNotNull
            LogicalPlan newPlan = plan.transformExpressionsOnlyUp(IsNotNull.class, inn -> inferNotNullable(inn, aliases));
            return newPlan;
        }

        private Expression inferNotNullable(IsNotNull inn, AttributeMap<Expression> aliases) {
            Expression result = inn;
            Set<Expression> refs = resolveExpressionAsRootAttributes(inn.field(), aliases);
            // no refs found or could not detect - return the original function
            if (refs.size() > 0) {
                // add IsNull for the filters along with the initial inn
                var innList = CollectionUtils.combine(refs.stream().map(r -> (Expression) new IsNotNull(inn.source(), r)).toList(), inn);
                result = Predicates.combineAnd(innList);
            }
            return result;
        }

        /**
         * Unroll the expression to its references to get to the root fields
         * that really matter for filtering.
         */
        protected Set<Expression> resolveExpressionAsRootAttributes(Expression exp, AttributeMap<Expression> aliases) {
            Set<Expression> resolvedExpressions = new LinkedHashSet<>();
            boolean changed = doResolve(exp, aliases, resolvedExpressions);
            return changed ? resolvedExpressions : emptySet();
        }

        private boolean doResolve(Expression exp, AttributeMap<Expression> aliases, Set<Expression> resolvedExpressions) {
            boolean changed = false;
            // check if the expression can be skipped or is not nullabe
            if (skipExpression(exp)) {
                resolvedExpressions.add(exp);
            } else {
                for (Expression e : exp.references()) {
                    Expression resolved = aliases.resolve(e, e);
                    // found a root attribute, bail out
                    if (resolved instanceof Attribute a && resolved == e) {
                        resolvedExpressions.add(a);
                        // don't mark things as change if the original expression hasn't been broken down
                        changed |= resolved != exp;
                    } else {
                        // go further
                        changed |= doResolve(resolved, aliases, resolvedExpressions);
                    }
                }
            }
            return changed;
        }

        private static boolean skipExpression(Expression e) {
            return e instanceof Coalesce;
        }
    }

    /**
     * Local aggregation can only produce intermediate state that get wired into the global agg.
     */
    private static class LocalPropagateEmptyRelation extends PropagateEmptyRelation {

        /**
         * Local variant of the aggregation that returns the intermediate value.
         */
        @Override
        protected void aggOutput(NamedExpression agg, AggregateFunction aggFunc, BlockFactory blockFactory, List<Block> blocks) {
            List<Attribute> output = AbstractPhysicalOperationProviders.intermediateAttributes(List.of(agg), List.of());
            for (Attribute o : output) {
                DataType dataType = o.dataType();
                // boolean right now is used for the internal #seen so always return true
                var value = dataType == DataType.BOOLEAN ? true
                    // look for count(literal) with literal != null
                    : aggFunc instanceof Count count && (count.foldable() == false || count.fold() != null) ? 0L
                    // otherwise nullify
                    : null;
                var wrapper = BlockUtils.wrapperFor(blockFactory, PlannerUtils.toElementType(dataType), 1);
                wrapper.accept(value);
                blocks.add(wrapper.builder().build());
            }
        }
    }

    /**
     * The vast majority of aggs ignore null entries - this rule adds a pushable filter, as it is cheap
     * to execute, to filter this entries out to begin with.
     * STATS x = min(a), y = sum(b)
     * becomes
     * | WHERE a IS NOT NULL OR b IS NOT NULL
     * | STATS x = min(a), y = sum(b)
     * <br>
     * Unfortunately this optimization cannot be applied when grouping is necessary since it can filter out
     * groups containing only null values
     */
    static class InferNonNullAggConstraint extends ParameterizedOptimizerRule<Aggregate, LocalLogicalOptimizerContext> {

        @Override
        protected LogicalPlan rule(Aggregate aggregate, LocalLogicalOptimizerContext context) {
            // only look at aggregates with default grouping
            if (aggregate.groupings().size() > 0) {
                return aggregate;
            }

            SearchStats stats = context.searchStats();
            LogicalPlan plan = aggregate;
            var aggs = aggregate.aggregates();
            Set<Expression> nonNullAggFields = Sets.newLinkedHashSetWithExpectedSize(aggs.size());
            for (var agg : aggs) {
                if (Alias.unwrap(agg) instanceof AggregateFunction af) {
                    Expression field = af.field();
                    // ignore literals (e.g. COUNT(1))
                    // make sure the field exists at the source and is indexed (not runtime)
                    if (field.foldable() == false && field instanceof FieldAttribute fa && stats.isIndexed(fa.name())) {
                        nonNullAggFields.add(field);
                    } else {
                        // otherwise bail out since unless disjunction needs to cover _all_ fields, things get filtered out
                        return plan;
                    }
                }
            }

            if (nonNullAggFields.size() > 0) {
                Expression condition = Predicates.combineOr(
                    nonNullAggFields.stream().map(f -> (Expression) new IsNotNull(aggregate.source(), f)).toList()
                );
                plan = aggregate.replaceChild(new Filter(aggregate.source(), aggregate.child(), condition));
            }
            return plan;
        }
    }

    abstract static class ParameterizedOptimizerRule<SubPlan extends LogicalPlan, P> extends ParameterizedRule<SubPlan, LogicalPlan, P> {

        public final LogicalPlan apply(LogicalPlan plan, P context) {
            return plan.transformUp(typeToken(), t -> rule(t, context));
        }

        protected abstract LogicalPlan rule(SubPlan plan, P context);
    }
}
