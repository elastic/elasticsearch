/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.ExpressionSet;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.ArbitraryConditionalFunction;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.Case;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.IfConditional;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer.CleanAliases;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStatsEnclosed;
import org.elasticsearch.xpack.sql.expression.function.aggregate.First;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixStatsEnclosed;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRank;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRanks;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentiles;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Stats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.TopHits;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.session.SingletonExecutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.Expressions.equalsAsAttribute;
import static org.elasticsearch.xpack.ql.expression.Literal.FALSE;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.inCommon;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.splitOr;
import static org.elasticsearch.xpack.ql.expression.predicate.Predicates.subtract;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;


public class Optimizer extends RuleExecutor<LogicalPlan> {

    public ExecutionInfo debugOptimize(LogicalPlan verified) {
        return verified.optimized() ? null : executeWithInfo(verified);
    }

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch pivot = new Batch("Pivot Rewrite", Limiter.ONCE,
                new RewritePivot());

        Batch refs = new Batch("Replace References", Limiter.ONCE,
                new ReplaceReferenceAttributeWithSource());

        Batch operators = new Batch("Operator Optimization",
                // combining
                new CombineProjections(),
                // folding
                new ReplaceFoldableAttributes(),
                new FoldNull(),
                new ConstantFolding(),
                new SimplifyConditional(),
                new SimplifyCase(),
                // boolean
                new BooleanSimplification(),
                new BooleanLiteralsOnTheRight(),
                new BinaryComparisonSimplification(),
                // needs to occur before BinaryComparison combinations (see class)
                new PropagateEquals(),
                new CombineBinaryComparisons(),
                // prune/elimination
                new PruneLiteralsInGroupBy(),
                new PruneDuplicatesInGroupBy(),
                new PruneFilters(),
                new PruneOrderByForImplicitGrouping(),
                new PruneLiteralsInOrderBy(),
                new PruneOrderByNestedFields(),
                new PruneCast(),
                // order by alignment of the aggs
                new SortAggregateOnOrderBy()
                );

        Batch aggregate = new Batch("Aggregation Rewrite",
                new ReplaceMinMaxWithTopHits(),
                new ReplaceAggsWithMatrixStats(),
                new ReplaceAggsWithExtendedStats(),
                new ReplaceAggsWithStats(),
                new PromoteStatsToExtendedStats(),
                new ReplaceAggsWithPercentiles(),
                new ReplaceAggsWithPercentileRanks()
                );

        Batch local = new Batch("Skip Elasticsearch",
                new SkipQueryOnLimitZero(),
                new SkipQueryIfFoldingProjection()
                );
        Batch label = new Batch("Set as Optimized", Limiter.ONCE,
                CleanAliases.INSTANCE,
                new SetAsOptimized());

        return Arrays.asList(pivot, refs, operators, aggregate, local, label);
    }

    static class RewritePivot extends OptimizerRule<Pivot> {

        @Override
        protected LogicalPlan rule(Pivot plan) {
            // 1. add the IN filter
            List<Expression> rawValues = new ArrayList<>(plan.values().size());
            for (NamedExpression namedExpression : plan.values()) {
                // everything should have resolved to an alias
                if (namedExpression instanceof Alias) {
                    rawValues.add(Literal.of(((Alias) namedExpression).child()));
                }
                // fallback - should not happen
                else {
                    UnresolvedAttribute attr = new UnresolvedAttribute(namedExpression.source(), namedExpression.name(), null,
                            "Unexpected alias");
                    return new Pivot(plan.source(), plan.child(), plan.column(), singletonList(attr), plan.aggregates());
                }
            }
            Filter filter = new Filter(plan.source(), plan.child(), new In(plan.source(), plan.column(), rawValues));
            // 2. preserve the PIVOT
            return new Pivot(plan.source(), filter, plan.column(), plan.values(), plan.aggregates(), plan.groupings());
        }
    }

    //
    // Replace any reference attribute with its source, if it does not affect the result.
    // This avoid ulterior look-ups between attributes and its source across nodes, which is
    // problematic when doing script translation.
    //
    static class ReplaceReferenceAttributeWithSource extends OptimizerBasicRule {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            final Map<Attribute, Expression> collectRefs = new LinkedHashMap<>();

            // collect aliases
            plan.forEachUp(p -> p.forEachExpressionsUp(e -> {
                if (e instanceof Alias) {
                    Alias a = (Alias) e;
                    collectRefs.put(a.toAttribute(), a.child());
                }
            }));

            plan = plan.transformUp(p -> {
                // non attribute defining plans get their references removed
                if ((p instanceof Pivot || p instanceof Aggregate || p instanceof Project) == false || p.children().isEmpty()) {
                    p = p.transformExpressionsOnly(e -> {
                        if (e instanceof ReferenceAttribute) {
                            e = collectRefs.getOrDefault(e, e);
                        }
                        return e;
                    });
                }
                return p;
            });

            return plan;
        }
    }

    static class PruneLiteralsInGroupBy extends OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate agg) {
            List<Expression> groupings = agg.groupings();
            List<Expression> prunedGroupings = new ArrayList<>();

            for (Expression g : groupings) {
                if (g.foldable()) {
                    prunedGroupings.add(g);
                }
            }

            // everything was eliminated, the grouping
            if (prunedGroupings.size() > 0) {
                List<Expression> newGroupings = new ArrayList<>(groupings);
                newGroupings.removeAll(prunedGroupings);
                return new Aggregate(agg.source(), agg.child(), newGroupings, agg.aggregates());
            }

            return agg;
        }
    }

    static class PruneDuplicatesInGroupBy extends OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate agg) {
            List<Expression> groupings = agg.groupings();
            if (groupings.isEmpty()) {
                return agg;
            }
            ExpressionSet<Expression> unique = new ExpressionSet<>(groupings);
            if (unique.size() != groupings.size()) {
                return new Aggregate(agg.source(), agg.child(), new ArrayList<>(unique), agg.aggregates());
            }
            return agg;
        }
    }

    static class PruneOrderByNestedFields extends OptimizerRule<Project> {

        private void findNested(Expression exp, AttributeMap<Function> functions, Consumer<FieldAttribute> onFind) {
            exp.forEachUp(e -> {
                if (e instanceof ReferenceAttribute) {
                    Function f = functions.get(e);
                    if (f != null) {
                        findNested(f, functions, onFind);
                    }
                }
                if (e instanceof FieldAttribute) {
                    FieldAttribute fa = (FieldAttribute) e;
                    if (fa.isNested()) {
                        onFind.accept(fa);
                    }
                }
            });
        }

        @Override
        protected LogicalPlan rule(Project project) {
            // check whether OrderBy relies on nested fields which are not used higher up
            if (project.child() instanceof OrderBy) {
                OrderBy ob = (OrderBy) project.child();

                // resolve function references (that maybe hiding the target)
                final Map<Attribute, Function> collectRefs = new LinkedHashMap<>();

                // collect Attribute sources
                // only Aliases are interesting since these are the only ones that hide expressions
                // FieldAttribute for example are self replicating.
                project.forEachUp(p -> p.forEachExpressionsUp(e -> {
                    if (e instanceof Alias) {
                        Alias a = (Alias) e;
                        if (a.child() instanceof Function) {
                            collectRefs.put(a.toAttribute(), (Function) a.child());
                        }
                    }
                }));

                AttributeMap<Function> functions = new AttributeMap<>(collectRefs);

                // track the direct parents
                Map<String, Order> nestedOrders = new LinkedHashMap<>();

                for (Order order : ob.order()) {
                    // traverse the tree since the field might be wrapped in a function
                    findNested(order.child(), functions, fa -> nestedOrders.put(fa.nestedParent().name(), order));
                }

                // no nested fields in sort
                if (nestedOrders.isEmpty()) {
                    return project;
                }

                // count the nested parents (if any) inside the parents
                List<String> nestedTopFields = new ArrayList<>();

                for (NamedExpression ne : project.projections()) {
                    // traverse the tree since the field might be wrapped in a function
                    findNested(ne, functions, fa -> nestedTopFields.add(fa.nestedParent().name()));
                }

                List<Order> orders = new ArrayList<>(ob.order());
                // projection has no nested field references, remove any nested orders
                if (nestedTopFields.isEmpty()) {
                    orders.removeAll(nestedOrders.values());
                }
                else {
                    // remove orders that are not ancestors of the nested projections
                    for (Entry<String, Order> entry : nestedOrders.entrySet()) {
                        String parent = entry.getKey();
                        boolean shouldKeep = false;
                        for (String topParent : nestedTopFields) {
                            if (topParent.startsWith(parent)) {
                                shouldKeep = true;
                                break;
                            }
                        }
                        if (!shouldKeep) {
                            orders.remove(entry.getValue());
                        }
                    }
                }

                // no orders left, eliminate it all-together
                if (orders.isEmpty()) {
                    return new Project(project.source(), ob.child(), project.projections());
                }

                if (orders.size() != ob.order().size()) {
                    OrderBy newOrder = new OrderBy(ob.source(), ob.child(), orders);
                    return new Project(project.source(), newOrder, project.projections());
                }
            }
            return project;
        }
    }

    static class PruneLiteralsInOrderBy extends OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy ob) {
            List<Order> prunedOrders = new ArrayList<>();

            for (Order o : ob.order()) {
                if (o.child().foldable()) {
                    prunedOrders.add(o);
                }
            }

            // everything was eliminated, the order isn't needed anymore
            if (prunedOrders.size() == ob.order().size()) {
                return ob.child();
            }
            if (prunedOrders.size() > 0) {
                List<Order> newOrders = new ArrayList<>(ob.order());
                newOrders.removeAll(prunedOrders);
                return new OrderBy(ob.source(), ob.child(), newOrders);
            }

            return ob;
        }
    }

    static class PruneOrderByForImplicitGrouping extends OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy ob) {
            Holder<Boolean> foundAggregate = new Holder<>(Boolean.FALSE);
            Holder<Boolean> foundImplicitGroupBy = new Holder<>(Boolean.FALSE);

            // if the first found aggregate has no grouping, there's no need to do ordering
            ob.forEachDown(a -> {
                // take into account
                if (foundAggregate.get() == Boolean.TRUE) {
                    return;
                }
                foundAggregate.set(Boolean.TRUE);
                if (a.groupings().isEmpty()) {
                    foundImplicitGroupBy.set(Boolean.TRUE);
                }
            }, Aggregate.class);

            if (foundImplicitGroupBy.get() == Boolean.TRUE) {
                return ob.child();
            }
            return ob;
        }
    }

    /**
     * Align the order in aggregate based on the order by.
     */
    static class SortAggregateOnOrderBy extends OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy ob) {
            List<Order> order = ob.order();

            // put the items in reverse order so the iteration happens back to front
            List<Order> nonConstant = new LinkedList<>();
            for (int i = order.size() - 1; i >= 0; i--) {
                nonConstant.add(order.get(i));
            }

            Holder<Boolean> foundAggregate = new Holder<>(Boolean.FALSE);

            // if the first found aggregate has no grouping, there's no need to do ordering
            return ob.transformDown(a -> {
                // take into account
                if (foundAggregate.get() == Boolean.TRUE) {
                    return a;
                }
                foundAggregate.set(Boolean.TRUE);

                List<Expression> groupings = new LinkedList<>(a.groupings());

                for (Order o : nonConstant) {
                    Expression fieldToOrder = o.child();
                    for (Expression group : a.groupings()) {
                        Holder<Boolean> isMatching = new Holder<>(Boolean.FALSE);
                        if (equalsAsAttribute(fieldToOrder, group)) {
                            isMatching.set(Boolean.TRUE);
                        } else {
                            a.aggregates().forEach(alias -> {
                                if (alias instanceof Alias) {
                                    Expression child = ((Alias) alias).child();
                                    // Check if the groupings (a, y) match the orderings (b, x) through the aggregates' aliases (x, y)
                                    // e.g. SELECT a AS x, b AS y ... GROUP BY a, y ORDER BY b, x
                                    if ((equalsAsAttribute(child, group)
                                            && (equalsAsAttribute(alias, fieldToOrder) || equalsAsAttribute(child, fieldToOrder)))
                                        || (equalsAsAttribute(alias, group)
                                            && (equalsAsAttribute(alias, fieldToOrder) || equalsAsAttribute(child, fieldToOrder)))) {
                                        isMatching.set(Boolean.TRUE);
                                    }
                                }
                            });
                        }

                        if (isMatching.get() == true) {
                            // move grouping in front
                            groupings.remove(group);
                            groupings.add(0, group);
                        }
                    }
                }

                if (groupings.equals(a.groupings()) == false) {
                    return new Aggregate(a.source(), a.child(), groupings, a.aggregates());
                }

                return a;
            }, Aggregate.class);
        }
    }

    static class CombineLimits extends OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.child() instanceof Limit) {
                throw new UnsupportedOperationException("not implemented yet");
            }
            throw new UnsupportedOperationException("not implemented yet");
        }
    }

    // NB: it is important to start replacing casts from the bottom to properly replace aliases
    static class PruneCast extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return rule(plan);
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            // eliminate redundant casts
            LogicalPlan transformed = plan.transformExpressionsUp(e -> {
                if (e instanceof Cast) {
                    Cast c = (Cast) e;
                    if (c.from() == c.to()) {
                        return c.field();
                    }
                }
                return e;
            });

            return transformed;
        }
    }

    static class CombineProjections extends OptimizerRule<Project> {

        CombineProjections() {
            super(TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(Project project) {
            LogicalPlan child = project.child();
            if (child instanceof Project) {
                Project p = (Project) child;
                // eliminate lower project but first replace the aliases in the upper one
                return new Project(p.source(), p.child(), combineProjections(project.projections(), p.projections()));
            }

            if (child instanceof Aggregate) {
                Aggregate a = (Aggregate) child;
                return new Aggregate(a.source(), a.child(), a.groupings(), combineProjections(project.projections(), a.aggregates()));
            }
            // if the pivot custom columns are not used, convert the project + pivot into a GROUP BY/Aggregate
            if (child instanceof Pivot) {
                Pivot p = (Pivot) child;
                if (project.outputSet().subsetOf(p.groupingSet())) {
                    return new Aggregate(p.source(), p.child(), new ArrayList<>(project.projections()), project.projections());
                }
            }
            // TODO: add rule for combining Agg/Pivot with underlying project
            return project;
        }

        // normally only the upper projections should survive but since the lower list might have aliases definitions
        // that might be reused by the upper one, these need to be replaced.
        // for example an alias defined in the lower list might be referred in the upper - without replacing it the alias becomes invalid
        private List<NamedExpression> combineProjections(List<? extends NamedExpression> upper, List<? extends NamedExpression> lower) {

            //TODO: this need rewriting when moving functions of NamedExpression

            // collect aliases in the lower list
            Map<Attribute, NamedExpression> map = new LinkedHashMap<>();
            for (NamedExpression ne : lower) {
                if ((ne instanceof Attribute) == false) {
                    map.put(ne.toAttribute(), ne);
                }
            }

            AttributeMap<NamedExpression> aliases = new AttributeMap<>(map);
            List<NamedExpression> replaced = new ArrayList<>();

            // replace any matching attribute with a lower alias (if there's a match)
            // but clean-up non-top aliases at the end
            for (NamedExpression ne : upper) {
                NamedExpression replacedExp = (NamedExpression) ne.transformUp(a -> {
                    NamedExpression as = aliases.get(a);
                    return as != null ? as : a;
                }, Attribute.class);

                replaced.add((NamedExpression) CleanAliases.trimNonTopLevelAliases(replacedExp));
            }
            return replaced;
        }
    }


    // replace attributes of foldable expressions with the foldable trees
    // SELECT 5 a, 3 + 2 b ... WHERE a < 10 ORDER BY b

    static class ReplaceFoldableAttributes extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return rule(plan);
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            Map<Attribute, Alias> aliases = new LinkedHashMap<>();
            List<Attribute> attrs = new ArrayList<>();

            // find aliases of all projections
            plan.forEachDown(p -> {
                for (NamedExpression ne : p.projections()) {
                    if (ne instanceof Alias) {
                        if (((Alias) ne).child().foldable()) {
                            Attribute attr = ne.toAttribute();
                            attrs.add(attr);
                            aliases.put(attr, (Alias) ne);
                        }
                    }
                }
            }, Project.class);

            if (attrs.isEmpty()) {
                return plan;
            }

            Holder<Boolean> stop = new Holder<>(Boolean.FALSE);

            // propagate folding up to unary nodes
            // anything higher and the propagate stops
            plan = plan.transformUp(p -> {
                if (stop.get() == Boolean.FALSE && canPropagateFoldable(p)) {
                    return p.transformExpressionsDown(e -> {
                        if (e instanceof Attribute && attrs.contains(e)) {
                            Alias as = aliases.get(e);
                            if (as == null) {
                                // might need to implement an Attribute map
                                throw new SqlIllegalArgumentException("unsupported");
                            }
                            return as;
                        }
                        return e;
                    });
                }

                if (p.children().size() > 1) {
                    stop.set(Boolean.TRUE);
                }

                return p;
            });

            // finally clean-up aliases
            return CleanAliases.INSTANCE.apply(plan);

        }

        private boolean canPropagateFoldable(LogicalPlan p) {
            return p instanceof Project
                    || p instanceof Filter
                    || p instanceof SubQueryAlias
                    || p instanceof Aggregate
                    || p instanceof Limit
                    || p instanceof OrderBy;
        }
    }

    static class FoldNull extends OptimizerExpressionRule {

        FoldNull() {
            super(TransformDirection.UP);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof IsNotNull) {
                if (((IsNotNull) e).field().nullable() == Nullability.FALSE) {
                    return new Literal(e.source(), Boolean.TRUE, DataType.BOOLEAN);
                }

            } else if (e instanceof IsNull) {
                if (((IsNull) e).field().nullable() == Nullability.FALSE) {
                    return new Literal(e.source(), Boolean.FALSE, DataType.BOOLEAN);
                }

            } else if (e instanceof In) {
                In in = (In) e;
                if (Expressions.isNull(in.value())) {
                    return Literal.of(in, null);
                }

            } else if (e instanceof Alias == false
                    && e.nullable() == Nullability.TRUE
                    && Expressions.anyMatch(e.children(), Expressions::isNull)) {
                    return Literal.of(e, null);
                }

            return e;
        }
    }

    static class ConstantFolding extends OptimizerExpressionRule {

        ConstantFolding() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(Expression e) {
            return e.foldable() ? Literal.of(e) : e;
        }
    }

    static class SimplifyConditional extends OptimizerExpressionRule {

        SimplifyConditional() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof ArbitraryConditionalFunction) {
                ArbitraryConditionalFunction c = (ArbitraryConditionalFunction) e;

                // exclude any nulls found
                List<Expression> newChildren = new ArrayList<>();
                for (Expression child : c.children()) {
                    if (Expressions.isNull(child) == false) {
                        newChildren.add(child);

                        // For Coalesce find the first non-null foldable child (if any) and break early
                        if (e instanceof Coalesce && child.foldable()) {
                            break;
                        }
                    }
                }

                if (newChildren.size() < c.children().size()) {
                    return c.replaceChildren(newChildren);
                }
            }

            return e;
        }
    }

    static class SimplifyCase extends OptimizerExpressionRule {

        SimplifyCase() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof Case) {
                Case c = (Case) e;

                // Remove or foldable conditions that fold to FALSE
                // Stop at the 1st foldable condition that folds to TRUE
                List<IfConditional> newConditions = new ArrayList<>();
                for (IfConditional conditional : c.conditions()) {
                    if (conditional.condition().foldable()) {
                        Boolean res = (Boolean) conditional.condition().fold();
                        if (res == Boolean.TRUE) {
                            newConditions.add(conditional);
                            break;
                        }
                    } else {
                        newConditions.add(conditional);
                    }
                }

                if (newConditions.size() < c.children().size()) {
                    return c.replaceChildren(combine(newConditions, c.elseResult()));
                }
            }

            return e;
        }
    }

    static class BooleanSimplification extends OptimizerExpressionRule {

        BooleanSimplification() {
            super(TransformDirection.UP);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof And || e instanceof Or) {
                return simplifyAndOr((BinaryPredicate<?, ?, ?, ?>) e);
            }
            if (e instanceof Not) {
                return simplifyNot((Not) e);
            }

            return e;
        }

        private Expression simplifyAndOr(BinaryPredicate<?, ?, ?, ?> bc) {
            Expression l = bc.left();
            Expression r = bc.right();

            if (bc instanceof And) {
                if (TRUE.equals(l)) {
                    return r;
                }
                if (TRUE.equals(r)) {
                    return l;
                }

                if (FALSE.equals(l) || FALSE.equals(r)) {
                    return new Literal(bc.source(), Boolean.FALSE, DataType.BOOLEAN);
                }
                if (l.semanticEquals(r)) {
                    return l;
                }

                //
                // common factor extraction -> (a || b) && (a || c) => a || (b && c)
                //
                List<Expression> leftSplit = splitOr(l);
                List<Expression> rightSplit = splitOr(r);

                List<Expression> common = inCommon(leftSplit, rightSplit);
                if (common.isEmpty()) {
                    return bc;
                }
                List<Expression> lDiff = subtract(leftSplit, common);
                List<Expression> rDiff = subtract(rightSplit, common);
                // (a || b || c || ... ) && (a || b) => (a || b)
                if (lDiff.isEmpty() || rDiff.isEmpty()) {
                    return combineOr(common);
                }
                // (a || b || c || ... ) && (a || b || d || ... ) => ((c || ...) && (d || ...)) || a || b
                Expression combineLeft = combineOr(lDiff);
                Expression combineRight = combineOr(rDiff);
                return combineOr(combine(common, new And(combineLeft.source(), combineLeft, combineRight)));
            }

            if (bc instanceof Or) {
                if (TRUE.equals(l) || TRUE.equals(r)) {
                    return new Literal(bc.source(), Boolean.TRUE, DataType.BOOLEAN);
                }

                if (FALSE.equals(l)) {
                    return r;
                }
                if (FALSE.equals(r)) {
                    return l;
                }

                if (l.semanticEquals(r)) {
                    return l;
                }

                //
                // common factor extraction -> (a && b) || (a && c) => a && (b || c)
                //
                List<Expression> leftSplit = splitAnd(l);
                List<Expression> rightSplit = splitAnd(r);

                List<Expression> common = inCommon(leftSplit, rightSplit);
                if (common.isEmpty()) {
                    return bc;
                }
                List<Expression> lDiff = subtract(leftSplit, common);
                List<Expression> rDiff = subtract(rightSplit, common);
                // (a || b || c || ... ) && (a || b) => (a || b)
                if (lDiff.isEmpty() || rDiff.isEmpty()) {
                    return combineAnd(common);
                }
                // (a || b || c || ... ) && (a || b || d || ... ) => ((c || ...) && (d || ...)) || a || b
                Expression combineLeft = combineAnd(lDiff);
                Expression combineRight = combineAnd(rDiff);
                return combineAnd(combine(common, new Or(combineLeft.source(), combineLeft, combineRight)));
            }

            // TODO: eliminate conjunction/disjunction
            return bc;
        }

        @SuppressWarnings("rawtypes")
        private Expression simplifyNot(Not n) {
            Expression c = n.field();

            if (TRUE.semanticEquals(c)) {
                return new Literal(n.source(), Boolean.FALSE, DataType.BOOLEAN);
            }
            if (FALSE.semanticEquals(c)) {
                return new Literal(n.source(), Boolean.TRUE, DataType.BOOLEAN);
            }

            if (c instanceof Negatable) {
                return ((Negatable) c).negate();
            }

            if (c instanceof Not) {
                return ((Not) c).field();
            }

            return n;
        }
    }

    static class BinaryComparisonSimplification extends OptimizerExpressionRule {

        BinaryComparisonSimplification() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(Expression e) {
            return e instanceof BinaryComparison ? simplify((BinaryComparison) e) : e;
        }

        private Expression simplify(BinaryComparison bc) {
            Expression l = bc.left();
            Expression r = bc.right();

            // true for equality
            if (bc instanceof Equals || bc instanceof GreaterThanOrEqual || bc instanceof LessThanOrEqual) {
                if (l.nullable() == Nullability.FALSE && r.nullable() == Nullability.FALSE && l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.TRUE, DataType.BOOLEAN);
                }
            }
            if (bc instanceof NullEquals) {
                if (l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.TRUE, DataType.BOOLEAN);
                }
                if (Expressions.isNull(r)) {
                    return new IsNull(bc.source(), l);
                }
            }

            // false for equality
            if (bc instanceof NotEquals || bc instanceof GreaterThan || bc instanceof LessThan) {
                if (l.nullable() == Nullability.FALSE && r.nullable() == Nullability.FALSE && l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.FALSE, DataType.BOOLEAN);
                }
            }

            return bc;
        }
    }

    static class BooleanLiteralsOnTheRight extends OptimizerExpressionRule {

        BooleanLiteralsOnTheRight() {
            super(TransformDirection.UP);
        }

        @Override
        protected Expression rule(Expression e) {
            return e instanceof BinaryOperator ? literalToTheRight((BinaryOperator<?, ?, ?, ?>) e) : e;
        }

        private Expression literalToTheRight(BinaryOperator<?, ?, ?, ?> be) {
            return be.left() instanceof Literal && !(be.right() instanceof Literal) ? be.swapLeftAndRight() : be;
        }
    }

    /**
     * Propagate Equals to eliminate conjuncted Ranges or BinaryComparisons.
     * When encountering a different Equals, non-containing {@link Range} or {@link BinaryComparison}, the conjunction becomes false.
     * When encountering a containing {@link Range}, {@link BinaryComparison} or {@link NotEquals}, these get eliminated by the equality.
     *
     * Since this rule can eliminate Ranges and BinaryComparisons, it should be applied before {@link CombineBinaryComparisons}.
     *
     * This rule doesn't perform any promotion of {@link BinaryComparison}s, that is handled by
     * {@link CombineBinaryComparisons} on purpose as the resulting Range might be foldable
     * (which is picked by the folding rule on the next run).
     */
    static class PropagateEquals extends OptimizerExpressionRule {

        PropagateEquals() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof And) {
                return propagate((And) e);
            } else if (e instanceof Or) {
                return propagate((Or) e);
            }
            return e;
        }

        // combine conjunction
        private Expression propagate(And and) {
            List<Range> ranges = new ArrayList<>();
            // Only equalities, not-equalities and inequalities with a foldable .right are extracted separately;
            // the others go into the general 'exps'.
            List<BinaryComparison> equals = new ArrayList<>();
            List<NotEquals> notEquals = new ArrayList<>();
            List<BinaryComparison> inequalities = new ArrayList<>();
            List<Expression> exps = new ArrayList<>();

            boolean changed = false;

            for (Expression ex : Predicates.splitAnd(and)) {
                if (ex instanceof Range) {
                    ranges.add((Range) ex);
                } else if (ex instanceof Equals || ex instanceof NullEquals) {
                    BinaryComparison otherEq = (BinaryComparison) ex;
                    // equals on different values evaluate to FALSE
                    if (otherEq.right().foldable()) {
                        for (BinaryComparison eq : equals) {
                            if (otherEq.left().semanticEquals(eq.left())) {
                                    Integer comp = BinaryComparison.compare(eq.right().fold(), otherEq.right().fold());
                                    if (comp != null) {
                                        // var cannot be equal to two different values at the same time
                                        if (comp != 0) {
                                            return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
                                        }
                                    }
                                }
                            }
                        equals.add(otherEq);
                    } else {
                        exps.add(otherEq);
                        }
                } else if (ex instanceof GreaterThan || ex instanceof GreaterThanOrEqual ||
                    ex instanceof LessThan || ex instanceof LessThanOrEqual) {
                    BinaryComparison bc = (BinaryComparison) ex;
                    if (bc.right().foldable()) {
                        inequalities.add(bc);
                    } else {
                        exps.add(ex);
                    }
                } else if (ex instanceof NotEquals) {
                    NotEquals otherNotEq = (NotEquals) ex;
                    if (otherNotEq.right().foldable()) {
                        notEquals.add(otherNotEq);
                } else {
                    exps.add(ex);
                }
                } else {
                    exps.add(ex);
            }
            }

            // check
            for (BinaryComparison eq : equals) {
                Object eqValue = eq.right().fold();

                for (int i = 0; i < ranges.size(); i++) {
                    Range range = ranges.get(i);

                    if (range.value().semanticEquals(eq.left())) {
                        // if equals is outside the interval, evaluate the whole expression to FALSE
                        if (range.lower().foldable()) {
                            Integer compare = BinaryComparison.compare(range.lower().fold(), eqValue);
                            if (compare != null && (
                                 // eq outside the lower boundary
                                 compare > 0 ||
                                 // eq matches the boundary but should not be included
                                 (compare == 0 && !range.includeLower()))
                            ) {
                                return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
                            }
                        }
                        if (range.upper().foldable()) {
                            Integer compare = BinaryComparison.compare(range.upper().fold(), eqValue);
                            if (compare != null && (
                                 // eq outside the upper boundary
                                 compare < 0 ||
                                 // eq matches the boundary but should not be included
                                 (compare == 0 && !range.includeUpper()))
                            ) {
                                return new Literal(and.source(), Boolean.FALSE, DataType.BOOLEAN);
                            }
                        }

                        // it's in the range and thus, remove it
                        ranges.remove(i);
                        changed = true;
                    }
                }

                // evaluate all NotEquals against the Equal
                for (Iterator<NotEquals> iter = notEquals.iterator(); iter.hasNext(); ) {
                    NotEquals neq = iter.next();
                    if (eq.left().semanticEquals(neq.left())) {
                        Integer comp = BinaryComparison.compare(eqValue, neq.right().fold());
                        if (comp != null) {
                            if (comp == 0) {
                                return FALSE; // clashing and conflicting: a = 1 AND a != 1
                            } else {
                                iter.remove(); // clashing and redundant: a = 1 AND a != 2
                                changed = true;
            }
                        }
                    }
                }

                // evaluate all inequalities against the Equal
                for (Iterator<BinaryComparison> iter = inequalities.iterator(); iter.hasNext(); ) {
                    BinaryComparison bc = iter.next();
                    if (eq.left().semanticEquals(bc.left())) {
                        Integer compare = BinaryComparison.compare(eqValue, bc.right().fold());
                        if (compare != null) {
                            if (bc instanceof LessThan || bc instanceof LessThanOrEqual) { // a = 2 AND a </<= ?
                                if ((compare == 0 && bc instanceof LessThan) || // a = 2 AND a < 2
                                    0 < compare) { // a = 2 AND a </<= 1
                                    return FALSE;
        }
                            } else if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) { // a = 2 AND a >/>= ?
                                if ((compare == 0 && bc instanceof GreaterThan) || // a = 2 AND a > 2
                                    compare < 0) { // a = 2 AND a >/>= 3
                                    return FALSE;
    }
                            }

                            iter.remove();
                            changed = true;
                        }
                    }
                }
            }

            return changed ? Predicates.combineAnd(CollectionUtils.combine(exps, equals, notEquals, inequalities, ranges)) : and;
        }

        // combine disjunction:
        // a = 2 OR a > 3 -> nop; a = 2 OR a > 1 -> a > 1
        // a = 2 OR a < 3 -> a < 3; a = 2 OR a < 1 -> nop
        // a = 2 OR 3 < a < 5 -> nop; a = 2 OR 1 < a < 3 -> 1 < a < 3; a = 2 OR 0 < a < 1 -> nop
        // a = 2 OR a != 2 -> TRUE; a = 2 OR a = 5 -> nop; a = 2 OR a != 5 -> a != 5
        private Expression propagate(Or or) {
            List<Expression> exps = new ArrayList<>();
            List<Equals> equals = new ArrayList<>(); // foldable right term Equals
            List<NotEquals> notEquals = new ArrayList<>(); // foldable right term NotEquals
            List<Range> ranges = new ArrayList<>();
            List<BinaryComparison> inequalities = new ArrayList<>(); // foldable right term (=limit) BinaryComparision

            // split expressions by type
            for (Expression ex : Predicates.splitOr(or)) {
                if (ex instanceof Equals) {
                    Equals eq = (Equals) ex;
                    if (eq.right().foldable()) {
                        equals.add(eq);
                    } else {
                        exps.add(ex);
                    }
                } else if (ex instanceof NotEquals) {
                    NotEquals neq = (NotEquals) ex;
                    if (neq.right().foldable()) {
                        notEquals.add(neq);
                    } else {
                        exps.add(ex);
                    }
                } else if (ex instanceof Range) {
                    ranges.add((Range) ex);
                } else if (ex instanceof BinaryComparison) {
                    BinaryComparison bc = (BinaryComparison) ex;
                    if (bc.right().foldable()) {
                        inequalities.add(bc);
                    } else {
                        exps.add(ex);
                    }
                } else {
                    exps.add(ex);
                }
            }

            boolean updated = false; // has the expression been modified?

            // evaluate the impact of each Equal over the different types of Expressions
            for (Iterator<Equals> iterEq = equals.iterator(); iterEq.hasNext(); ) {
                Equals eq = iterEq.next();
                Object eqValue = eq.right().fold();
                boolean removeEquals = false;

                // Equals OR NotEquals
                for (NotEquals neq : notEquals) {
                    if (eq.left().semanticEquals(neq.left())) { // a = 2 OR a != ? -> ...
                        Integer comp = BinaryComparison.compare(eqValue, neq.right().fold());
                        if (comp != null) {
                            if (comp == 0) { // a = 2 OR a != 2 -> TRUE
                                return TRUE;
                            } else { // a = 2 OR a != 5 -> a != 5
                                removeEquals = true;
                                break;
                            }
                        }
                    }
                }
                if (removeEquals) {
                    iterEq.remove();
                    updated = true;
                    continue;
                }

                // Equals OR Range
                for (int i = 0; i < ranges.size(); i ++) { // might modify list, so use index loop
                    Range range = ranges.get(i);
                    if (eq.left().semanticEquals(range.value())) {
                        Integer lowerComp = range.lower().foldable() ? BinaryComparison.compare(eqValue, range.lower().fold()) : null;
                        Integer upperComp = range.upper().foldable() ? BinaryComparison.compare(eqValue, range.upper().fold()) : null;

                        if (lowerComp != null && lowerComp == 0) {
                            if (!range.includeLower()) { // a = 2 OR 2 < a < ? -> 2 <= a < ?
                                ranges.set(i, new Range(range.source(), range.value(), range.lower(), true,
                                    range.upper(), range.includeUpper()));
                            } // else : a = 2 OR 2 <= a < ? -> 2 <= a < ?
                            removeEquals = true; // update range with lower equality instead or simply superfluous
                            break;
                        } else if (upperComp != null && upperComp == 0) {
                            if (!range.includeUpper()) { // a = 2 OR ? < a < 2 -> ? < a <= 2
                                ranges.set(i, new Range(range.source(), range.value(), range.lower(), range.includeLower(),
                                    range.upper(), true));
                            } // else : a = 2 OR ? < a <= 2 -> ? < a <= 2
                            removeEquals = true; // update range with upper equality instead
                            break;
                        } else if (lowerComp != null && upperComp != null) {
                            if (0 < lowerComp && upperComp < 0) { // a = 2 OR 1 < a < 3
                                removeEquals = true; // equality is superfluous
                                break;
                            }
                        }
                    }
                }
                if (removeEquals) {
                    iterEq.remove();
                    updated = true;
                    continue;
                }

                // Equals OR Inequality
                for (int i = 0; i < inequalities.size(); i ++) {
                    BinaryComparison bc = inequalities.get(i);
                    if (eq.left().semanticEquals(bc.left())) {
                        Integer comp = BinaryComparison.compare(eqValue, bc.right().fold());
                        if (comp != null) {
                            if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) {
                                if (comp < 0) { // a = 1 OR a > 2 -> nop
                                    continue;
                                } else if (comp == 0 && bc instanceof GreaterThan) { // a = 2 OR a > 2 -> a >= 2
                                    inequalities.set(i, new GreaterThanOrEqual(bc.source(), bc.left(), bc.right()));
                                } // else (0 < comp || bc instanceof GreaterThanOrEqual) :
                                // a = 3 OR a > 2 -> a > 2; a = 2 OR a => 2 -> a => 2

                                removeEquals = true; // update range with equality instead or simply superfluous
                                break;
                            } else if (bc instanceof LessThan || bc instanceof LessThanOrEqual) {
                                if (comp > 0) { // a = 2 OR a < 1 -> nop
                                    continue;
                                }
                                if (comp == 0 && bc instanceof LessThan) { // a = 2 OR a < 2 -> a <= 2
                                    inequalities.set(i, new LessThanOrEqual(bc.source(), bc.left(), bc.right()));
                                } // else (comp < 0 || bc instanceof LessThanOrEqual) : a = 2 OR a < 3 -> a < 3; a = 2 OR a <= 2 -> a <= 2
                                removeEquals = true; // update range with equality instead or simply superfluous
                                break;
                            }
                        }
                    }
                }
                if (removeEquals) {
                    iterEq.remove();
                    updated = true;
                }
            }

            return updated ? Predicates.combineOr(CollectionUtils.combine(exps, equals, notEquals, inequalities, ranges)) : or;
        }
    }

    static class CombineBinaryComparisons extends OptimizerExpressionRule {

        CombineBinaryComparisons() {
            super(TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof And) {
                return combine((And) e);
            } else if (e instanceof Or) {
                return combine((Or) e);
            }
            return e;
        }

        // combine conjunction
        private Expression combine(And and) {
            List<Range> ranges = new ArrayList<>();
            List<BinaryComparison> bcs = new ArrayList<>();
            List<Expression> exps = new ArrayList<>();

            boolean changed = false;

            List<Expression> andExps = Predicates.splitAnd(and);
            // Ranges need to show up before BinaryComparisons in list, to allow the latter be optimized away into a Range, if possible
            andExps.sort((o1, o2) -> {
                if (o1 instanceof Range && o2 instanceof Range) {
                    return 0; // keep ranges' order
                } else if (o1 instanceof Range || o2 instanceof Range) {
                    return o2 instanceof Range ? 1 : -1;
                } else {
                    return 0; // keep non-ranges' order
                }
            });
            for (Expression ex : andExps) {
                if (ex instanceof Range) {
                    Range r = (Range) ex;
                    if (findExistingRange(r, ranges, true)) {
                        changed = true;
                    } else {
                        ranges.add(r);
                    }
                } else if (ex instanceof BinaryComparison && !(ex instanceof Equals)) {
                    BinaryComparison bc = (BinaryComparison) ex;

                    if (bc.right().foldable() && (findConjunctiveComparisonInRange(bc, ranges) || findExistingComparison(bc, bcs, true))) {
                        changed = true;
                    } else {
                        bcs.add(bc);
                    }
                } else {
                    exps.add(ex);
                }
            }

            // finally try combining any left BinaryComparisons into possible Ranges
            // this could be a different rule but it's clearer here wrt the order of comparisons

            for (int i = 0; i < bcs.size() - 1; i++) {
                BinaryComparison main = bcs.get(i);

                for (int j = i + 1; j < bcs.size(); j++) {
                    BinaryComparison other = bcs.get(j);

                    if (main.left().semanticEquals(other.left())) {
                        // >/>= AND </<=
                        if ((main instanceof GreaterThan || main instanceof GreaterThanOrEqual)
                                && (other instanceof LessThan || other instanceof LessThanOrEqual)) {
                            bcs.remove(j);
                            bcs.remove(i);

                            ranges.add(new Range(and.source(), main.left(),
                                    main.right(), main instanceof GreaterThanOrEqual,
                                    other.right(), other instanceof LessThanOrEqual));

                            changed = true;
                        }
                        // </<= AND >/>=
                        else if ((other instanceof GreaterThan || other instanceof GreaterThanOrEqual)
                                && (main instanceof LessThan || main instanceof LessThanOrEqual)) {
                                    bcs.remove(j);
                                    bcs.remove(i);

                            ranges.add(new Range(and.source(), main.left(),
                                    other.right(), other instanceof GreaterThanOrEqual,
                                    main.right(), main instanceof LessThanOrEqual));

                                    changed = true;
                        }
                    }
                }
            }


            return changed ? Predicates.combineAnd(CollectionUtils.combine(exps, bcs, ranges)) : and;
        }

        // combine disjunction
        private Expression combine(Or or) {
            List<BinaryComparison> bcs = new ArrayList<>();
            List<Range> ranges = new ArrayList<>();
            List<Expression> exps = new ArrayList<>();

            boolean changed = false;

            for (Expression ex : Predicates.splitOr(or)) {
                if (ex instanceof Range) {
                    Range r = (Range) ex;
                    if (findExistingRange(r, ranges, false)) {
                        changed = true;
                    } else {
                        ranges.add(r);
                    }
                } else if (ex instanceof BinaryComparison) {
                    BinaryComparison bc = (BinaryComparison) ex;
                    if (bc.right().foldable() && findExistingComparison(bc, bcs, false)) {
                        changed = true;
                    } else {
                        bcs.add(bc);
                    }
                } else {
                    exps.add(ex);
                }
            }

            return changed ? Predicates.combineOr(CollectionUtils.combine(exps, bcs, ranges)) : or;
        }

        private static boolean findExistingRange(Range main, List<Range> ranges, boolean conjunctive) {
            if (!main.lower().foldable() && !main.upper().foldable()) {
                return false;
            }
            // NB: the loop modifies the list (hence why the int is used)
            for (int i = 0; i < ranges.size(); i++) {
                Range other = ranges.get(i);

                if (main.value().semanticEquals(other.value())) {

                    // make sure the comparison was done
                    boolean compared = false;

                    boolean lower = false;
                    boolean upper = false;
                    // boundary equality (useful to differentiate whether a range is included or not)
                    // and thus whether it should be preserved or ignored
                    boolean lowerEq = false;
                    boolean upperEq = false;

                    // evaluate lower
                    if (main.lower().foldable() && other.lower().foldable()) {
                        compared = true;

                        Integer comp = BinaryComparison.compare(main.lower().fold(), other.lower().fold());
                        // values are comparable
                        if (comp != null) {
                            // boundary equality
                            lowerEq = comp == 0 && main.includeLower() == other.includeLower();
                            // AND
                            if (conjunctive) {
                                // (2 < a < 3) AND (1 < a < 3) -> (2 < a < 3)
                                lower = comp > 0 ||
                                // (2 < a < 3) AND (2 <= a < 3) -> (2 < a < 3)
                                        (comp == 0 && !main.includeLower() && other.includeLower());
                            }
                            // OR
                            else {
                                // (1 < a < 3) OR (2 < a < 3) -> (1 < a < 3)
                                lower = comp < 0 ||
                                // (2 <= a < 3) OR (2 < a < 3) -> (2 <= a < 3)
                                        (comp == 0 && main.includeLower() && !other.includeLower()) || lowerEq;
                            }
                        }
                    }
                    // evaluate upper
                    if (main.upper().foldable() && other.upper().foldable()) {
                        compared = true;

                        Integer comp = BinaryComparison.compare(main.upper().fold(), other.upper().fold());
                        // values are comparable
                        if (comp != null) {
                            // boundary equality
                            upperEq = comp == 0 && main.includeUpper() == other.includeUpper();

                            // AND
                            if (conjunctive) {
                                // (1 < a < 2) AND (1 < a < 3) -> (1 < a < 2)
                                upper = comp < 0 ||
                                // (1 < a < 2) AND (1 < a <= 2) -> (1 < a < 2)
                                        (comp == 0 && !main.includeUpper() && other.includeUpper());
                            }
                            // OR
                            else {
                                // (1 < a < 3) OR (1 < a < 2) -> (1 < a < 3)
                                upper = comp > 0 ||
                                // (1 < a <= 3) OR (1 < a < 3) -> (2 < a < 3)
                                        (comp == 0 && main.includeUpper() && !other.includeUpper()) || upperEq;
                            }
                        }
                    }

                    // AND - at least one of lower or upper
                    if (conjunctive) {
                        // can tighten range
                        if (lower || upper) {
                            ranges.remove(i);
                            ranges.add(i,
                                    new Range(main.source(), main.value(),
                                            lower ? main.lower() : other.lower(),
                                            lower ? main.includeLower() : other.includeLower(),
                                            upper ? main.upper() : other.upper(),
                                            upper ? main.includeUpper() : other.includeUpper()));
                        }

                        // range was comparable
                        return compared;
                    }
                    // OR - needs both upper and lower to loosen range
                    else {
                        // can loosen range
                        if (lower && upper) {
                            ranges.remove(i);
                            ranges.add(i,
                                    new Range(main.source(), main.value(),
                                            lower ? main.lower() : other.lower(),
                                            lower ? main.includeLower() : other.includeLower(),
                                            upper ? main.upper() : other.upper(),
                                            upper ? main.includeUpper() : other.includeUpper()));
                            return true;
                        }

                        // if the range in included, no need to add it
                        return compared && (!((lower && !lowerEq) || (upper && !upperEq)));
                    }
                }
            }
            return false;
        }

        private boolean findConjunctiveComparisonInRange(BinaryComparison main, List<Range> ranges) {
            Object value = main.right().fold();

            // NB: the loop modifies the list (hence why the int is used)
            for (int i = 0; i < ranges.size(); i++) {
                Range other = ranges.get(i);

                if (main.left().semanticEquals(other.value())) {

                    if (main instanceof GreaterThan || main instanceof GreaterThanOrEqual) {
                        if (other.lower().foldable()) {
                            Integer comp = BinaryComparison.compare(value, other.lower().fold());
                            if (comp != null) {
                                // 2 < a AND (2 <= a < 3) -> 2 < a < 3
                                boolean lowerEq = comp == 0 && other.includeLower() && main instanceof GreaterThan;
                                // 2 < a AND (1 < a < 3) -> 2 < a < 3
                                boolean lower = comp > 0 || lowerEq;

                                if (lower) {
                                    ranges.remove(i);
                                    ranges.add(i,
                                            new Range(other.source(), other.value(),
                                                    main.right(), lowerEq ? false : main instanceof GreaterThanOrEqual,
                                                    other.upper(), other.includeUpper()));
                                }

                                // found a match
                                return true;
                            }
                        }
                    } else if (main instanceof LessThan || main instanceof LessThanOrEqual) {
                        if (other.upper().foldable()) {
                            Integer comp = BinaryComparison.compare(value, other.upper().fold());
                            if (comp != null) {
                                // a < 2 AND (1 < a <= 2) -> 1 < a < 2
                                boolean upperEq = comp == 0 && other.includeUpper() && main instanceof LessThan;
                                // a < 2 AND (1 < a < 3) -> 1 < a < 2
                                boolean upper = comp < 0 || upperEq;

                                if (upper) {
                                    ranges.remove(i);
                                    ranges.add(i, new Range(other.source(), other.value(),
                                            other.lower(), other.includeLower(),
                                            main.right(), upperEq ? false : main instanceof LessThanOrEqual));
                                }

                                // found a match
                                return true;
                            }
                        }
                    }

                    return false;
                }
            }
            return false;
        }

        /**
         * Find commonalities between the given comparison in the given list.
         * The method can be applied both for conjunctive (AND) or disjunctive purposes (OR).
         */
        private static boolean findExistingComparison(BinaryComparison main, List<BinaryComparison> bcs, boolean conjunctive) {
            Object value = main.right().fold();

            // NB: the loop modifies the list (hence why the int is used)
            for (int i = 0; i < bcs.size(); i++) {
                BinaryComparison other = bcs.get(i);
                // skip if cannot evaluate
                if (!other.right().foldable()) {
                    continue;
                }
                // if bc is a higher/lower value or gte vs gt, use it instead
                if ((other instanceof GreaterThan || other instanceof GreaterThanOrEqual) &&
                    (main instanceof GreaterThan || main instanceof GreaterThanOrEqual)) {

                    if (main.left().semanticEquals(other.left())) {
                        Integer compare = BinaryComparison.compare(value, other.right().fold());

                        if (compare != null) {
                            // AND
                            if ((conjunctive &&
                            // a > 3 AND a > 2 -> a > 3
                                    (compare > 0 ||
                                    // a > 2 AND a >= 2 -> a > 2
                                            (compare == 0 && main instanceof GreaterThan && other instanceof GreaterThanOrEqual)))
                                    ||
                                    // OR
                                    (!conjunctive &&
                                    // a > 2 OR a > 3 -> a > 2
                                            (compare < 0 ||
                                            // a >= 2 OR a > 2 -> a >= 2
                                  (compare == 0 && main instanceof GreaterThanOrEqual && other instanceof GreaterThan)))) {
                                bcs.remove(i);
                                bcs.add(i, main);
                            }
                            // found a match
                            return true;
                        }

                        return false;
                    }
                }
                // if bc is a lower/higher value or lte vs lt, use it instead
                else if ((other instanceof LessThan || other instanceof LessThanOrEqual) &&
                        (main instanceof LessThan || main instanceof LessThanOrEqual)) {

                            if (main.left().semanticEquals(other.left())) {
                                Integer compare = BinaryComparison.compare(value, other.right().fold());

                                if (compare != null) {
                                    // AND
                                    if ((conjunctive &&
                                    // a < 2 AND a < 3 -> a < 2
                                    (compare < 0 ||
                                    // a < 2 AND a <= 2 -> a < 2
                                  (compare == 0 && main instanceof LessThan && other instanceof LessThanOrEqual)))
                                ||
                                    // OR
                                    (!conjunctive &&
                                    // a < 2 OR a < 3 -> a < 3
                                    (compare > 0 ||
                                    // a <= 2 OR a < 2 -> a <= 2
                                    (compare == 0 && main instanceof LessThanOrEqual && other instanceof LessThan)))) {
                                        bcs.remove(i);
                                        bcs.add(i, main);

                                    }
                                    // found a match
                                    return true;
                                }

                                return false;
                            }
                        }
            }

            return false;
        }
    }


    static class ReplaceAggsWithMatrixStats extends OptimizerBasicRule {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            // minimal reuse of the same matrix stat object
            final Map<Expression, MatrixStats> seen = new LinkedHashMap<>();

            return p.transformExpressionsUp(e -> {
                if (e instanceof MatrixStatsEnclosed) {
                    AggregateFunction f = (AggregateFunction) e;

                    Expression argument = f.field();
                    MatrixStats matrixStats = seen.get(argument);

                    if (matrixStats == null) {
                        Source source = new Source(f.sourceLocation(), "MATRIX(" + argument.sourceText() + ")");
                        matrixStats = new MatrixStats(source, argument);
                        seen.put(argument, matrixStats);
                    }

                    InnerAggregate ia = new InnerAggregate(f.source(), f, matrixStats, argument);
                    return ia;
                }

                return e;
            });
        }
    }

    static class ReplaceAggsWithExtendedStats extends OptimizerBasicRule {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            // minimal reuse of the same matrix stat object
            final Map<Expression, ExtendedStats> seen = new LinkedHashMap<>();

            return p.transformExpressionsUp(e -> {
                if (e instanceof ExtendedStatsEnclosed) {
                    AggregateFunction f = (AggregateFunction) e;

                    Expression argument = f.field();
                    ExtendedStats extendedStats = seen.get(argument);

                    if (extendedStats == null) {
                        Source source = new Source(f.sourceLocation(), "EXT_STATS(" + argument.sourceText() + ")");
                        extendedStats = new ExtendedStats(source, argument);
                        seen.put(argument, extendedStats);
                    }

                    InnerAggregate ia = new InnerAggregate(f, extendedStats);
                    return ia;
                }

                return e;
            });
        }
    }

    static class ReplaceAggsWithStats extends OptimizerBasicRule {

        private static class Match {
            final Stats stats;
            private final Set<Class<? extends AggregateFunction>> functionTypes = new LinkedHashSet<>();
            private Map<Class<? extends AggregateFunction>, InnerAggregate> innerAggs = null;

            Match(Stats stats) {
                this.stats = stats;
            }

            @Override
            public String toString() {
                return stats.toString();
            }

            public void add(Class<? extends AggregateFunction> aggType) {
                functionTypes.add(aggType);
            }

            // if the stat has at least two different functions for it, promote it as stat
            // also keep the promoted function around for reuse
            public AggregateFunction maybePromote(AggregateFunction agg) {
                if (functionTypes.size() > 1) {
                    if (innerAggs == null) {
                        innerAggs = new LinkedHashMap<>();
                    }
                    return innerAggs.computeIfAbsent(agg.getClass(), k -> new InnerAggregate(agg, stats));
                }
                return agg;
            }
        }

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            // 1. first check whether there are at least 2 aggs for the same fields so that there can be a promotion
            final Map<Expression, Match> potentialPromotions = new LinkedHashMap<>();

            p.forEachExpressionsUp(e -> {
                if (Stats.isTypeCompatible(e)) {
                    AggregateFunction f = (AggregateFunction) e;

                    Expression argument = f.field();
                    Match match = potentialPromotions.get(argument);

                    if (match == null) {
                        Source source = new Source(f.sourceLocation(), "STATS(" + argument.sourceText() + ")");
                        match = new Match(new Stats(source, argument));
                        potentialPromotions.put(argument, match);
                    }
                    match.add(f.getClass());
                }
            });

            // no promotions found - skip
            if (potentialPromotions.isEmpty()) {
                return p;
            }

            // start promotion

            // 2. promote aggs to InnerAggs
            return p.transformExpressionsUp(e -> {
                if (Stats.isTypeCompatible(e)) {
                    AggregateFunction f = (AggregateFunction) e;

                    Expression argument = f.field();
                    Match match = potentialPromotions.get(argument);

                    if (match != null) {
                        return match.maybePromote(f);
                    }
                }
                return e;
            });
        }
    }

    static class PromoteStatsToExtendedStats extends OptimizerBasicRule {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            final Map<Expression, ExtendedStats> seen = new LinkedHashMap<>();

            // count the extended stats
            p.forEachExpressionsUp(e -> {
                if (e instanceof InnerAggregate) {
                    InnerAggregate ia = (InnerAggregate) e;
                    if (ia.outer() instanceof ExtendedStats) {
                        ExtendedStats extStats = (ExtendedStats) ia.outer();
                        seen.putIfAbsent(extStats.field(), extStats);
                    }
                }
            });

            // then if there's a match, replace the stat inside the InnerAgg
            return p.transformExpressionsUp(e -> {
                if (e instanceof InnerAggregate) {
                    InnerAggregate ia = (InnerAggregate) e;
                    if (ia.outer() instanceof Stats) {
                        Stats stats = (Stats) ia.outer();
                        ExtendedStats ext = seen.get(stats.field());
                        if (ext != null && stats.field().equals(ext.field())) {
                            return new InnerAggregate(ia.inner(), ext);
                        }
                    }
                }

                return e;
            });
        }
    }

    static class ReplaceAggsWithPercentiles extends OptimizerBasicRule {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            // percentile per field/expression
            Map<Expression, Set<Expression>> percentsPerField = new LinkedHashMap<>();

            // count gather the percents for each field
            p.forEachExpressionsUp(e -> {
                if (e instanceof Percentile) {
                    Percentile per = (Percentile) e;
                    Expression field = per.field();
                    Set<Expression> percentiles = percentsPerField.get(field);

                    if (percentiles == null) {
                        percentiles = new LinkedHashSet<>();
                        percentsPerField.put(field, percentiles);
                    }

                    percentiles.add(per.percent());
                }
            });

            Map<Expression, Percentiles> percentilesPerField = new LinkedHashMap<>();
            // create a Percentile agg for each field (and its associated percents)
            percentsPerField.forEach((k, v) -> {
                percentilesPerField.put(k, new Percentiles(v.iterator().next().source(), k, new ArrayList<>(v)));
            });

            return p.transformExpressionsUp(e -> {
                if (e instanceof Percentile) {
                    Percentile per = (Percentile) e;
                    Percentiles percentiles = percentilesPerField.get(per.field());
                    return new InnerAggregate(per, percentiles);
                }

                return e;
            });
        }
    }

    static class ReplaceAggsWithPercentileRanks extends OptimizerBasicRule {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            // percentile per field/expression
            final Map<Expression, Set<Expression>> percentPerField = new LinkedHashMap<>();

            // count gather the percents for each field
            p.forEachExpressionsUp(e -> {
                if (e instanceof PercentileRank) {
                    PercentileRank per = (PercentileRank) e;
                    Expression field = per.field();
                    Set<Expression> percentiles = percentPerField.get(field);

                    if (percentiles == null) {
                        percentiles = new LinkedHashSet<>();
                        percentPerField.put(field, percentiles);
                    }

                    percentiles.add(per.value());
                }
            });

            Map<Expression, PercentileRanks> ranksPerField = new LinkedHashMap<>();
            // create a PercentileRanks agg for each field (and its associated values)
            percentPerField.forEach((k, v) -> {
                ranksPerField.put(k, new PercentileRanks(v.iterator().next().source(), k, new ArrayList<>(v)));
            });

            return p.transformExpressionsUp(e -> {
                if (e instanceof PercentileRank) {
                    PercentileRank per = (PercentileRank) e;
                    PercentileRanks ranks = ranksPerField.get(per.field());
                    return new InnerAggregate(per, ranks);
                }

                return e;
            });
        }
    }

    static class ReplaceMinMaxWithTopHits extends OptimizerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            Map<Expression, TopHits> mins = new HashMap<>();
            Map<Expression, TopHits> maxs = new HashMap<>();
            return plan.transformExpressionsDown(e -> {
                if (e instanceof Min) {
                    Min min = (Min) e;
                    if (min.field().dataType().isString()) {
                        return mins.computeIfAbsent(min.field(), k -> new First(min.source(), k, null));
                    }
                }
                if (e instanceof Max) {
                    Max max = (Max) e;
                    if (max.field().dataType().isString()) {
                        return maxs.computeIfAbsent(max.field(), k -> new Last(max.source(), k, null));
                    }
                }
                return e;
            });
        }
    }

    static class PruneFilters extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            Expression condition = filter.condition().transformUp(PruneFilters::foldBinaryLogic);

            if (condition instanceof Literal) {
                if (TRUE.equals(condition)) {
                    return filter.child();
                }
                if (FALSE.equals(condition) || Expressions.isNull(condition)) {
                    return new LocalRelation(filter.source(), new EmptyExecutable(filter.output()));
                }
            }

            if (!condition.equals(filter.condition())) {
                return new Filter(filter.source(), filter.child(), condition);
            }
            return filter;
        }

        private static Expression foldBinaryLogic(Expression expression) {
            if (expression instanceof Or) {
                Or or = (Or) expression;
                boolean nullLeft = Expressions.isNull(or.left());
                boolean nullRight = Expressions.isNull(or.right());
                if (nullLeft && nullRight) {
                    return new Literal(expression.source(), null, DataType.NULL);
                }
                if (nullLeft) {
                    return or.right();
                }
                if (nullRight) {
                    return or.left();
                }
            }
            if (expression instanceof And) {
                And and = (And) expression;
                if (Expressions.isNull(and.left()) || Expressions.isNull(and.right())) {
                    return new Literal(expression.source(), null, DataType.NULL);
                }
            }
            return expression;
        }
    }


    static class SkipQueryOnLimitZero extends OptimizerRule<Limit> {
        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.limit() instanceof Literal) {
                if (Integer.valueOf(0).equals((limit.limit().fold()))) {
                    return new LocalRelation(limit.source(), new EmptyExecutable(limit.output()));
                }
            }
            return limit;
        }
    }

    static class SkipQueryIfFoldingProjection extends OptimizerRule<LogicalPlan> {
        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            Holder<LocalRelation> optimizedPlan = new Holder<>();
            plan.forEachDown(p -> {
                List<Object> values = extractConstants(p.projections());
                if (values.size() == p.projections().size() && !(p.child() instanceof EsRelation) &&
                    isNotQueryWithFromClauseAndFilterFoldedToFalse(p)) {
                    optimizedPlan.set(new LocalRelation(p.source(), new SingletonExecutable(p.output(), values.toArray())));
                }
            }, Project.class);

            if (optimizedPlan.get() != null) {
                return optimizedPlan.get();
            }

            plan.forEachDown(a -> {
                List<Object> values = extractConstants(a.aggregates());
                if (values.size() == a.aggregates().size() && isNotQueryWithFromClauseAndFilterFoldedToFalse(a)) {
                    optimizedPlan.set(new LocalRelation(a.source(), new SingletonExecutable(a.output(), values.toArray())));
                }
            }, Aggregate.class);

            if (optimizedPlan.get() != null) {
                return optimizedPlan.get();
            }

            return plan;
        }

        private List<Object> extractConstants(List<? extends NamedExpression> named) {
            List<Object> values = new ArrayList<>();
            for (NamedExpression n : named) {
                if (n instanceof Alias) {
                    Alias a = (Alias) n;
                    if (a.child().foldable()) {
                        values.add(a.child().fold());
                    }
                    // not everything is foldable, bail out early
                    else {
                        return values;
                    }
                } else if (n.foldable()) {
                    values.add(n.fold());
                }
                else {
                    // not everything is foldable, bail-out early
                    return values;
                }
            }
            return values;
        }

        /**
         * Check if the plan doesn't model a query with FROM clause on a table
         * that its filter (WHERE clause) is folded to FALSE.
         */
        private static boolean isNotQueryWithFromClauseAndFilterFoldedToFalse(UnaryPlan plan) {
            return (!(plan.child() instanceof LocalRelation) || (plan.child() instanceof LocalRelation &&
                !(((LocalRelation) plan.child()).executable() instanceof EmptyExecutable)));
        }
    }


    static class SetAsOptimized extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            plan.forEachUp(this::rule);
            return plan;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (!plan.optimized()) {
                plan.setOptimized();
            }
            return plan;
        }
    }


    abstract static class OptimizerRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        private final TransformDirection direction;

        OptimizerRule() {
            this(TransformDirection.DOWN);
        }

        protected OptimizerRule(TransformDirection direction) {
            this.direction = direction;
        }


        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return direction == TransformDirection.DOWN ?
                plan.transformDown(this::rule, typeToken()) : plan.transformUp(this::rule, typeToken());
        }

        @Override
        protected abstract LogicalPlan rule(SubPlan plan);
    }

    abstract static class OptimizerExpressionRule extends Rule<LogicalPlan, LogicalPlan> {

        private final TransformDirection direction;

        OptimizerExpressionRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return direction == TransformDirection.DOWN ? plan.transformExpressionsDown(this::rule) : plan
                    .transformExpressionsUp(this::rule);
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan;
        }

        protected abstract Expression rule(Expression e);
    }

    abstract static class OptimizerBasicRule extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public abstract LogicalPlan apply(LogicalPlan plan);

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan;
        }
    }

    enum TransformDirection {
        UP, DOWN
    }
}
