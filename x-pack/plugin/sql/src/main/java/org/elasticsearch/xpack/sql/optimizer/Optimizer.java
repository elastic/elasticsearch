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
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanLiteralsOnTheRight;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.CombineBinaryComparisons;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.OptimizerExpressionRule;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.OptimizerRule;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PropagateEquals;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SetAsOptimized;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.TransformDirection;
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
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer.CleanAliases;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
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
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.aggregate.TopHits;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.ArbitraryConditionalFunction;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Case;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfConditional;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Iif;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.session.SingletonExecutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
                new ReplaceReferenceAttributeWithSource(),
                new ReplaceAggregatesWithLiterals(),
                new ReplaceCountInLocalRelation()
                );

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

                        if (isMatching.get()) {
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
                    return new Literal(e.source(), Boolean.TRUE, DataTypes.BOOLEAN);
                }

            } else if (e instanceof IsNull) {
                if (((IsNull) e).field().nullable() == Nullability.FALSE) {
                    return new Literal(e.source(), Boolean.FALSE, DataTypes.BOOLEAN);
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
                    return new Literal(bc.source(), Boolean.TRUE, DataTypes.BOOLEAN);
                }
            }
            if (bc instanceof NullEquals) {
                if (l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.TRUE, DataTypes.BOOLEAN);
                }
                if (Expressions.isNull(r)) {
                    return new IsNull(bc.source(), l);
                }
            }

            // false for equality
            if (bc instanceof NotEquals || bc instanceof GreaterThan || bc instanceof LessThan) {
                if (l.nullable() == Nullability.FALSE && r.nullable() == Nullability.FALSE && l.semanticEquals(r)) {
                    return new Literal(bc.source(), Boolean.FALSE, DataTypes.BOOLEAN);
                }
            }

            return bc;
        }
    }

    /**
     * Any numeric aggregates (avg, min, max, sum) acting on literals are converted to an iif(count(1)=0, null, literal*count(1)) for sum,
     * and to iif(count(1)=0,null,literal) for the other three.
     * Additionally count(DISTINCT literal) is converted to iif(count(1)=0, 0, 1).
     */
    private static class ReplaceAggregatesWithLiterals extends OptimizerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan p) {
            return p.transformExpressionsDown(e -> {
                if (e instanceof Min || e instanceof Max || e instanceof Avg || e instanceof Sum ||
                    (e instanceof Count && ((Count) e).distinct())) {

                    AggregateFunction a = (AggregateFunction) e;

                    if (a.field().foldable()) {
                        Expression countOne = new Count(a.source(), new Literal(Source.EMPTY, 1, a.dataType()), false);
                        Equals countEqZero = new Equals(a.source(), countOne, new Literal(Source.EMPTY, 0, a.dataType()));
                        Expression argument = a.field();
                        Literal foldedArgument = new Literal(argument.source(), argument.fold(), a.dataType());

                        Expression iifResult = Literal.NULL;
                        Expression iifElseResult = foldedArgument;
                        if (e instanceof Sum) {
                            iifElseResult = new Mul(a.source(), countOne, foldedArgument);
                        } else if (e instanceof Count) {
                            iifResult =  new Literal(Source.EMPTY, 0, e.dataType());
                            iifElseResult = new Literal(Source.EMPTY, 1, e.dataType());
                        }

                        return new Iif(a.source(), countEqZero, iifResult, iifElseResult);
                    }
                }
                return e;
            });
        }
    }

    /**
     * A COUNT in a local relation will always be 1.
     */
    private static class ReplaceCountInLocalRelation extends OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate a) {
            boolean hasLocalRelation = a.anyMatch(LocalRelation.class::isInstance);
            
            return hasLocalRelation ? a.transformExpressionsDown(c -> {
                return c instanceof Count ? new Literal(c.source(), 1, c.dataType()) : c;
            }) : a;
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
                    if (DataTypes.isString(min.field().dataType())) {
                        return mins.computeIfAbsent(min.field(), k -> new First(min.source(), k, null));
                    }
                }
                if (e instanceof Max) {
                    Max max = (Max) e;
                    if (DataTypes.isString(max.field().dataType())) {
                        return maxs.computeIfAbsent(max.field(), k -> new Last(max.source(), k, null));
                    }
                }
                return e;
            });
        }
    }

    static class PruneFilters extends org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneFilters {

        @Override
        protected LogicalPlan nonMatchingFilter(Filter filter) {
            return new LocalRelation(filter.source(), new EmptyExecutable(filter.output()));
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
                if (values.size() == a.aggregates().size() && a.groupings().isEmpty()
                    && isNotQueryWithFromClauseAndFilterFoldedToFalse(a)) {
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
                } else {
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

    abstract static class OptimizerBasicRule extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public abstract LogicalPlan apply(LogicalPlan plan);

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan;
        }
    }
}
