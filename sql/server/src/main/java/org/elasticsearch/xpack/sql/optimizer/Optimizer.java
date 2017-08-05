/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.AttributeSet;
import org.elasticsearch.xpack.sql.expression.BinaryExpression;
import org.elasticsearch.xpack.sql.expression.BinaryExpression.Negateable;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionSet;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.NestedFieldAttribute;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStatsEnclosed;
import org.elasticsearch.xpack.sql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixStatsEnclosed;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRank;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRanks;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentiles;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Stats;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.predicate.And;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryComparison;
import org.elasticsearch.xpack.sql.expression.predicate.Equals;
import org.elasticsearch.xpack.sql.expression.predicate.GreaterThan;
import org.elasticsearch.xpack.sql.expression.predicate.GreaterThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.LessThan;
import org.elasticsearch.xpack.sql.expression.predicate.LessThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.Not;
import org.elasticsearch.xpack.sql.expression.predicate.Or;
import org.elasticsearch.xpack.sql.expression.predicate.Range;
import org.elasticsearch.xpack.sql.plan.logical.Aggregate;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.plan.logical.Limit;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.OrderBy;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.plan.logical.Queryless;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.rule.Rule;
import org.elasticsearch.xpack.sql.rule.RuleExecutor;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.sql.expression.Literal.FALSE;
import static org.elasticsearch.xpack.sql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.sql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.sql.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.sql.expression.predicate.Predicates.inCommon;
import static org.elasticsearch.xpack.sql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.sql.expression.predicate.Predicates.splitOr;
import static org.elasticsearch.xpack.sql.expression.predicate.Predicates.subtract;
import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;


public class Optimizer extends RuleExecutor<LogicalPlan> {

    private final Catalog catalog;

    public Optimizer(Catalog catalog) {
        this.catalog = catalog;
    }

    public ExecutionInfo debugOptimize(LogicalPlan verified) {
        return verified.optimized() ? null : executeWithInfo(verified);
    }

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch resolution = new Batch("Finish Analysis", 
                new PruneSubqueryAliases(),
                new CleanAliases()
                );

        Batch aggregate = new Batch("Aggregation", 
                new PruneDuplicatesInGroupBy(),
                new ReplaceDuplicateAggsWithReferences(),
                new CombineAggsToMatrixStats(),
                new CombineAggsToExtendedStats(),
                new CombineAggsToStats(),
                new PromoteStatsToExtendedStats(), new CombineAggsToPercentiles(), new CombineAggsToPercentileRanks()
                );

        Batch cleanup = new Batch("Operator Optimization",
                // can't really do it since alias information is lost (packed inside EsQuery)
                //new ProjectPruning(),
                new BooleanSimplification(),
                new BinaryComparisonSimplification(),
                new BooleanLiteralsOnTheRight(),
                new CombineComparisonsIntoRange(),
                new PruneFilters(),
                new PruneOrderBy(),
                new PruneOrderByNestedFields(),
                new PruneCast(),
                new PruneDuplicateFunctions(),
                new SkipQueryOnLimitZero()
                );
                //new BalanceBooleanTrees());
        Batch label = new Batch("Set as Optimized", Limiter.ONCE,
                new SetAsOptimized());
        
        return Arrays.asList(resolution, aggregate, cleanup, label);
    }


    static class PruneSubqueryAliases extends OptimizerRule<SubQueryAlias> {

        PruneSubqueryAliases() {
            super(false);
        }

        @Override
        protected LogicalPlan rule(SubQueryAlias alias) {
            return alias.child();
        }
    }

    static class CleanAliases extends OptimizerRule<LogicalPlan> {

        CleanAliases() {
            super(false);
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan instanceof Project) {
                Project p = (Project) plan;
                return new Project(p.location(), p.child(), cleanExpressions(p.projections()));
            }

            if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                // clean group expressions
                List<Expression> cleanedGroups = a.groupings().stream().map(this::trimAliases).collect(toList());
                return new Aggregate(a.location(), a.child(), cleanedGroups, cleanExpressions(a.aggregates()));
            }

            return plan.transformExpressionsOnly(e -> {
                if (e instanceof Alias) {
                    return ((Alias) e).child();
                }
                return e;
            });
        }

        private List<NamedExpression> cleanExpressions(List<? extends NamedExpression> args) {
            return args.stream().map(this::trimNonTopLevelAliases).map(NamedExpression.class::cast)
                    .collect(toList());
        }

        private Expression trimNonTopLevelAliases(Expression e) {
            if (e instanceof Alias) {
                Alias a = (Alias) e;
                return new Alias(a.location(), a.name(), a.qualifier(), trimAliases(a.child()), a.id());
            }
            return trimAliases(e);
        }

        private Expression trimAliases(Expression e) {
            return e.transformDown(Alias::child, Alias.class);
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
                return new Aggregate(agg.location(), agg.child(), new ArrayList<>(unique), agg.aggregates());
            }
            return agg;
        }
    }

    static class ReplaceDuplicateAggsWithReferences extends OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate agg) {
            List<? extends NamedExpression> aggs = agg.aggregates();

            Map<Expression, NamedExpression> unique = new HashMap<>();
            Map<NamedExpression, Expression> reverse = new HashMap<>();

            // find duplicates by looking at the function and canonical form
            for (NamedExpression ne : aggs) {
                if (ne instanceof Alias) {
                    Alias a = (Alias) ne;
                    unique.putIfAbsent(a.child(), a);
                    reverse.putIfAbsent(ne, a.child());
                }
                else {
                    unique.putIfAbsent(ne.canonical(), ne);
                    reverse.putIfAbsent(ne, ne.canonical());
                }
            }

            if (unique.size() != aggs.size()) {
                List<NamedExpression> newAggs = new ArrayList<>(aggs.size());
                for (NamedExpression ne : aggs) {
                    newAggs.add(unique.get(reverse.get(ne)));
                }
                return new Aggregate(agg.location(), agg.child(), agg.groupings(), newAggs);
            }

            return agg;
        }
    }

    static class CombineAggsToMatrixStats extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            Map<Expression, MatrixStats> seen = new LinkedHashMap<>();
            Map<String, AggregateFunctionAttribute> promotedFunctionIds = new LinkedHashMap<>();

            p = p.transformExpressionsUp(e -> rule(e, seen, promotedFunctionIds));
            return p.transformExpressionsDown(e -> CombineAggsToStats.updateFunctionAttrs(e, promotedFunctionIds));
        }

        @Override
        protected LogicalPlan rule(LogicalPlan e) {
            return e;
        }

        protected Expression rule(Expression e, Map<Expression, MatrixStats> seen, Map<String, AggregateFunctionAttribute> promotedIds) {
            if (e instanceof MatrixStatsEnclosed) {
                AggregateFunction f = (AggregateFunction) e;

                Expression argument = f.field();
                MatrixStats matrixStats = seen.get(argument);

                if (matrixStats == null) {
                    matrixStats = new MatrixStats(f.location(), argument);
                    seen.put(argument, matrixStats);
                }

                InnerAggregate ia = new InnerAggregate(f, matrixStats, f.field());
                promotedIds.putIfAbsent(f.functionId(), ia.toAttribute());
                return ia;
            }

            return e;
        }
    }

    static class CombineAggsToExtendedStats extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            Map<String, AggregateFunctionAttribute> promotedFunctionIds = new LinkedHashMap<>();
            Map<Expression, ExtendedStats> seen = new LinkedHashMap<>();
            p = p.transformExpressionsUp(e -> rule(e, seen, promotedFunctionIds));
            // update old agg attributes 
            return p.transformExpressionsDown(e -> CombineAggsToStats.updateFunctionAttrs(e, promotedFunctionIds));
        }

        @Override
        protected LogicalPlan rule(LogicalPlan e) {
            return e;
        }

        protected Expression rule(Expression e, Map<Expression, ExtendedStats> seen, Map<String, AggregateFunctionAttribute> promotedIds) {
            if (e instanceof ExtendedStatsEnclosed) {
                AggregateFunction f = (AggregateFunction) e;

                Expression argument = f.field();
                ExtendedStats extendedStats = seen.get(argument);

                if (extendedStats == null) {
                    extendedStats = new ExtendedStats(f.location(), argument);
                    seen.put(argument, extendedStats);
                }

                InnerAggregate ia = new InnerAggregate(f, extendedStats);
                promotedIds.putIfAbsent(f.functionId(), ia.toAttribute());
                return ia;
            }

            return e;
        }
    }

    static class CombineAggsToStats extends Rule<LogicalPlan, LogicalPlan> {

        private static class Counter {
            final Stats stats;
            int count = 1;
            final Set<Class<? extends AggregateFunction>> functionTypes = new LinkedHashSet<>();

            Counter(Stats stats) {
                this.stats = stats;
            }
        }

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            Map<Expression, Counter> potentialPromotions = new LinkedHashMap<>();
            // old functionId to new function attribute
            Map<String, AggregateFunctionAttribute> promotedFunctionIds = new LinkedHashMap<>();

            p.forEachExpressionsUp(e -> count(e, potentialPromotions));
            // promote aggs to InnerAggs
            p = p.transformExpressionsUp(e -> promote(e, potentialPromotions, promotedFunctionIds));
            // update old agg attributes (TODO: this might be applied while updating the InnerAggs since the promotion happens bottom-up (and thus any attributes should be only in higher nodes)
            return p.transformExpressionsDown(e -> updateFunctionAttrs(e, promotedFunctionIds));
        }

        @Override
        protected LogicalPlan rule(LogicalPlan e) {
            return e;
        }

        private Expression count(Expression e, Map<Expression, Counter> seen) {
            if (Stats.isTypeCompatible(e)) {
                AggregateFunction f = (AggregateFunction) e;

                Expression argument = f.field();
                Counter counter = seen.get(argument);

                if (counter == null) {
                    counter = new Counter(new Stats(f.location(), argument));
                    counter.functionTypes.add(f.getClass());
                    seen.put(argument, counter);
                }
                else {
                    if (counter.functionTypes.add(f.getClass())) {
                        counter.count++;
                    }
                }
            }

            return e;
        }

        private Expression promote(Expression e, Map<Expression, Counter> seen, Map<String, AggregateFunctionAttribute> attrs) {
            if (Stats.isTypeCompatible(e)) {
                AggregateFunction f = (AggregateFunction) e;

                Expression argument = f.field();
                Counter counter = seen.get(argument);

                // if the stat has at least two different functions for it, promote it as stat
                if (counter != null && counter.count > 1) {
                    InnerAggregate innerAgg = new InnerAggregate(f, counter.stats);
                    attrs.putIfAbsent(f.functionId(), innerAgg.toAttribute());
                    return innerAgg;
                }
            }
            return e;
        }

        static Expression updateFunctionAttrs(Expression e, Map<String, AggregateFunctionAttribute> promotedIds) {
            if (e instanceof AggregateFunctionAttribute) {
                AggregateFunctionAttribute ae = (AggregateFunctionAttribute) e;
                AggregateFunctionAttribute promoted = promotedIds.get(ae.functionId());
                if (promoted != null) {
                    return ae.withFunctionId(promoted.functionId(), promoted.propertyPath());
                }
            }
            return e;
        }
    }
    
    static class PromoteStatsToExtendedStats extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            Map<Expression, ExtendedStats> seen = new LinkedHashMap<>();

            // count the extended stats
            p.forEachExpressionsUp(e -> count(e, seen));
            // then if there's a match, replace the stat inside the InnerAgg
            return p.transformExpressionsUp(e -> promote(e, seen));
        }

        @Override
        protected LogicalPlan rule(LogicalPlan e) {
            return e;
        }

        private void count(Expression e, Map<Expression, ExtendedStats> seen) {
            if (e instanceof InnerAggregate) {
                InnerAggregate ia = (InnerAggregate) e;
                if (ia.outer() instanceof ExtendedStats) {
                    ExtendedStats extStats = (ExtendedStats) ia.outer();
                    seen.putIfAbsent(extStats.field(), extStats);
                }
            }
        }

        protected Expression promote(Expression e, Map<Expression, ExtendedStats> seen) {
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
        }
    }

    static class CombineAggsToPercentiles extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            // percentile per field/expression
            Map<Expression, Set<Expression>> percentsPerField = new LinkedHashMap<>();

            // count gather the percents for each field
            p.forEachExpressionsUp(e -> count(e, percentsPerField));

            Map<Expression, Percentiles> percentilesPerField = new LinkedHashMap<>();
            // create a Percentile agg for each field (and its associated percents)
            percentsPerField.forEach((k, v) -> {
                percentilesPerField.put(k, new Percentiles(v.iterator().next().location(), k, new ArrayList<>(v)));
            });

            // now replace the agg with pointer to the main ones
            Map<String, AggregateFunctionAttribute> promotedFunctionIds = new LinkedHashMap<>();
            p = p.transformExpressionsUp(e -> rule(e, percentilesPerField, promotedFunctionIds));
            // finally update all the function references as well
            return p.transformExpressionsDown(e -> CombineAggsToStats.updateFunctionAttrs(e, promotedFunctionIds));
        }

        private void count(Expression e, Map<Expression, Set<Expression>> percentsPerField) {
            if (e instanceof Percentile) {
                Percentile p = (Percentile) e;
                Expression field = p.field();
                Set<Expression> percentiles = percentsPerField.get(field);

                if (percentiles == null) {
                    percentiles = new LinkedHashSet<>();
                    percentsPerField.put(field, percentiles);
                }

                percentiles.add(p.percent());
            }
        }

        protected Expression rule(Expression e, Map<Expression, Percentiles> percentilesPerField, Map<String, AggregateFunctionAttribute> promotedIds) {
            if (e instanceof Percentile) {
                Percentile p = (Percentile) e;
                Percentiles percentiles = percentilesPerField.get(p.field());

                InnerAggregate ia = new InnerAggregate(p, percentiles);
                promotedIds.putIfAbsent(p.functionId(), ia.toAttribute());
                return ia;
            }

            return e;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan e) {
            return e;
        }
    }

    static class CombineAggsToPercentileRanks extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            // percentile per field/expression
            Map<Expression, Set<Expression>> valuesPerField = new LinkedHashMap<>();

            // count gather the percents for each field
            p.forEachExpressionsUp(e -> count(e, valuesPerField));

            Map<Expression, PercentileRanks> ranksPerField = new LinkedHashMap<>();
            // create a PercentileRanks agg for each field (and its associated values)
            valuesPerField.forEach((k, v) -> {
                ranksPerField.put(k, new PercentileRanks(v.iterator().next().location(), k, new ArrayList<>(v)));
            });

            // now replace the agg with pointer to the main ones
            Map<String, AggregateFunctionAttribute> promotedFunctionIds = new LinkedHashMap<>();
            p = p.transformExpressionsUp(e -> rule(e, ranksPerField, promotedFunctionIds));
            // finally update all the function references as well
            return p.transformExpressionsDown(e -> CombineAggsToStats.updateFunctionAttrs(e, promotedFunctionIds));
        }

        private void count(Expression e, Map<Expression, Set<Expression>> ranksPerField) {
            if (e instanceof PercentileRank) {
                PercentileRank p = (PercentileRank) e;
                Expression field = p.field();
                Set<Expression> percentiles = ranksPerField.get(field);

                if (percentiles == null) {
                    percentiles = new LinkedHashSet<>();
                    ranksPerField.put(field, percentiles);
                }

                percentiles.add(p.value());
            }
        }

        protected Expression rule(Expression e, Map<Expression, PercentileRanks> ranksPerField, Map<String, AggregateFunctionAttribute> promotedIds) {
            if (e instanceof PercentileRank) {
                PercentileRank p = (PercentileRank) e;
                PercentileRanks ranks = ranksPerField.get(p.field());

                InnerAggregate ia = new InnerAggregate(p, ranks);
                promotedIds.putIfAbsent(p.functionId(), ia.toAttribute());
                return ia;
            }

            return e;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan e) {
            return e;
        }
    }

    static class PruneFilters extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            if (filter.condition() instanceof Literal) {
                if (TRUE.equals(filter.condition())) {
                    return filter.child();
                }
                // TODO: add comparison with null as well
                if (FALSE.equals(filter.condition())) {
                    throw new UnsupportedOperationException("Put empty relation");
                }
            }

            return filter;
        }
    }

    static class ReplaceAliasesInHaving extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            if (filter.child() instanceof Aggregate) {
                Expression cond = filter.condition();
                // resolve attributes to their actual
                Expression newCondition = cond.transformDown(a -> {

                    return a;
                }, AggregateFunctionAttribute.class);

                if (newCondition != cond) {
                    return new Filter(filter.location(), filter.child(), newCondition);
                }
            }
            return filter;
        }
    }

    static class ProjectPruning extends OptimizerRule<Project> {

        @Override
        protected LogicalPlan rule(Project project) {
            // eliminate Project added for resolving OrderBy
            if (project.child() instanceof OrderBy) {
                OrderBy ob = (OrderBy) project.child();
                if (ob.child() instanceof Project) {
                    Project grandChild = (Project) ob.child();
                    if (project.outputSet().substract(grandChild.outputSet()).isEmpty()) {
                        ob = new OrderBy(ob.location(), grandChild.child(), ob.order());
                        return new Project(project.location(), ob, project.output());
                    }
                }
            }
            return project;
        }
    }

    static class PruneOrderByNestedFields extends OptimizerRule<Project> {

        @Override
        protected LogicalPlan rule(Project project) {
            // check whether OrderBy relies on nested fields which are not used higher up
            if (project.child() instanceof OrderBy) {
                OrderBy ob = (OrderBy) project.child();

                // count the direct parents
                Map<String, Order> nestedOrders = new LinkedHashMap<>();

                for (Order order : ob.order()) {
                    Attribute attr = ((NamedExpression) order.child()).toAttribute();
                    if (attr instanceof NestedFieldAttribute) {
                        nestedOrders.put(((NestedFieldAttribute) attr).parentPath(), order);
                    }
                }

                // no nested fields in sort
                if (nestedOrders.isEmpty()) {
                    return project;
                }

                // count the nested parents (if any) inside the parents
                List<String> nestedTopFields = new ArrayList<>();

                for (Attribute attr : project.output()) {
                    if (attr instanceof NestedFieldAttribute) {
                        nestedTopFields.add(((NestedFieldAttribute) attr).parentPath());
                    }
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
                    return new Project(project.location(), ob.child(), project.projections());
                }

                if (orders.size() != ob.order().size()) {
                    OrderBy newOrder = new OrderBy(ob.location(), ob.child(), orders);
                    return new Project(project.location(), newOrder, project.projections());
                }
            }
            return project;
        }
    }

    static class PruneOrderBy extends OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy ob) {
            List<Order> order = ob.order();

            // remove constants
            List<Order> nonConstant = order.stream().filter(o -> !o.child().foldable()).collect(toList());

            if (nonConstant.isEmpty()) {
                return ob.child();
            }

            // if the sort points to an agg, consider it only if there's grouping
            if (ob.child() instanceof Aggregate) {
                Aggregate a = (Aggregate) ob.child();

                if (a.groupings().isEmpty()) {
                    AttributeSet aggsAttr = new AttributeSet(Expressions.asAttributes(a.aggregates()));

                    List<Order> nonAgg = nonConstant.stream().filter(o -> {
                        if (o.child() instanceof NamedExpression) {
                            return !aggsAttr.contains(((NamedExpression) o.child()).toAttribute());
                        }
                        return true;
                    }).collect(toList());

                    return nonAgg.isEmpty() ? ob.child() : new OrderBy(ob.location(), ob.child(), nonAgg);
                }
            }
            return ob;
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
            final Map<Attribute, Attribute> replacedCast = new LinkedHashMap<>();

            // first eliminate casts inside Aliases
            LogicalPlan transformed = plan.transformExpressionsUp(e -> {
                // cast wrapped in an alias
                if (e instanceof Alias) {
                    Alias as = (Alias) e;
                    if (as.child() instanceof Cast) {
                        Cast c = (Cast) as.child();

                        if (c.from().same(c.to())) {
                            Alias newAs = new Alias(as.location(), as.name(), as.qualifier(), c.argument(), as.id(), as.synthetic());
                            replacedCast.put(as.toAttribute(), newAs.toAttribute());
                            return newAs;
                        }
                    }
                    return e;
                }
                return e;
            });

            // then handle stand-alone casts (mixed together the cast rule will kick in before the alias)
            transformed = plan.transformExpressionsUp(e -> {
                if (e instanceof Cast) {
                    Cast c = (Cast) e;

                    if (c.from().same(c.to())) {
                        Expression argument = c.argument();
                        if (!(argument instanceof NamedExpression)) {
                            throw new SqlIllegalArgumentException("Expected a NamedExpression but got %s", argument);
                        }
                        replacedCast.put(c.toAttribute(), ((NamedExpression) argument).toAttribute());
                        return argument;
                    }
                }
                return e;
            });


            // replace attributes from previous removed Casts
            if (!replacedCast.isEmpty()) {
                return transformed.transformUp(p -> {
                    List<Attribute> newProjections = new ArrayList<>();

                    boolean changed = false;
                    for (NamedExpression ne : p.projections()) {
                        Attribute found = replacedCast.get(ne.toAttribute());
                        if (found != null) {
                            changed = true;
                            newProjections.add(found);
                        }
                        else {
                            newProjections.add(ne.toAttribute());
                        }
                    }

                    return changed ? new Project(p.location(), p.child(), newProjections) : p;

                }, Project.class);
            }
            return transformed;
        }
    }

    static class PruneDuplicateFunctions extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan p) {
            List<Function> seen = new ArrayList<>();
            return p.transformExpressionsUp(e -> rule(e, seen));
        }

        @Override
        protected LogicalPlan rule(LogicalPlan e) {
            return e;
        }

        protected Expression rule(Expression exp, List<Function> seen) {
            Expression e = exp;
            if (e instanceof Function) {
                Function f = (Function) e;
                for (Function seenFunction : seen) {
                    if (seenFunction != f && f.functionEquals(seenFunction)) {
                        return seenFunction;
                    }
                }
            }

            return exp;
        }
    }

    static class SkipQueryOnLimitZero extends OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.limit() instanceof Literal) {
                if (Integer.valueOf(0).equals(Integer.parseInt(((Literal) limit.limit()).value().toString()))) {
                    return new Queryless(limit.location(), new EmptyExecutable(limit.output()));
                }
            }
            return limit;
        }
    }

    static class CombineFilters extends OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            if (filter.child() instanceof Filter) {
                Filter child = (Filter) filter.child();
                throw new UnsupportedOperationException("not implemented yet");
            }
            throw new UnsupportedOperationException("not implemented yet");
        }
    }

    static class BooleanSimplification extends OptimizerExpressionUpRule {

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof BinaryExpression) {
                return simplifyAndOr((BinaryExpression) e);
            }
            if (e instanceof Not) {
                return simplifyNot((Not) e);
            }

            return e;
        }

        private Expression simplifyAndOr(BinaryExpression bc) {
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
                    return FALSE;
                }
                if (l.canonicalEquals(r)) {
                    return l;
                }

                //
                // common factor extraction -> (a || b) && (a || c) => a && (b || c)
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
                return combineOr(combine(common, new And(combineLeft.location(), combineLeft, combineRight)));
            }

            if (bc instanceof Or) {
                if (TRUE.equals(l) || TRUE.equals(r)) {
                    return TRUE;
                }

                if (TRUE.equals(l)) {
                    return r;
                }
                if (TRUE.equals(r)) {
                    return l;
                }

                if (l.canonicalEquals(r)) {
                    return l;
                }

                //
                // common factor extraction -> (a && b) || (a && c) => a || (b & c)
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
                return combineAnd(combine(common, new Or(combineLeft.location(), combineLeft, combineRight)));
            }

            // TODO: eliminate conjunction/disjunction 
            return bc;
        }

        private Expression simplifyNot(Not n) {
            Expression c = n.child();

            if (TRUE.equals(c)) {
                return FALSE;
            }
            if (FALSE.equals(c)) {
                return TRUE;
            }

            if (c instanceof Negateable) {
                return ((Negateable) c).negate();
            }

            if (c instanceof Not) {
                return ((Not) c).child();
            }

            return n;
        }
    }

    static class BinaryComparisonSimplification extends OptimizerExpressionUpRule {

        @Override
        protected Expression rule(Expression e) {
            return e instanceof BinaryComparison ? simplify((BinaryComparison) e) : e;
        }

        private Expression simplify(BinaryComparison bc) {
            Expression l = bc.left();
            Expression r = bc.right();

            // true for equality
            if (bc instanceof Equals || bc instanceof GreaterThanOrEqual || bc instanceof LessThanOrEqual) {
                if (!l.nullable() && !r.nullable() && l.canonicalEquals(r)) {
                    return TRUE;
                }
            }

            // false for equality
            if (bc instanceof GreaterThan || bc instanceof LessThan) {
                if (!l.nullable() && !r.nullable() && l.canonicalEquals(r)) {
                    return FALSE;
                }
            }

            return bc;
        }
    }

    static class BooleanLiteralsOnTheRight extends OptimizerExpressionUpRule {

        @Override
        protected Expression rule(Expression e) {
            return e instanceof BinaryExpression ? literalToTheRight((BinaryExpression) e) : e;
        }

        private Expression literalToTheRight(BinaryExpression be) {
            return be.left() instanceof Literal && !(be.right() instanceof Literal) ? be.swapLeftAndRight() : be;
        }
    }

    static class CombineComparisonsIntoRange extends OptimizerExpressionUpRule {

        @Override
        protected Expression rule(Expression e) {
            return e instanceof And ? combine((And) e) : e;
        }

        private Expression combine(And and) {
            Expression l = and.left();
            Expression r = and.right();

            if (l instanceof BinaryComparison && r instanceof BinaryComparison) {
                // if the same operator is used
                BinaryComparison lb = (BinaryComparison) l;
                BinaryComparison rb = (BinaryComparison) r;


                if (lb.left().equals(((BinaryComparison) r).left()) && lb.right() instanceof Literal && rb.right() instanceof Literal) {
                    // >/>= AND </<=
                    if ((l instanceof GreaterThan || l instanceof GreaterThanOrEqual)
                            && (r instanceof LessThan || r instanceof LessThanOrEqual)) {
                        return new Range(and.location(), lb.left(), lb.right(), l instanceof GreaterThanOrEqual, rb.right(),
                                r instanceof LessThanOrEqual);
                    }
                    // </<= AND >/>= 
                    else if ((r instanceof GreaterThan || r instanceof GreaterThanOrEqual)
                            && (l instanceof LessThan || l instanceof LessThanOrEqual)) {
                        return new Range(and.location(), rb.left(), rb.right(), r instanceof GreaterThanOrEqual, lb.right(),
                                l instanceof LessThanOrEqual);
                    }
                }
            }

            return and;
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

        private final boolean transformDown;

        OptimizerRule() {
            this(true);
        }

        OptimizerRule(boolean transformDown) {
            this.transformDown = transformDown;
        }


        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return transformDown ? plan.transformDown(this::rule, typeToken()) : plan.transformUp(this::rule, typeToken());
        }

        @Override
        protected abstract LogicalPlan rule(SubPlan plan);
    }

    abstract static class OptimizerExpressionUpRule extends Rule<LogicalPlan, LogicalPlan> {

        private final boolean transformDown;

        OptimizerExpressionUpRule() {
            //NB: expressions are transformed up (not down like the plan)
            this(false);
        }

        OptimizerExpressionUpRule(boolean transformDown) {
            this.transformDown = transformDown;
        }


        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return transformDown ? plan.transformExpressionsDown(this::rule) : plan.transformExpressionsUp(this::rule);
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan;
        }

        protected abstract Expression rule(Expression e);
    }
}