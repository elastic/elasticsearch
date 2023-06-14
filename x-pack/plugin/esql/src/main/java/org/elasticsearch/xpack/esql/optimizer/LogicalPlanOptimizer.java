/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.ExpressionSet;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.expression.predicate.regex.StringPattern;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BinaryComparisonSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanFunctionEqualsElimination;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.LiteralsOnTheRight;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PruneLiteralsInOrderBy;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SetAsOptimized;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.SimplifyComparisonsArithmetics;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;
import static org.elasticsearch.xpack.ql.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.ql.expression.Literal.FALSE;

public class LogicalPlanOptimizer extends RuleExecutor<LogicalPlan> {

    public LogicalPlan optimize(LogicalPlan verified) {
        return verified.optimized() ? verified : execute(verified);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        return rules();
    }

    protected static List<Batch<LogicalPlan>> rules() {
        var operators = new Batch<>(
            "Operator Optimization",
            new CombineProjections(),
            new PruneEmptyPlans(),
            new PropagateEmptyRelation(),
            new ConvertStringToByteRef(),
            new FoldNull(),
            new FoldNullInIn(),
            new ConstantFolding(),
            // boolean
            new BooleanSimplification(),
            new LiteralsOnTheRight(),
            new BinaryComparisonSimplification(),
            new BooleanFunctionEqualsElimination(),
            new ReplaceRegexMatch(),
            new CombineDisjunctionsToIn(),
            new SimplifyComparisonsArithmetics(EsqlDataTypes::areCompatible),
            // prune/elimination
            new PruneFilters(),
            new PruneLiteralsInOrderBy(),
            new PushDownAndCombineLimits(),
            new PushDownAndCombineFilters(),
            new PushDownEval(),
            new PushDownRegexExtract(),
            new PushDownEnrich(),
            new PushDownAndCombineOrderBy(),
            new PruneOrderByBeforeStats(),
            new PruneRedundantSortClauses()
        );

        var skip = new Batch<>("Skip Compute", new SkipQueryOnLimitZero());
        var cleanup = new Batch<>("Clean Up", new ReplaceLimitAndSortAsTopN());
        var label = new Batch<>("Set as Optimized", Limiter.ONCE, new SetAsOptimized());

        return asList(operators, skip, cleanup, label);
    }

    static class ConvertStringToByteRef extends OptimizerRules.OptimizerExpressionRule<Literal> {

        ConvertStringToByteRef() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected Expression rule(Literal lit) {
            if (lit.value() == null) {
                return lit;
            }
            if (lit.value() instanceof String s) {
                return Literal.of(lit, new BytesRef(s));
            }
            if (lit.value() instanceof List<?> l) {
                if (l.isEmpty() || false == l.get(0) instanceof String) {
                    return lit;
                }
                return Literal.of(lit, l.stream().map(v -> new BytesRef((String) v)).toList());
            }
            return lit;
        }
    }

    static class CombineProjections extends OptimizerRules.OptimizerRule<UnaryPlan> {

        CombineProjections() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            LogicalPlan child = plan.child();

            if (plan instanceof Project project) {
                if (child instanceof Project p) {
                    // eliminate lower project but first replace the aliases in the upper one
                    return p.withProjections(combineProjections(project.projections(), p.projections()));
                } else if (child instanceof Aggregate a) {
                    return new Aggregate(a.source(), a.child(), a.groupings(), combineProjections(project.projections(), a.aggregates()));
                }
            }

            // Agg with underlying Project (group by on sub-queries)
            if (plan instanceof Aggregate a) {
                if (child instanceof Project p) {
                    return new Aggregate(a.source(), p.child(), a.groupings(), combineProjections(a.aggregates(), p.projections()));
                }
            }

            return plan;
        }

        // normally only the upper projections should survive but since the lower list might have aliases definitions
        // that might be reused by the upper one, these need to be replaced.
        // for example an alias defined in the lower list might be referred in the upper - without replacing it the alias becomes invalid
        private List<NamedExpression> combineProjections(List<? extends NamedExpression> upper, List<? extends NamedExpression> lower) {

            // collect aliases in the lower list
            AttributeMap.Builder<NamedExpression> aliasesBuilder = AttributeMap.builder();
            for (NamedExpression ne : lower) {
                if ((ne instanceof Attribute) == false) {
                    aliasesBuilder.put(ne.toAttribute(), ne);
                }
            }

            AttributeMap<NamedExpression> aliases = aliasesBuilder.build();
            List<NamedExpression> replaced = new ArrayList<>();

            // replace any matching attribute with a lower alias (if there's a match)
            // but clean-up non-top aliases at the end
            for (NamedExpression ne : upper) {
                NamedExpression replacedExp = (NamedExpression) ne.transformUp(Attribute.class, a -> aliases.resolve(a, a));
                replaced.add((NamedExpression) trimNonTopLevelAliases(replacedExp));
            }
            return replaced;
        }

        public static Expression trimNonTopLevelAliases(Expression e) {
            if (e instanceof Alias a) {
                return new Alias(a.source(), a.name(), a.qualifier(), trimAliases(a.child()), a.id());
            }
            return trimAliases(e);
        }

        private static Expression trimAliases(Expression e) {
            return e.transformDown(Alias.class, Alias::child);
        }
    }

    static class FoldNull extends OptimizerRules.OptimizerExpressionRule<Expression> {

        FoldNull() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected Expression rule(Expression e) {
            if (e instanceof Alias == false
                && e.nullable() == Nullability.TRUE
                && Expressions.anyMatch(e.children(), Expressions::isNull)) {
                return Literal.of(e, null);
            }
            return e;
        }
    }

    static class FoldNullInIn extends OptimizerRules.OptimizerExpressionRule<In> {

        FoldNullInIn() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        protected Expression rule(In in) {
            List<Expression> newList = new ArrayList<>(in.list());
            // In folds itself if value() is `null`
            newList.removeIf(Expressions::isNull);
            if (in.list().size() != newList.size()) {
                if (newList.size() == 0) {
                    return FALSE;
                }
                newList.add(in.value());
                return in.replaceChildren(newList);
            }
            return in;
        }
    }

    static class PushDownAndCombineLimits extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.child() instanceof Limit childLimit) {
                var limitSource = limit.limit();
                var l1 = (int) limitSource.fold();
                var l2 = (int) childLimit.limit().fold();
                return new Limit(limit.source(), Literal.of(limitSource, Math.min(l1, l2)), childLimit.child());
            } else if (limit.child() instanceof UnaryPlan unary) {
                if (unary instanceof Eval || unary instanceof Project || unary instanceof RegexExtract || unary instanceof Enrich) {
                    return unary.replaceChild(limit.replaceChild(unary.child()));
                }
                // check if there's a 'visible' descendant limit lower than the current one
                // and if so, align the current limit since it adds no value
                // this applies for cases such as | limit 1 | sort field | limit 10
                else {
                    Limit descendantLimit = descendantLimit(unary);
                    if (descendantLimit != null) {
                        var l1 = (int) limit.limit().fold();
                        var l2 = (int) descendantLimit.limit().fold();
                        if (l2 <= l1) {
                            return new Limit(limit.source(), Literal.of(limit.limit(), l2), limit.child());
                        }
                    }
                }
            }
            return limit;
        }

        /**
         * Checks the existence of another 'visible' Limit, that exists behind an operation that doesn't produce output more data than
         * its input (that is not a relation/source nor aggregation).
         * P.S. Typically an aggregation produces less data than the input.
         */
        private static Limit descendantLimit(UnaryPlan unary) {
            UnaryPlan plan = unary;
            while (plan instanceof Aggregate == false) {
                if (plan instanceof Limit limit) {
                    return limit;
                }
                if (plan.child() instanceof UnaryPlan unaryPlan) {
                    plan = unaryPlan;
                } else {
                    break;
                }
            }
            return null;
        }
    }

    private static class BooleanSimplification extends org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanSimplification {

        BooleanSimplification() {
            super();
        }

        @Override
        protected Expression maybeSimplifyNegatable(Expression e) {
            return null;
        }

    }

    static class PruneFilters extends OptimizerRules.PruneFilters {

        @Override
        protected LogicalPlan skipPlan(Filter filter) {
            return LogicalPlanOptimizer.skipPlan(filter);
        }
    }

    static class SkipQueryOnLimitZero extends OptimizerRules.SkipQueryOnLimitZero {

        @Override
        protected LogicalPlan skipPlan(Limit limit) {
            return LogicalPlanOptimizer.skipPlan(limit);
        }
    }

    static class PruneEmptyPlans extends OptimizerRules.OptimizerRule<UnaryPlan> {

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            return plan.output().isEmpty() ? skipPlan(plan) : plan;
        }
    }

    static class PropagateEmptyRelation extends OptimizerRules.OptimizerRule<UnaryPlan> {

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            LogicalPlan p = plan;
            if (plan.child() instanceof LocalRelation local && local.supplier() == LocalSupplier.EMPTY) {
                // only care about non-grouped aggs might return something (count)
                if (plan instanceof Aggregate agg && agg.groupings().isEmpty()) {
                    p = skipPlan(plan, aggsFromEmpty(agg.aggregates()));
                } else {
                    p = skipPlan(plan);
                }
            }
            return p;
        }

        private static LocalSupplier aggsFromEmpty(List<? extends NamedExpression> aggs) {
            var result = new ArrayList<Object>(aggs.size());
            for (var agg : aggs) {
                // there needs to be an alias
                if (agg instanceof Alias a && a.child() instanceof AggregateFunction aggFunc) {
                    result.add(aggFunc instanceof Count ? 0L : null);
                } else {
                    throw new EsqlIllegalArgumentException("Did not expect a non-aliased aggregation {}", agg);
                }
            }
            var blocks = BlockUtils.fromListRow(result);
            return LocalSupplier.of(blocks);
        }
    }

    private static LogicalPlan skipPlan(UnaryPlan plan) {
        return new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY);
    }

    private static LogicalPlan skipPlan(UnaryPlan plan, LocalSupplier supplier) {
        return new LocalRelation(plan.source(), plan.output(), supplier);
    }

    protected static class PushDownAndCombineFilters extends OptimizerRules.OptimizerRule<Filter> {
        @Override
        protected LogicalPlan rule(Filter filter) {
            LogicalPlan plan = filter;
            LogicalPlan child = filter.child();
            Expression condition = filter.condition();

            if (child instanceof Filter f) {
                // combine nodes into a single Filter with updated ANDed condition
                plan = f.with(Predicates.combineAnd(List.of(f.condition(), condition)));
            } else if (child instanceof Aggregate agg) { // TODO: re-evaluate along with multi-value support
                // Only push [parts of] a filter past an agg if these/it operates on agg's grouping[s], not output.
                plan = maybePushDownPastUnary(
                    filter,
                    agg,
                    e -> e instanceof Attribute && agg.output().contains(e) && agg.groupings().contains(e) == false
                        || e instanceof AggregateFunction
                );
            } else if (child instanceof Eval eval) {
                // Don't push if Filter (still) contains references of Eval's fields.
                List<Attribute> attributes = new ArrayList<>(eval.fields().size());
                for (NamedExpression ne : eval.fields()) {
                    attributes.add(ne.toAttribute());
                }
                plan = maybePushDownPastUnary(filter, eval, e -> e instanceof Attribute && attributes.contains(e));
            } else if (child instanceof RegexExtract re) {
                // Push down filters that do not rely on attributes created by RegexExtract
                List<Attribute> attributes = new ArrayList<>(re.extractedFields().size());
                for (Attribute ne : re.extractedFields()) {
                    attributes.add(ne.toAttribute());
                }
                plan = maybePushDownPastUnary(filter, re, e -> e instanceof Attribute && attributes.contains(e));
            } else if (child instanceof Enrich enrich) {
                // Push down filters that do not rely on attributes created by Enrich
                List<Attribute> attributes = new ArrayList<>(enrich.enrichFields());
                plan = maybePushDownPastUnary(filter, enrich, e -> attributes.contains(e));
            } else if (child instanceof Project) {
                return pushDownPastProject(filter);
            } else if (child instanceof OrderBy orderBy) {
                // swap the filter with its child
                plan = orderBy.replaceChild(filter.with(orderBy.child(), condition));
            }
            // cannot push past a Limit, this could change the tailing result set returned
            return plan;
        }

        private static LogicalPlan maybePushDownPastUnary(Filter filter, UnaryPlan unary, Predicate<Expression> cannotPush) {
            LogicalPlan plan;
            List<Expression> pushable = new ArrayList<>();
            List<Expression> nonPushable = new ArrayList<>();
            for (Expression exp : Predicates.splitAnd(filter.condition())) {
                (exp.anyMatch(cannotPush) ? nonPushable : pushable).add(exp);
            }
            // Push the filter down even if it might not be pushable all the way to ES eventually: eval'ing it closer to the source,
            // potentially still in the Exec Engine, distributes the computation.
            if (pushable.size() > 0) {
                if (nonPushable.size() > 0) {
                    Filter pushed = new Filter(filter.source(), unary.child(), Predicates.combineAnd(pushable));
                    plan = filter.with(unary.replaceChild(pushed), Predicates.combineAnd(nonPushable));
                } else {
                    plan = unary.replaceChild(filter.with(unary.child(), filter.condition()));
                }
            } else {
                plan = filter;
            }
            return plan;
        }
    }

    /**
     * Pushes Evals past OrderBys. Although it seems arbitrary whether the OrderBy or the Eval is executed first,
     * this transformation ensures that OrderBys only separated by an eval can be combined by PushDownAndCombineOrderBy.
     *
     * E.g.:
     *
     * ... | sort a | eval x = b + 1 | sort x
     *
     * becomes
     *
     * ... | eval x = b + 1 | sort a | sort x
     *
     * Ordering the evals before the orderBys has the advantage that it's always possible to order the plans like this.
     * E.g., in the example above it would not be possible to put the eval after the two orderBys.
     */
    protected static class PushDownEval extends OptimizerRules.OptimizerRule<Eval> {
        @Override
        protected LogicalPlan rule(Eval eval) {
            LogicalPlan child = eval.child();

            // TODO: combine with CombineEval from https://github.com/elastic/elasticsearch-internal/pull/511 when merged
            if (child instanceof OrderBy orderBy) {
                return orderBy.replaceChild(eval.replaceChild(orderBy.child()));
            } else if (child instanceof Project) {
                var projectWithEvalChild = pushDownPastProject(eval);
                var fieldProjections = asAttributes(eval.fields());
                return projectWithEvalChild.withProjections(mergeOutputExpressions(fieldProjections, projectWithEvalChild.projections()));
            }

            return eval;
        }
    }

    // same as for PushDownEval
    protected static class PushDownRegexExtract extends OptimizerRules.OptimizerRule<RegexExtract> {
        @Override
        protected LogicalPlan rule(RegexExtract re) {
            LogicalPlan child = re.child();

            if (child instanceof OrderBy orderBy) {
                return orderBy.replaceChild(re.replaceChild(orderBy.child()));
            } else if (child instanceof Project) {
                var projectWithChild = pushDownPastProject(re);
                return projectWithChild.withProjections(mergeOutputExpressions(re.extractedFields(), projectWithChild.projections()));
            }

            return re;
        }
    }

    // TODO double-check: this should be the same as EVAL and GROK/DISSECT, needed to avoid unbounded sort
    protected static class PushDownEnrich extends OptimizerRules.OptimizerRule<Enrich> {
        @Override
        protected LogicalPlan rule(Enrich re) {
            LogicalPlan child = re.child();

            if (child instanceof OrderBy orderBy) {
                return orderBy.replaceChild(re.replaceChild(orderBy.child()));
            } else if (child instanceof Project) {
                var projectWithChild = pushDownPastProject(re);
                return projectWithChild.withProjections(mergeOutputExpressions(re.enrichFields(), projectWithChild.projections()));
            }

            return re;
        }
    }

    protected static class PushDownAndCombineOrderBy extends OptimizerRules.OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy orderBy) {
            LogicalPlan child = orderBy.child();

            if (child instanceof OrderBy childOrder) {
                // combine orders
                return new OrderBy(orderBy.source(), childOrder.child(), CollectionUtils.combine(orderBy.order(), childOrder.order()));
            } else if (child instanceof Project) {
                return pushDownPastProject(orderBy);
            }

            return orderBy;
        }
    }

    static class PruneOrderByBeforeStats extends OptimizerRules.OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate agg) {
            OrderBy order = findPullableOrderBy(agg.child());

            LogicalPlan p = agg;
            if (order != null) {
                p = agg.transformDown(OrderBy.class, o -> o == order ? order.child() : o);
            }
            return p;
        }

        private static OrderBy findPullableOrderBy(LogicalPlan plan) {
            OrderBy pullable = null;
            if (plan instanceof OrderBy o) {
                pullable = o;
            } else if (plan instanceof Eval
                || plan instanceof Filter
                || plan instanceof Project
                || plan instanceof RegexExtract
                || plan instanceof Enrich) {
                    pullable = findPullableOrderBy(((UnaryPlan) plan).child());
                }
            return pullable;
        }

    }

    static class PruneRedundantSortClauses extends OptimizerRules.OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy plan) {
            var referencedAttributes = new ExpressionSet<Attribute>();
            var order = new ArrayList<Order>();
            for (Order o : plan.order()) {
                Attribute a = (Attribute) o.child();
                if (referencedAttributes.add(a)) {
                    order.add(o);
                }
            }

            return plan.order().size() == order.size() ? plan : new OrderBy(plan.source(), plan.child(), order);
        }
    }

    private static Project pushDownPastProject(UnaryPlan parent) {
        if (parent.child() instanceof Project project) {
            AttributeMap.Builder<Expression> aliasBuilder = AttributeMap.builder();
            project.forEachExpression(Alias.class, a -> aliasBuilder.put(a.toAttribute(), a.child()));
            var aliases = aliasBuilder.build();

            var expressionsWithResolvedAliases = (UnaryPlan) parent.transformExpressionsOnly(
                ReferenceAttribute.class,
                r -> aliases.resolve(r, r)
            );

            return project.replaceChild(expressionsWithResolvedAliases.replaceChild(project.child()));
        } else {
            throw new UnsupportedOperationException("Expected child to be instance of Project");
        }
    }

    /**
     * LIKE/RLIKE expressions can be simplified in some specific cases:
     *
     *  field LIKE "foo"  ->   field == "foo"       // constant match, no wildcards
     *  field LIKE "*"    ->   NOT (field IS NULL)  // match all
     */
    public static class ReplaceRegexMatch extends OptimizerRules.OptimizerExpressionRule<RegexMatch<?>> {

        public ReplaceRegexMatch() {
            super(OptimizerRules.TransformDirection.DOWN);
        }

        @Override
        protected Expression rule(RegexMatch<?> regexMatch) {
            Expression e = regexMatch;
            StringPattern pattern = regexMatch.pattern();
            if (pattern.matchesAll()) {
                e = new Not(e.source(), new IsNull(e.source(), regexMatch.field()));
            } else {
                String match = pattern.exactMatch();
                if (match != null) {
                    Literal literal = new Literal(regexMatch.source(), match, DataTypes.KEYWORD);
                    e = regexToEquals(regexMatch, literal);
                }
            }
            return e;
        }

        protected Expression regexToEquals(RegexMatch<?> regexMatch, Literal literal) {
            return new Equals(regexMatch.source(), regexMatch.field(), literal);
        }
    }

    static class CombineDisjunctionsToIn extends org.elasticsearch.xpack.ql.optimizer.OptimizerRules.CombineDisjunctionsToIn {
        @Override
        protected In createIn(Expression key, List<Expression> values, ZoneId zoneId) {
            return new In(key.source(), key, values);
        }
    }

    static class ReplaceLimitAndSortAsTopN extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit plan) {
            LogicalPlan p = plan;
            if (plan.child() instanceof OrderBy o) {
                p = new TopN(plan.source(), o.child(), o.order(), plan.limit());
            }
            return p;
        }
    }
}
