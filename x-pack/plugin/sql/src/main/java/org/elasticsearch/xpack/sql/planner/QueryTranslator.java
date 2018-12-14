/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.sql.expression.function.aggregate.CompoundNumericAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRanks;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentiles;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Stats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeHistogramFunction;
import org.elasticsearch.xpack.sql.expression.predicate.Range;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.logical.And;
import org.elasticsearch.xpack.sql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.sql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.sql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.sql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.AndAggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.AvgAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.CardinalityAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.ExtendedStatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByColumnKey;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByDateKey;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByKey;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByScriptKey;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MatrixStatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MaxAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MinAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.OrAggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.PercentileRanksAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.PercentilesAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.StatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.SumAgg;
import org.elasticsearch.xpack.sql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.sql.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MultiMatchQuery;
import org.elasticsearch.xpack.sql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.sql.querydsl.query.NotQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.sql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.sql.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.sql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.sql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.sql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.sql.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.util.Check;
import org.elasticsearch.xpack.sql.util.ReflectionUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.expression.Foldables.doubleValuesOf;
import static org.elasticsearch.xpack.sql.expression.Foldables.stringValueOf;
import static org.elasticsearch.xpack.sql.expression.Foldables.valueOf;

final class QueryTranslator {

    private QueryTranslator(){}

    private static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = Arrays.asList(
            new BinaryComparisons(),
            new InComparisons(),
            new Ranges(),
            new BinaryLogic(),
            new Nots(),
            new IsNullTranslator(),
            new IsNotNullTranslator(),
            new Likes(),
            new StringQueries(),
            new Matches(),
            new MultiMatches()
            );

    private static final List<AggTranslator<?>> AGG_TRANSLATORS = Arrays.asList(
            new Maxes(),
            new Mins(),
            new Avgs(),
            new Sums(),
            new StatsAggs(),
            new ExtendedStatsAggs(),
            new MatrixStatsAggs(),
            new PercentilesAggs(),
            new PercentileRanksAggs(),
            new DistinctCounts(),
            new DateTimes()
            );

    static class QueryTranslation {
        final Query query;
        // Agg filter / Function or Agg association
        final AggFilter aggFilter;

        QueryTranslation(Query query) {
            this(query, null);
        }

        QueryTranslation(AggFilter aggFilter) {
            this(null, aggFilter);
        }

        QueryTranslation(Query query, AggFilter aggFilter) {
            this.query = query;
            this.aggFilter = aggFilter;
        }
    }

    static QueryTranslation toQuery(Expression e, boolean onAggs) {
        QueryTranslation translation = null;
        for (ExpressionTranslator<?> translator : QUERY_TRANSLATORS) {
            translation = translator.translate(e, onAggs);
            if (translation != null) {
                return translation;
            }
        }

        throw new SqlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }

    static LeafAgg toAgg(String id, Function f) {

        for (AggTranslator<?> translator : AGG_TRANSLATORS) {
            LeafAgg agg = translator.apply(id, f);
            if (agg != null) {
                return agg;
            }
        }

        throw new SqlIllegalArgumentException("Don't know how to translate {} {}", f.nodeName(), f);
    }

    static class GroupingContext {
        final Map<ExpressionId, GroupByKey> groupMap;
        final GroupByKey tail;

        GroupingContext(Map<ExpressionId, GroupByKey> groupMap) {
            this.groupMap = groupMap;

            GroupByKey lastAgg = null;
            for (Entry<ExpressionId, GroupByKey> entry : groupMap.entrySet()) {
                lastAgg = entry.getValue();
            }

            tail = lastAgg;
        }

        GroupByKey groupFor(Expression exp) {
            if (Functions.isAggregate(exp)) {
                AggregateFunction f = (AggregateFunction) exp;
                // if there's at least one agg in the tree
                if (!groupMap.isEmpty()) {
                    GroupByKey matchingGroup = null;
                    // group found - finding the dedicated agg
                    if (f.field() instanceof NamedExpression) {
                        matchingGroup = groupMap.get(((NamedExpression) f.field()).id());
                    }
                    // return matching group or the tail (last group)
                    return matchingGroup != null ? matchingGroup : tail;
                }
                else {
                    return null;
                }
            }
            if (exp instanceof NamedExpression) {
                return groupMap.get(((NamedExpression) exp).id());
            }
            throw new SqlIllegalArgumentException("Don't know how to find group for expression {}", exp);
        }

        @Override
        public String toString() {
            return groupMap.toString();
        }
    }

    /**
     * Creates the list of GroupBy keys
     */
    static GroupingContext groupBy(List<? extends Expression> groupings) {
        if (groupings.isEmpty()) {
            return null;
        }

        Map<ExpressionId, GroupByKey> aggMap = new LinkedHashMap<>();

        for (Expression exp : groupings) {
            String aggId;
            if (exp instanceof NamedExpression) {
                NamedExpression ne = (NamedExpression) exp;

                // change analyzed to non non-analyzed attributes
                if (exp instanceof FieldAttribute) {
                    FieldAttribute fa = (FieldAttribute) exp;
                    if (fa.isInexact()) {
                        ne = fa.exactAttribute();
                    }
                }
                aggId = ne.id().toString();

                GroupByKey key;

                // handle functions differently
                if (exp instanceof Function) {
                    // dates are handled differently because of date histograms
                    if (exp instanceof DateTimeHistogramFunction) {
                        DateTimeHistogramFunction dthf = (DateTimeHistogramFunction) exp;
                        key = new GroupByDateKey(aggId, nameOf(exp), dthf.interval(), dthf.timeZone());
                    }
                    // all other scalar functions become a script
                    else if (exp instanceof ScalarFunction) {
                        ScalarFunction sf = (ScalarFunction) exp;
                        key = new GroupByScriptKey(aggId, nameOf(exp), sf.asScript());
                    }
                    // bumped into into an invalid function (which should be caught by the verifier)
                    else {
                        throw new SqlIllegalArgumentException("Cannot GROUP BY function {}", exp);
                    }
                }
                else {
                    key = new GroupByColumnKey(aggId, ne.name());
                }

                aggMap.put(ne.id(), key);
            }
            else {
                throw new SqlIllegalArgumentException("Don't know how to group on {}", exp.nodeString());
            }
        }
        return new GroupingContext(aggMap);
    }

    static QueryTranslation and(Location loc, QueryTranslation left, QueryTranslation right) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }

        Query newQ = null;
        if (left.query != null || right.query != null) {
            newQ = and(loc, left.query, right.query);
        }

        AggFilter aggFilter;

        if (left.aggFilter == null) {
            aggFilter = right.aggFilter;
        }
        else if (right.aggFilter == null) {
            aggFilter = left.aggFilter;
        }
        else {
            aggFilter = new AndAggFilter(left.aggFilter, right.aggFilter);
        }

        return new QueryTranslation(newQ, aggFilter);
    }

    static Query and(Location loc, Query left, Query right) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return new BoolQuery(loc, true, left, right);
    }

    static QueryTranslation or(Location loc, QueryTranslation left, QueryTranslation right) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }

        Query newQ = null;
        if (left.query != null || right.query != null) {
            newQ = or(loc, left.query, right.query);
        }

        AggFilter aggFilter = null;

        if (left.aggFilter == null) {
            aggFilter = right.aggFilter;
        }
        else if (right.aggFilter == null) {
            aggFilter = left.aggFilter;
        }
        else {
            aggFilter = new OrAggFilter(left.aggFilter, right.aggFilter);
        }

        return new QueryTranslation(newQ, aggFilter);
    }

    static Query or(Location loc, Query left, Query right) {
        Check.isTrue(left != null || right != null, "Both expressions are null");

        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return new BoolQuery(loc, false, left, right);
    }

    static String nameOf(Expression e) {
        if (e instanceof DateTimeFunction) {
            return nameOf(((DateTimeFunction) e).field());
        }
        if (e instanceof NamedExpression) {
            return ((NamedExpression) e).name();
        }
        if (e instanceof Literal) {
            return String.valueOf(e.fold());
        }
        throw new SqlIllegalArgumentException("Cannot determine name for {}", e);
    }

    static String idOf(Expression e) {
        if (e instanceof NamedExpression) {
            return ((NamedExpression) e).id().toString();
        }
        throw new SqlIllegalArgumentException("Cannot determine id for {}", e);
    }

    static String dateFormat(Expression e) {
        if (e instanceof DateTimeFunction) {
            return ((DateTimeFunction) e).dateTimeFormat();
        }
        return null;
    }

    static String field(AggregateFunction af) {
        Expression arg = af.field();
        if (arg instanceof FieldAttribute) {
            return ((FieldAttribute) arg).name();
        }
        if (arg instanceof Literal) {
            return String.valueOf(((Literal) arg).value());
        }
        throw new SqlIllegalArgumentException("Does not know how to convert argument {} for function {}", arg.nodeString(),
                af.nodeString());
    }

    // TODO: need to optimize on ngram
    // TODO: see whether escaping is needed
    static class Likes extends ExpressionTranslator<RegexMatch> {

        @Override
        protected QueryTranslation asQuery(RegexMatch e, boolean onAggs) {
            Query q = null;
            boolean inexact = true;
            String target = null;

            if (e.left() instanceof FieldAttribute) {
                FieldAttribute fa = (FieldAttribute) e.left();
                inexact = fa.isInexact();
                target = nameOf(inexact ? fa : fa.exactAttribute());
            } else {
                throw new SqlIllegalArgumentException("Scalar function ({}) not allowed (yet) as arguments for LIKE",
                        Expressions.name(e.left()));
            }

            if (e instanceof Like) {
                LikePattern p = ((Like) e).pattern();
                if (inexact) {
                    q = new QueryStringQuery(e.location(), p.asLuceneWildcard(), target);
                }
                else {
                    q = new WildcardQuery(e.location(), nameOf(e.left()), p.asLuceneWildcard());
                }
            }

            if (e instanceof RLike) {
                String pattern = stringValueOf(e.right());
                if (inexact) {
                    q = new QueryStringQuery(e.location(), "/" + pattern + "/", target);
                }
                else {
                    q = new RegexQuery(e.location(), nameOf(e.left()), pattern);
                }
            }

            return q != null ? new QueryTranslation(wrapIfNested(q, e.left())) : null;
        }
    }

    static class StringQueries extends ExpressionTranslator<StringQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(StringQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(new QueryStringQuery(q.location(), q.query(), q.fields(), q));
        }
    }

    static class Matches extends ExpressionTranslator<MatchQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(MatchQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(wrapIfNested(new MatchQuery(q.location(), nameOf(q.field()), q.query(), q), q.field()));
        }
    }

    static class MultiMatches extends ExpressionTranslator<MultiMatchQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(MultiMatchQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(new MultiMatchQuery(q.location(), q.query(), q.fields(), q));
        }
    }

    static class BinaryLogic extends ExpressionTranslator<org.elasticsearch.xpack.sql.expression.predicate.logical.BinaryLogic> {

        @Override
        protected QueryTranslation asQuery(org.elasticsearch.xpack.sql.expression.predicate.logical.BinaryLogic e, boolean onAggs) {
            if (e instanceof And) {
                return and(e.location(), toQuery(e.left(), onAggs), toQuery(e.right(), onAggs));
            }
            if (e instanceof Or) {
                return or(e.location(), toQuery(e.left(), onAggs), toQuery(e.right(), onAggs));
            }

            return null;
        }
    }

    static class Nots extends ExpressionTranslator<Not> {

        @Override
        protected QueryTranslation asQuery(Not not, boolean onAggs) {
            Query query = null;
            AggFilter aggFilter = null;

            if (onAggs) {
                aggFilter = new AggFilter(not.id().toString(), not.asScript());
            } else {
                query = handleQuery(not, not.field(),
                    () -> new NotQuery(not.location(), toQuery(not.field(), false).query));
            }

            return new QueryTranslation(query, aggFilter);
        }
    }

    static class IsNotNullTranslator extends ExpressionTranslator<IsNotNull> {

        @Override
        protected QueryTranslation asQuery(IsNotNull isNotNull, boolean onAggs) {
            Query query = null;
            AggFilter aggFilter = null;

            if (onAggs) {
                aggFilter = new AggFilter(isNotNull.id().toString(), isNotNull.asScript());
            } else {
                query = handleQuery(isNotNull, isNotNull.field(),
                    () -> new ExistsQuery(isNotNull.location(), nameOf(isNotNull.field())));
            }

            return new QueryTranslation(query, aggFilter);
        }
    }

    static class IsNullTranslator extends ExpressionTranslator<IsNull> {

        @Override
        protected QueryTranslation asQuery(IsNull isNull, boolean onAggs) {
            Query query = null;
            AggFilter aggFilter = null;

            if (onAggs) {
                aggFilter = new AggFilter(isNull.id().toString(), isNull.asScript());
            } else {
                query = handleQuery(isNull, isNull.field(),
                    () -> new NotQuery(isNull.location(), new ExistsQuery(isNull.location(), nameOf(isNull.field()))));
            }

            return new QueryTranslation(query, aggFilter);
        }
    }

    // assume the Optimizer properly orders the predicates to ease the translation
    static class BinaryComparisons extends ExpressionTranslator<BinaryComparison> {

        @Override
        protected QueryTranslation asQuery(BinaryComparison bc, boolean onAggs) {
            Check.isTrue(bc.right().foldable(),
                    "Line {}:{}: Comparisons against variables are not (currently) supported; offender [{}] in [{}]",
                    bc.right().location().getLineNumber(), bc.right().location().getColumnNumber(),
                    Expressions.name(bc.right()), bc.symbol());

            if (bc.left() instanceof NamedExpression) {
                NamedExpression ne = (NamedExpression) bc.left();

                Query query = null;
                AggFilter aggFilter = null;

                Attribute at = ne.toAttribute();
                //
                // Agg context means HAVING -> PipelineAggs
                //
                if (onAggs) {
                    aggFilter = new AggFilter(at.id().toString(), bc.asScript());
                }
                else {
                    query = handleQuery(bc, ne, () -> translateQuery(bc));
                }
                return new QueryTranslation(query, aggFilter);
            }
            //
            // if the code gets here it's a bug
            //
            else {
                throw new SqlIllegalArgumentException("No idea how to translate " + bc.left());
            }
        }

        private static Query translateQuery(BinaryComparison bc) {
            Location loc = bc.location();
            String name = nameOf(bc.left());
            Object value = valueOf(bc.right());
            String format = dateFormat(bc.left());

            if (bc instanceof GreaterThan) {
                return new RangeQuery(loc, name, value, false, null, false, format);
            }
            if (bc instanceof GreaterThanOrEqual) {
                return new RangeQuery(loc, name, value, true, null, false, format);
            }
            if (bc instanceof LessThan) {
                return new RangeQuery(loc, name, null, false, value, false, format);
            }
            if (bc instanceof LessThanOrEqual) {
                return new RangeQuery(loc, name, null, false, value, true, format);
            }
            if (bc instanceof Equals || bc instanceof NullEquals || bc instanceof NotEquals) {
                if (bc.left() instanceof FieldAttribute) {
                    FieldAttribute fa = (FieldAttribute) bc.left();
                    // equality should always be against an exact match
                    // (which is important for strings)
                    if (fa.isInexact()) {
                        name = fa.exactAttribute().name();
                    }
                }
                Query query = new TermQuery(loc, name, value);
                if (bc instanceof NotEquals) {
                    query = new NotQuery(loc, query);
            }
                return query;
            }

            throw new SqlIllegalArgumentException("Don't know how to translate binary comparison [{}] in [{}]", bc.right().nodeString(),
                    bc);
        }
    }

    // assume the Optimizer properly orders the predicates to ease the translation
    static class InComparisons extends ExpressionTranslator<In> {

        @Override
        protected QueryTranslation asQuery(In in, boolean onAggs) {
            Optional<Expression> firstNotFoldable = in.list().stream().filter(expression -> !expression.foldable()).findFirst();

            if (firstNotFoldable.isPresent()) {
                throw new SqlIllegalArgumentException(
                    "Line {}:{}: Comparisons against variables are not (currently) supported; offender [{}] in [{}]",
                    firstNotFoldable.get().location().getLineNumber(),
                    firstNotFoldable.get().location().getColumnNumber(),
                    Expressions.name(firstNotFoldable.get()),
                    in.name());
            }

            if (in.value() instanceof NamedExpression) {
                NamedExpression ne = (NamedExpression) in.value();

                Query query = null;
                AggFilter aggFilter = null;

                Attribute at = ne.toAttribute();
                //
                // Agg context means HAVING -> PipelineAggs
                //
                if (onAggs) {
                    aggFilter = new AggFilter(at.id().toString(), in.asScript());
                }
                else {
                    query = handleQuery(in, ne, () -> new TermsQuery(in.location(), ne.name(), in.list()));
                }
                return new QueryTranslation(query, aggFilter);
            }
            //
            // if the code gets here it's a bug
            //
            else {
                throw new SqlIllegalArgumentException("No idea how to translate " + in.value());
            }
        }
    }

    static class Ranges extends ExpressionTranslator<Range> {

        @Override
        protected QueryTranslation asQuery(Range r, boolean onAggs) {
            Expression e = r.value();
            
            if (e instanceof NamedExpression) {
                Query query = null;
                AggFilter aggFilter = null;

                //
                // Agg context means HAVING -> PipelineAggs
                //
                Attribute at = ((NamedExpression) e).toAttribute();

                if (onAggs) {
                    aggFilter = new AggFilter(at.id().toString(), r.asScript());
                } else {
                    query = handleQuery(r, r.value(),
                        () -> new RangeQuery(r.location(), nameOf(r.value()), valueOf(r.lower()), r.includeLower(),
                            valueOf(r.upper()), r.includeUpper(), dateFormat(r.value())));
                }
                return new QueryTranslation(query, aggFilter);
            } else {
                throw new SqlIllegalArgumentException("No idea how to translate " + e);
            }
        }
    }


    //
    // Agg translators
    //

    static class DistinctCounts extends SingleValueAggTranslator<Count> {

        @Override
        protected LeafAgg toAgg(String id, Count c) {
            if (!c.distinct()) {
                return null;
            }
            return new CardinalityAgg(id, field(c));
        }
    }

    static class Sums extends SingleValueAggTranslator<Sum> {

        @Override
        protected LeafAgg toAgg(String id, Sum s) {
            return new SumAgg(id, field(s));
        }
    }

    static class Avgs extends SingleValueAggTranslator<Avg> {

        @Override
        protected LeafAgg toAgg(String id, Avg a) {
            return new AvgAgg(id, field(a));
        }
    }

    static class Maxes extends SingleValueAggTranslator<Max> {

        @Override
        protected LeafAgg toAgg(String id, Max m) {
            return new MaxAgg(id, field(m));
        }
    }

    static class Mins extends SingleValueAggTranslator<Min> {

        @Override
        protected LeafAgg toAgg(String id, Min m) {
            return new MinAgg(id, field(m));
        }
    }

    static class StatsAggs extends CompoundAggTranslator<Stats> {

        @Override
        protected LeafAgg toAgg(String id, Stats s) {
            return new StatsAgg(id, field(s));
        }
    }

    static class ExtendedStatsAggs extends CompoundAggTranslator<ExtendedStats> {

        @Override
        protected LeafAgg toAgg(String id, ExtendedStats e) {
            return new ExtendedStatsAgg(id, field(e));
        }
    }

    static class MatrixStatsAggs extends CompoundAggTranslator<MatrixStats> {

        @Override
        protected LeafAgg toAgg(String id, MatrixStats m) {
            return new MatrixStatsAgg(id, singletonList(field(m)));
        }
    }

    static class PercentilesAggs extends CompoundAggTranslator<Percentiles> {

        @Override
        protected LeafAgg toAgg(String id, Percentiles p) {
            return new PercentilesAgg(id, field(p), doubleValuesOf(p.percents()));
        }
    }

    static class PercentileRanksAggs extends CompoundAggTranslator<PercentileRanks> {

        @Override
        protected LeafAgg toAgg(String id, PercentileRanks p) {
            return new PercentileRanksAgg(id, field(p), doubleValuesOf(p.values()));
        }
    }

    static class DateTimes extends SingleValueAggTranslator<Min> {

        @Override
        protected LeafAgg toAgg(String id, Min m) {
            return new MinAgg(id, field(m));
        }
    }

    abstract static class AggTranslator<F extends Function> {

        private final Class<?> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        @SuppressWarnings("unchecked")
        public final LeafAgg apply(String id, Function f) {
            return (typeToken.isInstance(f) ? asAgg(id, (F) f) : null);
        }

        protected abstract LeafAgg asAgg(String id, F f);
    }

    abstract static class SingleValueAggTranslator<F extends Function> extends AggTranslator<F> {

        @Override
        protected final LeafAgg asAgg(String id, F function) {
            return toAgg(id, function);
        }

        protected abstract LeafAgg toAgg(String id, F f);
    }

    abstract static class CompoundAggTranslator<C extends CompoundNumericAggregate> extends AggTranslator<C> {

        @Override
        protected final LeafAgg asAgg(String id, C function) {
            return toAgg(id, function);
        }

        protected abstract LeafAgg toAgg(String id, C f);
    }


    abstract static class ExpressionTranslator<E extends Expression> {

        private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        @SuppressWarnings("unchecked")
        public QueryTranslation translate(Expression exp, boolean onAggs) {
            return (typeToken.isInstance(exp) ? asQuery((E) exp, onAggs) : null);
        }

        protected abstract QueryTranslation asQuery(E e, boolean onAggs);


        protected static Query handleQuery(ScalarFunction sf, Expression field, Supplier<Query> query) {
            if (field instanceof FieldAttribute) {
                return wrapIfNested(query.get(), field);
            }
            return new ScriptQuery(sf.location(), sf.asScript());
        }

        protected static Query wrapIfNested(Query query, Expression exp) {
            if (exp instanceof FieldAttribute) {
                FieldAttribute fa = (FieldAttribute) exp;
                if (fa.isNested()) {
                    return new NestedQuery(fa.location(), fa.nestedParent().name(), query);
                }
            }
            return query;
        }
    }
}
