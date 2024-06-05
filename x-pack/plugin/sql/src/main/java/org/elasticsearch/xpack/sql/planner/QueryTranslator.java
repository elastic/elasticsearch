/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.GeoDistanceQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.sql.expression.function.aggregate.CompoundNumericAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.First;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRanks;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentiles;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Stats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.aggregate.TopHits;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StDistance;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.AggSource;
import org.elasticsearch.xpack.sql.querydsl.agg.AndAggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.AvgAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.CardinalityAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.ExtendedStatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.FilterExistsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MatrixStatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MaxAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MedianAbsoluteDeviationAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MinAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.OrAggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.PercentileRanksAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.PercentilesAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.StatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.SumAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.TopHitsAgg;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;
import org.elasticsearch.xpack.sql.util.Check;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.Expressions.id;
import static org.elasticsearch.xpack.ql.expression.Foldables.valueOf;

final class QueryTranslator {

    public static final String DATE_FORMAT = "strict_date_optional_time_nanos";
    public static final String TIME_FORMAT = "strict_hour_minute_second_fraction";

    private QueryTranslator() {}

    private static final List<SqlExpressionTranslator<?>> QUERY_TRANSLATORS = Arrays.asList(
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
        new MultiMatches(),
        new Scalars()
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
        new CountAggs(),
        new DateTimes(),
        new Firsts(),
        new Lasts(),
        new MADs()
    );

    static class QueryTranslation {
        final Query query;
        // Agg filter / Function or Agg association
        final AggFilter aggFilter;

        QueryTranslation(Query query) {
            this(query, null);
        }

        QueryTranslation(Query query, AggFilter aggFilter) {
            this.query = query;
            this.aggFilter = aggFilter;
        }
    }

    static QueryTranslation toQuery(Expression e, boolean onAggs) {
        QueryTranslation translation = null;
        TranslatorHandler handler = new SqlTranslatorHandler(onAggs);
        for (SqlExpressionTranslator<?> translator : QUERY_TRANSLATORS) {
            translation = translator.translate(e, onAggs, handler);
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

    static QueryTranslation and(Source source, QueryTranslation left, QueryTranslation right) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }

        Query newQ = null;
        if (left.query != null || right.query != null) {
            newQ = ExpressionTranslators.and(source, left.query, right.query);
        }

        AggFilter aggFilter;

        if (left.aggFilter == null) {
            aggFilter = right.aggFilter;
        } else if (right.aggFilter == null) {
            aggFilter = left.aggFilter;
        } else {
            aggFilter = new AndAggFilter(left.aggFilter, right.aggFilter);
        }

        return new QueryTranslation(newQ, aggFilter);
    }

    static QueryTranslation or(Source source, QueryTranslation left, QueryTranslation right) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }

        Query newQ = null;
        if (left.query != null || right.query != null) {
            newQ = ExpressionTranslators.or(source, left.query, right.query);
        }

        AggFilter aggFilter = null;

        if (left.aggFilter == null) {
            aggFilter = right.aggFilter;
        } else if (right.aggFilter == null) {
            aggFilter = left.aggFilter;
        } else {
            aggFilter = new OrAggFilter(left.aggFilter, right.aggFilter);
        }

        return new QueryTranslation(newQ, aggFilter);
    }

    static String nameOf(Expression e) {
        if (e instanceof DateTimeFunction dateTimeFunction) {
            return nameOf(dateTimeFunction.field());
        }
        if (e instanceof NamedExpression namedExpression) {
            return namedExpression.name();
        } else {
            return e.sourceText();
        }
    }

    static String field(AggregateFunction af, Expression arg) {
        if (arg.foldable()) {
            return String.valueOf(arg.fold());
        }
        if (arg instanceof FieldAttribute field) {
            // COUNT(DISTINCT) uses cardinality aggregation which works on exact values (not changed by analyzers or normalizers)
            if ((af instanceof Count && ((Count) af).distinct()) || af instanceof TopHits) {
                // use the `keyword` version of the field, if there is one
                return field.exactAttribute().name();
            }
            return field.name();
        }
        throw new SqlIllegalArgumentException(
            "Does not know how to convert argument {} for function {}",
            arg.nodeString(),
            af.nodeString()
        );
    }

    private static boolean isFieldOrLiteral(Expression e) {
        return e.foldable() || e instanceof FieldAttribute;
    }

    private static AggSource asFieldOrLiteralOrScript(AggregateFunction af) {
        return asFieldOrLiteralOrScript(af, af.field());
    }

    private static AggSource asFieldOrLiteralOrScript(AggregateFunction af, Expression e) {
        if (e == null) {
            return null;
        }
        return isFieldOrLiteral(e) ? AggSource.of(field(af, e)) : AggSource.of(((ScalarFunction) e).asScript());
    }

    // TODO: see whether escaping is needed
    @SuppressWarnings("rawtypes")
    static class Likes extends SqlExpressionTranslator<RegexMatch> {

        @Override
        protected QueryTranslation asQuery(RegexMatch e, boolean onAggs, TranslatorHandler handler) {
            Check.isTrue(onAggs == false, "Like not supported within an aggregation context");
            return new QueryTranslation(org.elasticsearch.xpack.ql.planner.ExpressionTranslators.Likes.doTranslate(e, handler));
        }
    }

    static class StringQueries extends SqlExpressionTranslator<StringQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(StringQueryPredicate q, boolean onAggs, TranslatorHandler handler) {
            Check.isTrue(onAggs == false, "Like not supported within an aggregation context");
            return new QueryTranslation(org.elasticsearch.xpack.ql.planner.ExpressionTranslators.StringQueries.doTranslate(q, handler));
        }
    }

    static class Matches extends SqlExpressionTranslator<MatchQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(MatchQueryPredicate q, boolean onAggs, TranslatorHandler handler) {
            Check.isTrue(onAggs == false, "Like not supported within an aggregation context");
            return new QueryTranslation(org.elasticsearch.xpack.ql.planner.ExpressionTranslators.Matches.doTranslate(q, handler));
        }
    }

    static class MultiMatches extends SqlExpressionTranslator<MultiMatchQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(MultiMatchQueryPredicate q, boolean onAggs, TranslatorHandler handler) {
            Check.isTrue(onAggs == false, "Like not supported within an aggregation context");
            return new QueryTranslation(org.elasticsearch.xpack.ql.planner.ExpressionTranslators.MultiMatches.doTranslate(q, handler));
        }
    }

    static class BinaryLogic extends SqlExpressionTranslator<org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic> {

        @Override
        protected QueryTranslation asQuery(
            org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic e,
            boolean onAggs,
            TranslatorHandler handler
        ) {
            if (e instanceof And) {
                return and(e.source(), toQuery(e.left(), onAggs), toQuery(e.right(), onAggs));
            }
            if (e instanceof Or) {
                return or(e.source(), toQuery(e.left(), onAggs), toQuery(e.right(), onAggs));
            }

            return null;
        }
    }

    static class Nots extends SqlExpressionTranslator<Not> {

        @Override
        protected QueryTranslation asQuery(Not not, boolean onAggs, TranslatorHandler handler) {
            Query query = null;
            AggFilter aggFilter = null;

            if (onAggs) {
                aggFilter = new AggFilter(id(not), not.asScript());
            } else {
                query = org.elasticsearch.xpack.ql.planner.ExpressionTranslators.Nots.doTranslate(not, handler);
            }

            return new QueryTranslation(query, aggFilter);
        }
    }

    static class IsNotNullTranslator extends SqlExpressionTranslator<IsNotNull> {

        @Override
        protected QueryTranslation asQuery(IsNotNull isNotNull, boolean onAggs, TranslatorHandler handler) {
            Query query = null;
            AggFilter aggFilter = null;

            if (onAggs) {
                aggFilter = new AggFilter(id(isNotNull), isNotNull.asScript());
            } else {
                query = ExpressionTranslators.IsNotNulls.doTranslate(isNotNull, handler);
            }

            return new QueryTranslation(query, aggFilter);
        }
    }

    static class IsNullTranslator extends SqlExpressionTranslator<IsNull> {

        @Override
        protected QueryTranslation asQuery(IsNull isNull, boolean onAggs, TranslatorHandler handler) {
            Query query = null;
            AggFilter aggFilter = null;

            if (onAggs) {
                aggFilter = new AggFilter(id(isNull), isNull.asScript());
            } else {
                query = ExpressionTranslators.IsNulls.doTranslate(isNull, handler);
            }

            return new QueryTranslation(query, aggFilter);
        }
    }

    // assume the Optimizer properly orders the predicates to ease the translation
    static class BinaryComparisons extends SqlExpressionTranslator<BinaryComparison> {

        @Override
        protected QueryTranslation asQuery(BinaryComparison bc, boolean onAggs, TranslatorHandler handler) {
            org.elasticsearch.xpack.ql.planner.ExpressionTranslators.BinaryComparisons.checkBinaryComparison(bc);

            Query query = null;
            AggFilter aggFilter = null;

            //
            // Agg context means HAVING -> PipelineAggs
            //
            if (onAggs) {
                aggFilter = new AggFilter(id(bc.left()), bc.asScript());
            } else {
                query = translateQuery(bc, handler);
            }
            return new QueryTranslation(query, aggFilter);
        }

        private static Query translateQuery(BinaryComparison bc, TranslatorHandler handler) {
            Source source = bc.source();
            Object value = valueOf(bc.right());

            // Possible geo optimization
            if (bc.left() instanceof StDistance && value instanceof Number) {
                if (bc instanceof LessThan || bc instanceof LessThanOrEqual) {
                    // Special case for ST_Distance translatable into geo_distance query
                    StDistance stDistance = (StDistance) bc.left();
                    if (stDistance.left() instanceof FieldAttribute && stDistance.right().foldable()) {
                        Object geoShape = valueOf(stDistance.right());
                        if (geoShape instanceof GeoShape) {
                            Geometry geometry = ((GeoShape) geoShape).toGeometry();
                            if (geometry instanceof Point) {
                                String field = nameOf(stDistance.left());
                                Query query = new GeoDistanceQuery(
                                    source,
                                    field,
                                    ((Number) value).doubleValue(),
                                    ((Point) geometry).getY(),
                                    ((Point) geometry).getX()
                                );
                                return ExpressionTranslator.wrapIfNested(query, stDistance.left());
                            }
                        }
                    }
                }
            }
            // fallback default
            return org.elasticsearch.xpack.ql.planner.ExpressionTranslators.BinaryComparisons.doTranslate(bc, handler);
        }
    }

    // assume the Optimizer properly orders the predicates to ease the translation
    static class InComparisons extends SqlExpressionTranslator<In> {

        @Override
        protected QueryTranslation asQuery(In in, boolean onAggs, TranslatorHandler handler) {

            Query query = null;
            AggFilter aggFilter = null;

            //
            // Agg context means HAVING -> PipelineAggs
            //
            if (onAggs) {
                aggFilter = new AggFilter(id(in.value()), in.asScript());
            } else {
                query = org.elasticsearch.xpack.ql.planner.ExpressionTranslators.InComparisons.doTranslate(in, handler);
            }
            return new QueryTranslation(query, aggFilter);
        }
    }

    static class Ranges extends SqlExpressionTranslator<Range> {

        @Override
        protected QueryTranslation asQuery(Range r, boolean onAggs, TranslatorHandler handler) {
            Expression e = r.value();

            Query query = null;
            AggFilter aggFilter = null;

            //
            // Agg context means HAVING -> PipelineAggs
            //
            if (onAggs) {
                aggFilter = new AggFilter(id(e), r.asScript());
            } else {
                query = org.elasticsearch.xpack.ql.planner.ExpressionTranslators.Ranges.doTranslate(r, handler);
            }
            return new QueryTranslation(query, aggFilter);
        }
    }

    static class Scalars extends SqlExpressionTranslator<ScalarFunction> {

        @Override
        protected QueryTranslation asQuery(ScalarFunction f, boolean onAggs, TranslatorHandler handler) {

            Query query = null;
            AggFilter aggFilter = null;

            if (onAggs) {
                aggFilter = new AggFilter(id(f), f.asScript());
            } else {
                query = ExpressionTranslators.Scalars.doTranslate(f, handler);
            }

            return new QueryTranslation(query, aggFilter);
        }
    }

    abstract static class SqlExpressionTranslator<E extends Expression> {

        private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        @SuppressWarnings("unchecked")
        public QueryTranslation translate(Expression exp, boolean onAggs, TranslatorHandler handler) {
            return (typeToken.isInstance(exp) ? asQuery((E) exp, onAggs, handler) : null);
        }

        protected abstract QueryTranslation asQuery(E e, boolean onAggs, TranslatorHandler handler);

    }

    //
    // Agg translators
    //

    static class CountAggs extends SingleValueAggTranslator<Count> {

        @Override
        protected LeafAgg toAgg(String id, Count c) {
            if (c.distinct()) {
                return new CardinalityAgg(id, asFieldOrLiteralOrScript(c));
            } else {
                return new FilterExistsAgg(id, asFieldOrLiteralOrScript(c));
            }
        }
    }

    static class Sums extends SingleValueAggTranslator<Sum> {

        @Override
        protected LeafAgg toAgg(String id, Sum s) {
            return new SumAgg(id, asFieldOrLiteralOrScript(s));
        }
    }

    static class Avgs extends SingleValueAggTranslator<Avg> {

        @Override
        protected LeafAgg toAgg(String id, Avg a) {
            return new AvgAgg(id, asFieldOrLiteralOrScript(a));
        }
    }

    static class Maxes extends SingleValueAggTranslator<Max> {

        @Override
        protected LeafAgg toAgg(String id, Max m) {
            return new MaxAgg(id, asFieldOrLiteralOrScript(m));
        }
    }

    static class Mins extends SingleValueAggTranslator<Min> {

        @Override
        protected LeafAgg toAgg(String id, Min m) {
            return new MinAgg(id, asFieldOrLiteralOrScript(m));
        }
    }

    static class MADs extends SingleValueAggTranslator<MedianAbsoluteDeviation> {
        @Override
        protected LeafAgg toAgg(String id, MedianAbsoluteDeviation m) {
            return new MedianAbsoluteDeviationAgg(id, asFieldOrLiteralOrScript(m));
        }
    }

    static class Firsts extends TopHitsAggTranslator<First> {

        @Override
        protected LeafAgg toAgg(String id, First f) {
            return new TopHitsAgg(
                id,
                asFieldOrLiteralOrScript(f, f.field()),
                f.dataType(),
                asFieldOrLiteralOrScript(f, f.orderField()),
                f.orderField() == null ? null : f.orderField().dataType(),
                SortOrder.ASC
            );
        }
    }

    static class Lasts extends TopHitsAggTranslator<Last> {

        @Override
        protected LeafAgg toAgg(String id, Last l) {
            return new TopHitsAgg(
                id,
                asFieldOrLiteralOrScript(l, l.field()),
                l.dataType(),
                asFieldOrLiteralOrScript(l, l.orderField()),
                l.orderField() == null ? null : l.orderField().dataType(),
                SortOrder.DESC
            );
        }
    }

    static class StatsAggs extends CompoundAggTranslator<Stats> {

        @Override
        protected LeafAgg toAgg(String id, Stats s) {
            return new StatsAgg(id, asFieldOrLiteralOrScript(s));
        }
    }

    static class ExtendedStatsAggs extends CompoundAggTranslator<ExtendedStats> {

        @Override
        protected LeafAgg toAgg(String id, ExtendedStats e) {
            return new ExtendedStatsAgg(id, asFieldOrLiteralOrScript(e));
        }
    }

    static class MatrixStatsAggs extends CompoundAggTranslator<MatrixStats> {

        @Override
        protected LeafAgg toAgg(String id, MatrixStats m) {
            if (isFieldOrLiteral(m.field())) {
                return new MatrixStatsAgg(id, singletonList(field(m, m.field())));
            }
            throw new SqlIllegalArgumentException(
                "Cannot use scalar functions or operators: [{}] in aggregate functions [KURTOSIS] and [SKEWNESS]",
                m.field().toString()
            );
        }
    }

    static class PercentilesAggs extends CompoundAggTranslator<Percentiles> {

        @Override
        protected LeafAgg toAgg(String id, Percentiles p) {
            return new PercentilesAgg(id, asFieldOrLiteralOrScript(p), foldAndConvertToDoubles(p.percents()), p.percentilesConfig());
        }
    }

    static class PercentileRanksAggs extends CompoundAggTranslator<PercentileRanks> {

        @Override
        protected LeafAgg toAgg(String id, PercentileRanks p) {
            return new PercentileRanksAgg(id, asFieldOrLiteralOrScript(p), foldAndConvertToDoubles(p.values()), p.percentilesConfig());
        }
    }

    static class DateTimes extends SingleValueAggTranslator<Min> {

        @Override
        protected LeafAgg toAgg(String id, Min m) {
            return new MinAgg(id, asFieldOrLiteralOrScript(m));
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

    abstract static class TopHitsAggTranslator<C extends TopHits> extends AggTranslator<C> {

        @Override
        protected final LeafAgg asAgg(String id, C function) {
            return toAgg(id, function);
        }

        protected abstract LeafAgg toAgg(String id, C f);
    }

    private static List<Double> foldAndConvertToDoubles(List<Expression> list) {
        List<Double> values = new ArrayList<>(list.size());
        for (Expression e : list) {
            values.add((Double) SqlDataTypeConverter.convert(Foldables.valueOf(e), DataTypes.DOUBLE));
        }
        return values;
    }

}
