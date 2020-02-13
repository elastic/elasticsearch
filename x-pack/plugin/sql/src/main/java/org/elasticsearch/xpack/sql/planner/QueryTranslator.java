/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.ql.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.ql.querydsl.query.GeoDistanceQuery;
import org.elasticsearch.xpack.ql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.ql.querydsl.query.MultiMatchQuery;
import org.elasticsearch.xpack.ql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.ql.querydsl.query.NotQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.ql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.ql.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.ql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.ql.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.Holder;
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
import org.elasticsearch.xpack.sql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
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

import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.Expressions.id;
import static org.elasticsearch.xpack.sql.expression.Foldables.doubleValuesOf;
import static org.elasticsearch.xpack.sql.expression.Foldables.valueOf;

final class QueryTranslator {

    public static final String DATE_FORMAT = "strict_date_time";
    public static final String TIME_FORMAT = "strict_hour_minute_second_millis";

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
            newQ = and(source, left.query, right.query);
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

    static Query and(Source source, Query left, Query right) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return new BoolQuery(source, true, left, right);
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
            newQ = or(source, left.query, right.query);
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

    static Query or(Source source, Query left, Query right) {
        Check.isTrue(left != null || right != null, "Both expressions are null");

        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return new BoolQuery(source, false, left, right);
    }

    static String nameOf(Expression e) {
        if (e instanceof DateTimeFunction) {
            return nameOf(((DateTimeFunction) e).field());
        }
        if (e instanceof NamedExpression) {
            return ((NamedExpression) e).name();
        }
        else {
            return e.sourceText();
        }
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
            FieldAttribute field = (FieldAttribute) arg;
            // COUNT(DISTINCT) uses cardinality aggregation which works on exact values (not changed by analyzers or normalizers)
            if (af instanceof Count && ((Count) af).distinct()) {
                // use the `keyword` version of the field, if there is one
                return field.exactAttribute().name();
            }
            return field.name();
        }
        if (arg instanceof Literal) {
            return String.valueOf(((Literal) arg).value());
        }
        throw new SqlIllegalArgumentException("Does not know how to convert argument {} for function {}", arg.nodeString(),
                af.nodeString());
    }

    private static String topAggsField(AggregateFunction af, Expression e) {
        if (e == null) {
            return null;
        }
        if (e instanceof FieldAttribute) {
            return ((FieldAttribute) e).exactAttribute().name();
        }
        throw new SqlIllegalArgumentException("Does not know how to convert argument {} for function {}", e.nodeString(),
            af.nodeString());
    }

    // TODO: see whether escaping is needed
    @SuppressWarnings("rawtypes")
    static class Likes extends ExpressionTranslator<RegexMatch> {

        @Override
        protected QueryTranslation asQuery(RegexMatch e, boolean onAggs) {
            Query q = null;
            String targetFieldName = null;

            if (e.field() instanceof FieldAttribute) {
                targetFieldName = nameOf(((FieldAttribute) e.field()).exactAttribute());
            } else {
                throw new SqlIllegalArgumentException("Scalar function [{}] not allowed (yet) as argument for " + e.sourceText(),
                        Expressions.name(e.field()));
            }

            if (e instanceof Like) {
                LikePattern p = ((Like) e).pattern();
                q = new WildcardQuery(e.source(), targetFieldName, p.asLuceneWildcard());
            }

            if (e instanceof RLike) {
                String pattern = ((RLike) e).pattern();
                q = new RegexQuery(e.source(), targetFieldName, pattern);
            }

            return q != null ? new QueryTranslation(wrapIfNested(q, e.field())) : null;
        }
    }

    static class StringQueries extends ExpressionTranslator<StringQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(StringQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(new QueryStringQuery(q.source(), q.query(), q.fields(), q));
        }
    }

    static class Matches extends ExpressionTranslator<MatchQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(MatchQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(wrapIfNested(new MatchQuery(q.source(), nameOf(q.field()), q.query(), q), q.field()));
        }
    }

    static class MultiMatches extends ExpressionTranslator<MultiMatchQueryPredicate> {

        @Override
        protected QueryTranslation asQuery(MultiMatchQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(new MultiMatchQuery(q.source(), q.query(), q.fields(), q));
        }
    }

    static class BinaryLogic extends ExpressionTranslator<org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic> {

        @Override
        protected QueryTranslation asQuery(org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic e, boolean onAggs) {
            if (e instanceof And) {
                return and(e.source(), toQuery(e.left(), onAggs), toQuery(e.right(), onAggs));
            }
            if (e instanceof Or) {
                return or(e.source(), toQuery(e.left(), onAggs), toQuery(e.right(), onAggs));
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
                aggFilter = new AggFilter(id(not), not.asScript());
            } else {
                Expression e = not.field();
                Query wrappedQuery = toQuery(not.field(), false).query;
                Query q = wrappedQuery instanceof ScriptQuery ? new ScriptQuery(not.source(),
                        not.asScript()) : new NotQuery(not.source(), wrappedQuery);

                if (e instanceof FieldAttribute) {
                    query = wrapIfNested(q, e);
                }

                query = q;
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
                aggFilter = new AggFilter(id(isNotNull), isNotNull.asScript());
            } else {
                Query q = null;
                if (isNotNull.field() instanceof FieldAttribute) {
                    q = new ExistsQuery(isNotNull.source(), nameOf(isNotNull.field()));
                } else {
                    q = new ScriptQuery(isNotNull.source(), isNotNull.asScript());
                }
                final Query qu = q;
                query = handleQuery(isNotNull, isNotNull.field(), () -> qu);
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
                aggFilter = new AggFilter(id(isNull), isNull.asScript());
            } else {
                Query q = null;
                if (isNull.field() instanceof FieldAttribute) {
                    q = new NotQuery(isNull.source(), new ExistsQuery(isNull.source(), nameOf(isNull.field())));
                } else {
                    q = new ScriptQuery(isNull.source(), isNull.asScript());
                }
                final Query qu = q;

                query = handleQuery(isNull, isNull.field(), () -> qu);
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
                    bc.right().sourceLocation().getLineNumber(), bc.right().sourceLocation().getColumnNumber(),
                    Expressions.name(bc.right()), bc.symbol());

            Query query = null;
            AggFilter aggFilter = null;

            //
            // Agg context means HAVING -> PipelineAggs
            //
            if (onAggs) {
                aggFilter = new AggFilter(id(bc.left()), bc.asScript());
            } else {
                query = handleQuery(bc, bc.left(), () -> translateQuery(bc));
            }
            return new QueryTranslation(query, aggFilter);
        }

        private static Query translateQuery(BinaryComparison bc) {
            Source source = bc.source();
            String name = nameOf(bc.left());
            Object value = valueOf(bc.right());
            String format = dateFormat(bc.left());
            boolean isDateLiteralComparison = false;

            // for a date constant comparison, we need to use a format for the date, to make sure that the format is the same
            // no matter the timezone provided by the user
            if ((value instanceof ZonedDateTime || value instanceof OffsetTime) && format == null) {
                DateFormatter formatter;
                if (value instanceof ZonedDateTime) {
                    formatter = DateFormatter.forPattern(DATE_FORMAT);
                    // RangeQueryBuilder accepts an Object as its parameter, but it will call .toString() on the ZonedDateTime instance
                    // which can have a slightly different format depending on the ZoneId used to create the ZonedDateTime
                    // Since RangeQueryBuilder can handle date as String as well, we'll format it as String and provide the format as well.
                    value = formatter.format((ZonedDateTime) value);
                } else {
                    formatter = DateFormatter.forPattern(TIME_FORMAT);
                    value = formatter.format((OffsetTime) value);
                }
                format = formatter.pattern();
                isDateLiteralComparison = true;
            }

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
                                return new GeoDistanceQuery(source, field, ((Number) value).doubleValue(),
                                    ((Point) geometry).getY(), ((Point) geometry).getX());
                            }
                        }
                    }
                }
            }
            if (bc instanceof GreaterThan) {
                return new RangeQuery(source, name, value, false, null, false, format);
            }
            if (bc instanceof GreaterThanOrEqual) {
                return new RangeQuery(source, name, value, true, null, false, format);
            }
            if (bc instanceof LessThan) {
                return new RangeQuery(source, name, null, false, value, false, format);
            }
            if (bc instanceof LessThanOrEqual) {
                return new RangeQuery(source, name, null, false, value, true, format);
            }
            if (bc instanceof Equals || bc instanceof NullEquals || bc instanceof NotEquals) {
                if (bc.left() instanceof FieldAttribute) {
                    // equality should always be against an exact match
                    // (which is important for strings)
                    name = ((FieldAttribute) bc.left()).exactAttribute().name();
                }
                Query query;
                if (isDateLiteralComparison) {
                    // dates equality uses a range query because it's the one that has a "format" parameter
                    query = new RangeQuery(source, name, value, true, value, true, format);
                } else {
                    query = new TermQuery(source, name, value);
                }
                if (bc instanceof NotEquals) {
                    query = new NotQuery(source, query);
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

            Query query = null;
            AggFilter aggFilter = null;

            //
            // Agg context means HAVING -> PipelineAggs
            //
            if (onAggs) {
                aggFilter = new AggFilter(id(in.value()), in.asScript());
            }
            else {
                Query q = null;
                if (in.value() instanceof FieldAttribute) {
                    // equality should always be against an exact match (which is important for strings)
                    FieldAttribute fa = (FieldAttribute) in.value();
                    List<Expression> list = in.list();
                    // TODO: this needs to be handled inside the optimizer
                    list.removeIf(e -> DataTypes.isNull(e.dataType()));
                    DataType dt = list.get(0).dataType();
                    Set<Object> set = new LinkedHashSet<>(CollectionUtils.mapSize(list.size()));
                    for (Expression e : list) {
                        set.add(SqlDataTypeConverter.convert(e.fold(), dt));
                    }
                    q = new TermsQuery(in.source(), fa.exactAttribute().name(), set);
                } else {
                    q = new ScriptQuery(in.source(), in.asScript());
                }
                Query qu = q;
                query = handleQuery(in, in.value(), () -> qu);
            }
            return new QueryTranslation(query, aggFilter);
        }
    }

    static class Ranges extends ExpressionTranslator<Range> {

        @Override
        protected QueryTranslation asQuery(Range r, boolean onAggs) {
            Expression e = r.value();

            Query query = null;
            AggFilter aggFilter = null;

            //
            // Agg context means HAVING -> PipelineAggs
            //
            if (onAggs) {
                aggFilter = new AggFilter(id(e), r.asScript());
            } else {

                Holder<Object> lower = new Holder<>(valueOf(r.lower()));
                Holder<Object> upper = new Holder<>(valueOf(r.upper()));
                Holder<String> format = new Holder<>(dateFormat(r.value()));

                // for a date constant comparison, we need to use a format for the date, to make sure that the format is the same
                // no matter the timezone provided by the user
                if (format.get() == null) {
                    DateFormatter formatter = null;
                    if (lower.get() instanceof ZonedDateTime || upper.get() instanceof ZonedDateTime) {
                        formatter = DateFormatter.forPattern(DATE_FORMAT);
                    } else if (lower.get() instanceof OffsetTime || upper.get() instanceof OffsetTime) {
                        formatter = DateFormatter.forPattern(TIME_FORMAT);
                    }
                    if (formatter != null) {
                        // RangeQueryBuilder accepts an Object as its parameter, but it will call .toString() on the ZonedDateTime
                        // instance which can have a slightly different format depending on the ZoneId used to create the ZonedDateTime
                        // Since RangeQueryBuilder can handle date as String as well, we'll format it as String and provide the format.
                        if (lower.get() instanceof ZonedDateTime || lower.get() instanceof OffsetTime) {
                            lower.set(formatter.format((TemporalAccessor) lower.get()));
                        }
                        if (upper.get() instanceof ZonedDateTime || upper.get() instanceof OffsetTime) {
                            upper.set(formatter.format((TemporalAccessor) upper.get()));
                        }
                        format.set(formatter.pattern());
                    }
                }

                query = handleQuery(r, r.value(),
                        () -> new RangeQuery(r.source(), nameOf(r.value()), lower.get(), r.includeLower(), upper.get(), r.includeUpper(),
                                format.get()));
            }
            return new QueryTranslation(query, aggFilter);
        }
    }

    static class Scalars extends ExpressionTranslator<ScalarFunction> {

        @Override
        protected QueryTranslation asQuery(ScalarFunction f, boolean onAggs) {
            ScriptTemplate script = f.asScript();

            Query query = null;
            AggFilter aggFilter = null;

            if (onAggs) {
                aggFilter = new AggFilter(id(f), script);
            } else {
                query = handleQuery(f, f, () -> new ScriptQuery(f.source(), script));
            }

            return new QueryTranslation(query, aggFilter);
        }
    }


    //
    // Agg translators
    //

    static class CountAggs extends SingleValueAggTranslator<Count> {

        @Override
        protected LeafAgg toAgg(String id, Count c) {
            if (c.distinct()) {
                return new CardinalityAgg(id, field(c));
            } else {
                return new FilterExistsAgg(id, field(c));
            }
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

    static class MADs extends SingleValueAggTranslator<MedianAbsoluteDeviation> {
        @Override
        protected LeafAgg toAgg(String id, MedianAbsoluteDeviation m) {
            return new MedianAbsoluteDeviationAgg(id, field(m));
        }
    }

    static class Firsts extends TopHitsAggTranslator<First> {

        @Override
        protected LeafAgg toAgg(String id, First f) {
            return new TopHitsAgg(id, topAggsField(f, f.field()), f.dataType(),
                topAggsField(f, f.orderField()), f.orderField() == null ? null : f.orderField().dataType(), SortOrder.ASC);
        }
    }

    static class Lasts extends TopHitsAggTranslator<Last> {

        @Override
        protected LeafAgg toAgg(String id, Last l) {
            return new TopHitsAgg(id, topAggsField(l, l.field()), l.dataType(),
                topAggsField(l, l.orderField()), l.orderField() == null ? null : l.orderField().dataType(), SortOrder.DESC);
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

    abstract static class TopHitsAggTranslator<C extends TopHits> extends AggTranslator<C> {

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
            Query q = query.get();
            if (field instanceof StDistance && q instanceof GeoDistanceQuery) {
                return wrapIfNested(q, ((StDistance) field).left());
            }
            if (field instanceof FieldAttribute) {
                return wrapIfNested(q, field);
            }
            return new ScriptQuery(sf.source(), sf.asScript());
        }

        protected static Query wrapIfNested(Query query, Expression exp) {
            if (exp instanceof FieldAttribute) {
                FieldAttribute fa = (FieldAttribute) exp;
                if (fa.isNested()) {
                    return new NestedQuery(fa.source(), fa.nestedParent().name(), query);
                }
            }
            return query;
        }
    }
}