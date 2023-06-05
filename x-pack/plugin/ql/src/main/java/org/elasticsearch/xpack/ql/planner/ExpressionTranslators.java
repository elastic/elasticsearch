/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.StartsWith;
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
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardLike;
import org.elasticsearch.xpack.ql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.ql.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.ql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.ql.querydsl.query.MultiMatchQuery;
import org.elasticsearch.xpack.ql.querydsl.query.NotQuery;
import org.elasticsearch.xpack.ql.querydsl.query.PrefixQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.ql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.ql.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.ql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.ql.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Check;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.ql.type.DataTypes.IP;

public final class ExpressionTranslators {

    public static final String DATE_FORMAT = "strict_date_optional_time_nanos";
    public static final String TIME_FORMAT = "strict_hour_minute_second_fraction";

    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = List.of(
        new BinaryComparisons(),
        new Ranges(),
        new BinaryLogic(),
        new IsNulls(),
        new IsNotNulls(),
        new Nots(),
        new Likes(),
        new InComparisons(),
        new StringQueries(),
        new Matches(),
        new MultiMatches(),
        new Scalars()
    );

    public static Query toQuery(Expression e) {
        return toQuery(e, new QlTranslatorHandler());
    }

    public static Query toQuery(Expression e, TranslatorHandler handler) {
        Query translation = null;
        for (ExpressionTranslator<?> translator : QUERY_TRANSLATORS) {
            translation = translator.translate(e, handler);
            if (translation != null) {
                return translation;
            }
        }

        throw new QlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }

    public static Object valueOf(Expression e) {
        if (e.foldable()) {
            return e.fold();
        }
        throw new QlIllegalArgumentException("Cannot determine value for {}", e);
    }

    // TODO: see whether escaping is needed
    @SuppressWarnings("rawtypes")
    public static class Likes extends ExpressionTranslator<RegexMatch> {

        @Override
        protected Query asQuery(RegexMatch e, TranslatorHandler handler) {
            return doTranslate(e, handler);
        }

        public static Query doTranslate(RegexMatch e, TranslatorHandler handler) {
            Query q = null;
            String targetFieldName = null;

            if (e.field() instanceof FieldAttribute) {
                targetFieldName = handler.nameOf(((FieldAttribute) e.field()).exactAttribute());
                if (e instanceof Like l) {
                    q = new WildcardQuery(e.source(), targetFieldName, l.pattern().asLuceneWildcard(), l.caseInsensitive());
                }
                if (e instanceof WildcardLike l) {
                    q = new WildcardQuery(e.source(), targetFieldName, l.pattern().asLuceneWildcard(), l.caseInsensitive());
                }
                if (e instanceof RLike rl) {
                    q = new RegexQuery(e.source(), targetFieldName, rl.pattern().asJavaRegex(), rl.caseInsensitive());
                }
            } else {
                q = new ScriptQuery(e.source(), e.asScript());
            }

            return wrapIfNested(q, e.field());
        }
    }

    public static class StringQueries extends ExpressionTranslator<StringQueryPredicate> {

        @Override
        protected Query asQuery(StringQueryPredicate q, TranslatorHandler handler) {
            return doTranslate(q, handler);
        }

        public static Query doTranslate(StringQueryPredicate q, TranslatorHandler handler) {
            return new QueryStringQuery(q.source(), q.query(), q.fields(), q);
        }
    }

    public static class Matches extends ExpressionTranslator<MatchQueryPredicate> {

        @Override
        protected Query asQuery(MatchQueryPredicate q, TranslatorHandler handler) {
            return doTranslate(q, handler);
        }

        public static Query doTranslate(MatchQueryPredicate q, TranslatorHandler handler) {
            return new MatchQuery(q.source(), handler.nameOf(q.field()), q.query(), q);
        }
    }

    public static class MultiMatches extends ExpressionTranslator<MultiMatchQueryPredicate> {

        @Override
        protected Query asQuery(MultiMatchQueryPredicate q, TranslatorHandler handler) {
            return doTranslate(q, handler);
        }

        public static Query doTranslate(MultiMatchQueryPredicate q, TranslatorHandler handler) {
            return new MultiMatchQuery(q.source(), q.query(), q.fields(), q);
        }
    }

    public static class BinaryLogic extends ExpressionTranslator<org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic> {

        @Override
        protected Query asQuery(org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic e, TranslatorHandler handler) {
            if (e instanceof And) {
                return and(e.source(), toQuery(e.left(), handler), toQuery(e.right(), handler));
            }
            if (e instanceof Or) {
                return or(e.source(), toQuery(e.left(), handler), toQuery(e.right(), handler));
            }

            return null;
        }
    }

    public static class Nots extends ExpressionTranslator<Not> {

        @Override
        protected Query asQuery(Not not, TranslatorHandler handler) {
            return doTranslate(not, handler);
        }

        public static Query doTranslate(Not not, TranslatorHandler handler) {
            Expression e = not.field();
            Query wrappedQuery = handler.asQuery(not.field());
            Query q = wrappedQuery instanceof ScriptQuery
                ? new ScriptQuery(not.source(), not.asScript())
                : new NotQuery(not.source(), wrappedQuery);

            return wrapIfNested(q, e);
        }
    }

    public static class IsNotNulls extends ExpressionTranslator<IsNotNull> {

        @Override
        protected Query asQuery(IsNotNull isNotNull, TranslatorHandler handler) {
            return doTranslate(isNotNull, handler);
        }

        public static Query doTranslate(IsNotNull isNotNull, TranslatorHandler handler) {
            return handler.wrapFunctionQuery(isNotNull, isNotNull.field(), () -> translate(isNotNull, handler));
        }

        private static Query translate(IsNotNull isNotNull, TranslatorHandler handler) {
            return new ExistsQuery(isNotNull.source(), handler.nameOf(isNotNull.field()));
        }
    }

    public static class IsNulls extends ExpressionTranslator<IsNull> {

        @Override
        protected Query asQuery(IsNull isNull, TranslatorHandler handler) {
            return doTranslate(isNull, handler);
        }

        public static Query doTranslate(IsNull isNull, TranslatorHandler handler) {
            return handler.wrapFunctionQuery(isNull, isNull.field(), () -> translate(isNull, handler));
        }

        private static Query translate(IsNull isNull, TranslatorHandler handler) {
            return new NotQuery(isNull.source(), new ExistsQuery(isNull.source(), handler.nameOf(isNull.field())));
        }
    }

    // assume the Optimizer properly orders the predicates to ease the translation
    public static class BinaryComparisons extends ExpressionTranslator<BinaryComparison> {

        @Override
        protected Query asQuery(BinaryComparison bc, TranslatorHandler handler) {
            return doTranslate(bc, handler);
        }

        public static void checkBinaryComparison(BinaryComparison bc) {
            Check.isTrue(
                bc.right().foldable(),
                "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
                bc.right().sourceLocation().getLineNumber(),
                bc.right().sourceLocation().getColumnNumber(),
                Expressions.name(bc.right()),
                bc.symbol()
            );
        }

        public static Query doTranslate(BinaryComparison bc, TranslatorHandler handler) {
            checkBinaryComparison(bc);
            return handler.wrapFunctionQuery(bc, bc.left(), () -> translate(bc, handler));
        }

        static Query translate(BinaryComparison bc, TranslatorHandler handler) {
            FieldAttribute field = checkIsFieldAttribute(bc.left());
            Source source = bc.source();
            String name = handler.nameOf(field);
            Object value = valueOf(bc.right());
            String format = null;
            boolean isDateLiteralComparison = false;

            // for a date constant comparison, we need to use a format for the date, to make sure that the format is the same
            // no matter the timezone provided by the user
            if (value instanceof ZonedDateTime || value instanceof OffsetTime) {
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
            } else if (field.dataType() == IP && value instanceof BytesRef bytesRef) {
                value = DocValueFormat.IP.format(bytesRef);
            }

            ZoneId zoneId = null;
            if (DataTypes.isDateTime(field.dataType())) {
                zoneId = bc.zoneId();
            }
            if (bc instanceof GreaterThan) {
                return new RangeQuery(source, name, value, false, null, false, format, zoneId);
            }
            if (bc instanceof GreaterThanOrEqual) {
                return new RangeQuery(source, name, value, true, null, false, format, zoneId);
            }
            if (bc instanceof LessThan) {
                return new RangeQuery(source, name, null, false, value, false, format, zoneId);
            }
            if (bc instanceof LessThanOrEqual) {
                return new RangeQuery(source, name, null, false, value, true, format, zoneId);
            }
            if (bc instanceof Equals || bc instanceof NullEquals || bc instanceof NotEquals) {
                // equality should always be against an exact match
                // (which is important for strings)
                name = field.exactAttribute().name();

                Query query;
                if (isDateLiteralComparison) {
                    // dates equality uses a range query because it's the one that has a "format" parameter
                    query = new RangeQuery(source, name, value, true, value, true, format, zoneId);
                } else {
                    query = new TermQuery(source, name, value);
                }
                if (bc instanceof NotEquals) {
                    query = new NotQuery(source, query);
                }
                return query;
            }

            throw new QlIllegalArgumentException("Don't know how to translate binary comparison [{}] in [{}]", bc.right().nodeString(), bc);
        }
    }

    public static class Ranges extends ExpressionTranslator<Range> {

        @Override
        protected Query asQuery(Range r, TranslatorHandler handler) {
            return doTranslate(r, handler);
        }

        public static Query doTranslate(Range r, TranslatorHandler handler) {
            return handler.wrapFunctionQuery(r, r.value(), () -> translate(r, handler));
        }

        private static RangeQuery translate(Range r, TranslatorHandler handler) {
            Object lower = valueOf(r.lower());
            Object upper = valueOf(r.upper());
            String format = null;

            // for a date constant comparison, we need to use a format for the date, to make sure that the format is the same
            // no matter the timezone provided by the user
            DateFormatter formatter = null;
            if (lower instanceof ZonedDateTime || upper instanceof ZonedDateTime) {
                formatter = DateFormatter.forPattern(DATE_FORMAT);
            } else if (lower instanceof OffsetTime || upper instanceof OffsetTime) {
                formatter = DateFormatter.forPattern(TIME_FORMAT);
            }
            if (formatter != null) {
                // RangeQueryBuilder accepts an Object as its parameter, but it will call .toString() on the ZonedDateTime
                // instance which can have a slightly different format depending on the ZoneId used to create the ZonedDateTime
                // Since RangeQueryBuilder can handle date as String as well, we'll format it as String and provide the format.
                if (lower instanceof ZonedDateTime || lower instanceof OffsetTime) {
                    lower = formatter.format((TemporalAccessor) lower);
                }
                if (upper instanceof ZonedDateTime || upper instanceof OffsetTime) {
                    upper = formatter.format((TemporalAccessor) upper);
                }
                format = formatter.pattern();
            }
            return new RangeQuery(
                r.source(),
                handler.nameOf(r.value()),
                lower,
                r.includeLower(),
                upper,
                r.includeUpper(),
                format,
                r.zoneId()
            );
        }
    }

    public static class InComparisons extends ExpressionTranslator<In> {

        @Override
        protected Query asQuery(In in, TranslatorHandler handler) {
            return doTranslate(in, handler);
        }

        public static Query doTranslate(In in, TranslatorHandler handler) {
            return handler.wrapFunctionQuery(in, in.value(), () -> translate(in, handler));
        }

        private static Query translate(In in, TranslatorHandler handler) {
            FieldAttribute field = checkIsFieldAttribute(in.value());
            boolean needsTypeSpecificValueHandling = DataTypes.isDateTime(field.dataType()) || field.dataType() == IP;

            Set<Object> terms = new LinkedHashSet<>();
            List<Query> queries = new ArrayList<>();

            for (Expression rhs : in.list()) {
                if (DataTypes.isNull(rhs.dataType()) == false) {
                    if (needsTypeSpecificValueHandling) {
                        // delegates to BinaryComparisons translator to ensure consistent handling of date and time values
                        Query query = BinaryComparisons.translate(new Equals(in.source(), in.value(), rhs, in.zoneId()), handler);

                        if (query instanceof TermQuery) {
                            terms.add(((TermQuery) query).value());
                        } else {
                            queries.add(query);
                        }
                    } else {
                        terms.add(valueOf(rhs));
                    }
                }
            }

            if (terms.isEmpty() == false) {
                String fieldName = field.exactAttribute().name();
                queries.add(new TermsQuery(in.source(), fieldName, terms));
            }

            return queries.stream().reduce((q1, q2) -> or(in.source(), q1, q2)).get();
        }
    }

    public static class Scalars extends ExpressionTranslator<ScalarFunction> {

        @Override
        protected Query asQuery(ScalarFunction f, TranslatorHandler handler) {
            return doTranslate(f, handler);
        }

        public static Query doTranslate(ScalarFunction f, TranslatorHandler handler) {
            Query q = doKnownTranslate(f, handler);
            if (q != null) {
                return q;
            }
            return handler.wrapFunctionQuery(f, f, () -> new ScriptQuery(f.source(), f.asScript()));
        }

        public static Query doKnownTranslate(ScalarFunction f, TranslatorHandler handler) {
            if (f instanceof StartsWith sw) {
                if (sw.input() instanceof FieldAttribute && sw.pattern().foldable()) {
                    String targetFieldName = handler.nameOf(((FieldAttribute) sw.input()).exactAttribute());
                    String pattern = (String) sw.pattern().fold();

                    return new PrefixQuery(f.source(), targetFieldName, pattern, sw.isCaseInsensitive());
                }
            }
            return null;
        }
    }

    public static Query or(Source source, Query left, Query right) {
        return boolQuery(source, left, right, false);
    }

    public static Query and(Source source, Query left, Query right) {
        return boolQuery(source, left, right, true);
    }

    private static Query boolQuery(Source source, Query left, Query right, boolean isAnd) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        List<Query> queries;
        // check if either side is already a bool query to an extra bool query
        if (left instanceof BoolQuery bool && bool.isAnd() == isAnd) {
            queries = CollectionUtils.combine(bool.queries(), right);
        } else if (right instanceof BoolQuery bool && bool.isAnd() == isAnd) {
            queries = CollectionUtils.combine(bool.queries(), left);
        } else {
            queries = Arrays.asList(left, right);
        }
        return new BoolQuery(source, isAnd, queries);
    }
}
