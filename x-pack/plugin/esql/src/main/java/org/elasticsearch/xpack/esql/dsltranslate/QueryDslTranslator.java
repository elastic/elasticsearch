/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Turns a Query DSL {@link QueryBuilder} tree into an ES|QL {@link Expression} that means what the DSL means.
 *
 * <p>This is the generic, consumer-agnostic core of the DSL-to-ES|QL path. It is given a {@code fieldBinder} — a
 * function from a field name to the {@link Expression} that stands for that field on the source being translated
 * against. A present field binds to its attribute; a missing field binds to {@link Literal#NULL}. Because every leaf
 * predicate the translator emits is two-valued (never returns null — {@code mv_contains}, {@code mv_intersects},
 * {@code mv_in_range} and {@code IS NOT NULL} all have {@code Nullability.FALSE}, and the one-sided range comparisons
 * are wrapped by {@link #twoValued}), plain {@code AND}/{@code OR}/{@code NOT} composition over a null-bound leaf
 * reproduces the DSL leniency rules for free, negation included.
 *
 * <p>Literals are bound to the <em>field's</em> type, not the JSON value's: the graft runs after the analyzer, so
 * nothing downstream inserts the implicit cast a user-written {@code WHERE} would get. A JSON date string against a
 * {@code date} column must become a {@code datetime} literal here or the evaluator is handed the wrong block type.
 *
 * <p>The supported subset is the structural floor: {@code bool}, {@code term}, {@code terms}, {@code range},
 * {@code exists}, and {@code match_all}/{@code match_none}. Anything outside it — including a construct that carries an
 * option we cannot honor faithfully — raises {@link TranslationUnsupportedException} rather than silently
 * mis-translating; the caller decides the policy (the request filter degrades; a query function errors).
 */
public final class QueryDslTranslator {

    private final Function<String, Expression> fieldBinder;
    private final long nowInMillis;

    /**
     * @param fieldBinder resolves a DSL field name to the ES|QL expression standing for it on this source — the
     *                    source's attribute when the field exists, {@link Literal#NULL} when it does not.
     * @param nowInMillis the query's start time, epoch millis — the anchor for {@code now} date math on date bounds, so
     *                    that {@code "now-15m"} resolves to the same instant the index path would use for this request.
     */
    public QueryDslTranslator(Function<String, Expression> fieldBinder, long nowInMillis) {
        this.fieldBinder = fieldBinder;
        this.nowInMillis = nowInMillis;
    }

    /** Translate a DSL query into an ES|QL boolean predicate. */
    public Expression translate(QueryBuilder query) {
        if (query instanceof BoolQueryBuilder bool) {
            return bool(bool);
        }
        if (query instanceof TermQueryBuilder term) {
            return term(term);
        }
        if (query instanceof TermsQueryBuilder terms) {
            return terms(terms);
        }
        if (query instanceof ExistsQueryBuilder exists) {
            return exists(exists);
        }
        if (query instanceof MatchAllQueryBuilder) {
            return Literal.TRUE;
        }
        if (query instanceof MatchNoneQueryBuilder) {
            return Literal.FALSE;
        }
        if (query instanceof RangeQueryBuilder range) {
            return range(range);
        }
        throw new TranslationUnsupportedException(query.getName());
    }

    private Expression term(TermQueryBuilder term) {
        // A case-insensitive term is a different predicate; translating it as-is would silently under-match.
        if (term.caseInsensitive()) {
            throw new TranslationUnsupportedException("term[case_insensitive]");
        }
        // any-value equality: the field's values contain the term value
        Expression field = fieldBinder.apply(term.fieldName());
        // A term on a date field is a range over the value's rounding unit ("2020" means all of that year), not a
        // point — so a coarse or date-math value matches the same span the index path's term query does.
        if (isDate(field.dataType())) {
            return checkedLeaf(field, dateTermRange(field, field.dataType(), term.value(), null));
        }
        // A value that no value of the field's integral type can equal (a decimal, or one outside the type's range)
        // matches nothing, exactly as the index path's term query returns match-no-docs — never a truncated match.
        if (isPresent(field) && cannotEqualIntegral(field.dataType(), term.value())) {
            return Literal.FALSE;
        }
        return checkedLeaf(field, new MvContains(Source.EMPTY, field, literalFor(field, term.value())));
    }

    private Expression exists(ExistsQueryBuilder exists) {
        return new IsNotNull(Source.EMPTY, fieldBinder.apply(exists.fieldName()));
    }

    private Expression terms(TermsQueryBuilder terms) {
        // A terms-lookup fetches its values from another document at query time; there is nothing to translate, and
        // values() is null in that case (it would NPE below).
        if (terms.termsLookup() != null) {
            throw new TranslationUnsupportedException("terms[terms_lookup]");
        }
        List<Object> values = terms.values();
        // An empty terms list is legal DSL and matches nothing.
        if (values == null || values.isEmpty()) {
            return Literal.FALSE;
        }
        Expression field = fieldBinder.apply(terms.fieldName());
        // On a date field each term is a rounding-unit range, not a point (see the term branch); the set is their
        // union. Build it as an OR of per-value ranges so coarse and date-math values match the index-path span.
        if (isDate(field.dataType())) {
            Expression or = null;
            for (Object v : values) {
                Expression range = checkedLeaf(field, dateTermRange(field, field.dataType(), v, null));
                or = or == null ? range : new Or(Source.EMPTY, or, range);
            }
            return or;
        }
        // Drop values no value of an integral field can equal (decimals, out-of-range) — they match nothing, as the
        // index path's terms query does. If that empties the set, the whole clause matches nothing.
        if (isPresent(field)) {
            values = values.stream().filter(v -> cannotEqualIntegral(field.dataType(), v) == false).toList();
            if (values.isEmpty()) {
                return Literal.FALSE;
            }
        }
        // any-value set membership: the field's values intersect the term set
        return checkedLeaf(field, new MvIntersects(Source.EMPTY, field, listLiteralFor(field, values)));
    }

    private Expression bool(BoolQueryBuilder bool) {
        // How many should-clauses must match. Only two values have a faithful image as a plain OR: 1 (at least one
        // should clause is required) and 0 (should is optional, so in a filter context it drops out). Anything else
        // ("2", "50%", ...) needs real n-of-m counting, which an OR cannot express.
        //
        // Note this must ACCEPT an explicit 1: Kibana's "is one of" pill and every KQL `or` emit
        // bool{should:[...], minimum_should_match:1}. Refusing it would degrade the most common filters there are.
        Integer requiredShould = null; // null = not set, use the DSL default below
        String msm = bool.minimumShouldMatch();
        if (msm != null) {
            try {
                requiredShould = Integer.valueOf(msm.trim());
            } catch (NumberFormatException e) {
                throw new TranslationUnsupportedException("bool[minimum_should_match=" + msm + "]");
            }
            if (requiredShould != 0 && requiredShould != 1) {
                throw new TranslationUnsupportedException("bool[minimum_should_match=" + msm + "]");
            }
        }

        List<Expression> conjuncts = new ArrayList<>();
        for (QueryBuilder q : bool.must()) {
            conjuncts.add(translate(q));
        }
        for (QueryBuilder q : bool.filter()) {
            conjuncts.add(translate(q));
        }
        for (QueryBuilder q : bool.mustNot()) {
            conjuncts.add(new Not(Source.EMPTY, translate(q)));
        }

        if (bool.should().isEmpty() == false) {
            Expression or = null;
            for (QueryBuilder q : bool.should()) {
                Expression e = translate(q);
                or = or == null ? e : new Or(Source.EMPTY, or, e);
            }
            // The DSL default is 1 when the bool carries no must/filter, and 0 otherwise — must_not does NOT count
            // towards that (a bool of must_not + should still requires one should clause to match). Crucially, an
            // EXPLICIT minimum_should_match of 0 does NOT make a should-only bool match everything: Lucene still needs
            // one optional clause when there is no required (must/filter) clause, so msm=0 only drops the should group
            // when a must/filter is present. (msm=1 always requires one; msm=0 and the unset default coincide.)
            boolean hasMustOrFilter = bool.must().isEmpty() == false || bool.filter().isEmpty() == false;
            boolean shouldIsRequired = (requiredShould != null && requiredShould == 1) || hasMustOrFilter == false;
            if (shouldIsRequired) {
                conjuncts.add(or);
            }
        }

        if (conjuncts.isEmpty()) {
            return Literal.TRUE;
        }
        Expression and = conjuncts.get(0);
        for (int i = 1; i < conjuncts.size(); i++) {
            and = new And(Source.EMPTY, and, conjuncts.get(i));
        }
        return and;
    }

    private Expression range(RangeQueryBuilder range) {
        // A time zone shifts what the bounds mean; we parse them zone-naively, so honoring it is not something we can
        // fake. Reject rather than answer a differently-scoped question.
        if (range.timeZone() != null) {
            throw new TranslationUnsupportedException("range[time_zone]");
        }
        Expression field = fieldBinder.apply(range.fieldName());
        DataType type = field.dataType();
        DateFormatter formatter = range.format() == null ? null : DateFormatter.forPattern(range.format());

        boolean hasLower = range.from() != null;
        boolean hasUpper = range.to() != null;
        if (hasLower == false && hasUpper == false) {
            return Literal.TRUE;
        }

        // A date range carries its own rules the generic numeric path cannot fake: date math ("now-15m"), and a coarse
        // bound rounding to the edge of its unit ("2020-01" as an upper bound means the last millis of that month). The
        // index path resolves both through a DateMathParser with a per-bound round direction; we mirror it exactly and
        // fold the result to a closed inclusive interval, which is what mv_in_range wants. A MISSING date field keeps the
        // generic path so it stays null-bound and folds to false (leniency), never touching the parser.
        if (isDate(type) && isPresent(field)) {
            return dateRange(field, type, range, formatter, hasLower, hasUpper);
        }

        if (hasLower && hasUpper) {
            // Both bounds: this must be ONE any-value range test. Splitting it into two independent existentials
            // (mv_max >= lo AND mv_min <= hi) is an ENVELOPE test, which is wrong on multivalue fields: [0,100]
            // would satisfy (40,60) even though no single value lies inside it. mv_in_range is the exact predicate,
            // but it is closed/inclusive on both ends — so an exclusive bound is first normalized to the equivalent
            // inclusive one, which is only exact on whole-number types (there is no predecessor for a double).
            Object lower = coerce(field, range.from());
            Object upper = coerce(field, range.to());
            // The exclusive→inclusive normalization only matters for a PRESENT field. A missing field is null-bound and
            // the leaf folds to false regardless of the bounds, so skip it — otherwise an exclusive bound over a missing
            // field would wrongly degrade (unfiltered) where the index path's unmapped-field range matches nothing.
            if (isPresent(field) && (range.includeLower() == false || range.includeUpper() == false)) {
                if (isWholeNumbered(type) == false) {
                    throw new TranslationUnsupportedException("range[exclusive bound on " + type.typeName() + "]");
                }
                try {
                    if (range.includeLower() == false) {
                        lower = increment(lower, type, +1);
                    }
                    if (range.includeUpper() == false) {
                        upper = increment(upper, type, -1);
                    }
                } catch (ArithmeticException overflow) {
                    // The bound sits at the type's limit, so the open interval beyond it is empty.
                    return Literal.FALSE;
                }
            }
            return checkedLeaf(
                field,
                new MvInRange(Source.EMPTY, field, new Literal(Source.EMPTY, lower, type), new Literal(Source.EMPTY, upper, type))
            );
        }

        // Exactly one bound. Comparing against the field's extreme value is exact any-value here — "some value clears
        // the lower bound" is precisely "the largest value clears it" (and symmetrically for the upper bound with the
        // smallest). mv_max/mv_min are nullable, so twoValued() folds a missing field's null to false and the leniency
        // composes like the other leaves (missing field: must → nothing, must_not → all).
        if (hasLower) {
            Expression max = new MvMax(Source.EMPTY, field);
            Literal lo = literalFor(field, range.from());
            return checkedLeaf(
                field,
                twoValued(
                    range.includeLower()
                        ? new GreaterThanOrEqual(Source.EMPTY, max, lo, null)
                        : new GreaterThan(Source.EMPTY, max, lo, null)
                )
            );
        }
        Expression min = new MvMin(Source.EMPTY, field);
        Literal hi = literalFor(field, range.to());
        return checkedLeaf(
            field,
            twoValued(range.includeUpper() ? new LessThanOrEqual(Source.EMPTY, min, hi, null) : new LessThan(Source.EMPTY, min, hi, null))
        );
    }

    /**
     * Resolve a date range to a closed inclusive interval and build the any-value leaf over it, mirroring the index
     * path's {@code DateFieldType} resolution: each bound is parsed with the round direction that bound uses ({@code
     * roundUp} for an inclusive upper or an exclusive lower), then an exclusive bound is nudged one unit inward so the
     * interval is always closed. Both bounds → {@code mv_in_range}; one bound → an inclusive comparison against the
     * field's extreme value (the same any-value reduction the numeric path uses), wrapped {@link #twoValued}.
     */
    private Expression dateRange(
        Expression field,
        DataType type,
        RangeQueryBuilder range,
        DateFormatter formatter,
        boolean hasLower,
        boolean hasUpper
    ) {
        if (hasLower && hasUpper) {
            long lo = closedLowerBound(type, range.from(), formatter, range.includeLower());
            long hi = closedUpperBound(type, range.to(), formatter, range.includeUpper());
            if (lo > hi) {
                // Rounding collapsed the interval (e.g. lt and gt straddling the same unit) — it matches nothing.
                return Literal.FALSE;
            }
            return checkedLeaf(field, new MvInRange(Source.EMPTY, field, longLit(lo, type), longLit(hi, type)));
        }
        if (hasLower) {
            long lo = closedLowerBound(type, range.from(), formatter, range.includeLower());
            return checkedLeaf(
                field,
                twoValued(new GreaterThanOrEqual(Source.EMPTY, new MvMax(Source.EMPTY, field), longLit(lo, type), null))
            );
        }
        long hi = closedUpperBound(type, range.to(), formatter, range.includeUpper());
        return checkedLeaf(field, twoValued(new LessThanOrEqual(Source.EMPTY, new MvMin(Source.EMPTY, field), longLit(hi, type), null)));
    }

    /**
     * A {@code term}/{@code terms} value on a date field is the closed range from the start to the end of the value's
     * rounding unit — a full-precision instant collapses to a point, a coarse one ("2020") spans its whole unit.
     */
    private Expression dateTermRange(Expression field, DataType type, Object value, DateFormatter formatter) {
        long lo = dateBound(type, value, formatter, false);
        long hi = dateBound(type, value, formatter, true);
        return new MvInRange(Source.EMPTY, field, longLit(lo, type), longLit(hi, type));
    }

    /** The inclusive lower bound: round down for {@code gte}, up-then-plus-one for {@code gt}. */
    private long closedLowerBound(DataType type, Object value, DateFormatter formatter, boolean inclusive) {
        long l = dateBound(type, value, formatter, inclusive == false);
        return inclusive ? l : l + 1;
    }

    /** The inclusive upper bound: round up for {@code lte}, down-then-minus-one for {@code lt}. */
    private long closedUpperBound(DataType type, Object value, DateFormatter formatter, boolean inclusive) {
        long u = dateBound(type, value, formatter, inclusive);
        return inclusive ? u : u - 1;
    }

    /**
     * Parse one date bound to the field type's internal long. A numeric value is epoch <em>millis</em> on both date
     * types — matching the index field's {@code epoch_millis} parse — so it is converted to the type's resolution
     * (identity for {@code date}, ×10⁶ for {@code date_nanos}), never read as a raw nanos count. A string goes through a
     * {@link org.elasticsearch.common.time.DateMathParser} anchored at the query's {@code now}, with {@code roundUp}
     * choosing the edge of the value's rounding unit. The default formatters mirror the index field defaults (they
     * include {@code epoch_millis}), so a string epoch-millis bound resolves the same way a numeric one does.
     */
    private long dateBound(DataType type, Object value, DateFormatter formatter, boolean roundUp) {
        DateFieldMapper.Resolution resolution = type == DataType.DATE_NANOS
            ? DateFieldMapper.Resolution.NANOSECONDS
            : DateFieldMapper.Resolution.MILLISECONDS;
        try {
            if (value instanceof Number n) {
                return resolution.convert(Instant.ofEpochMilli(n.longValue()));
            }
            DateFormatter effective = formatter != null
                ? formatter
                : (type == DataType.DATE_NANOS
                    ? DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER
                    : DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);
            Instant instant = effective.toDateMathParser().parse(String.valueOf(value), () -> nowInMillis, roundUp, null);
            return resolution.convert(instant);
        } catch (RuntimeException e) {
            // An unparseable bound, a date-math expression we cannot resolve, or a numeric epoch out of the type's
            // representable range (e.g. a pre-1970 date_nanos) cannot be translated faithfully — degrade this clause.
            throw new TranslationUnsupportedException("range[date bound on " + type.typeName() + "]");
        }
    }

    private static boolean isDate(DataType type) {
        return type == DataType.DATETIME || type == DataType.DATE_NANOS;
    }

    private static Literal longLit(long value, DataType type) {
        return new Literal(Source.EMPTY, value, type);
    }

    /**
     * Wrap a nullable comparison so a null result (a missing or empty field) becomes {@code false}, keeping the leaf
     * two-valued. Without this a one-sided range over a missing field is {@code null}, and {@code NOT null} is {@code
     * null} — so {@code must_not} over a missing field would drop the row instead of matching everything.
     */
    private static Expression twoValued(Expression comparison) {
        return new Coalesce(Source.EMPTY, comparison, List.of(Literal.FALSE));
    }

    /**
     * Guards a leaf built over a PRESENT field. The graft runs after the analyzer and skips the Verifier, so a leaf
     * whose emitted function cannot type its field (a {@code range} over a boolean column, an order comparison over an
     * unorderable type) would otherwise sail through and fail in the compute engine. Rejecting it degrades the filter
     * per-source instead — and keeps this hand-written translator immune to any future drift between the types it
     * accepts and the types the emitted functions resolve. A MISSING field is deliberately null-bound and every leaf
     * folds it to {@code false}, so it is left untouched: that is leniency, not a type error.
     */
    private static Expression checkedLeaf(Expression field, Expression leaf) {
        DataType type = field.dataType();
        if (DataType.isNull(type) == false) {
            // An analyzed text field matches on ANALYZED TOKENS in the index (term "quick" hits "the quick fox"); a
            // structural equality/range leaf here compares the whole raw string and would silently under-match. We
            // cannot reproduce the analyzer, so degrade rather than answer a narrower question. (exists is analysis-
            // independent and does not pass through here, so IS NOT NULL over a text field stays valid.)
            if (type == DataType.TEXT) {
                throw new TranslationUnsupportedException(leaf.nodeName() + "[on analyzed " + type.typeName() + "]");
            }
            if (leaf.resolved() == false) {
                throw new TranslationUnsupportedException(leaf.nodeName() + "[on " + type.typeName() + "]");
            }
        }
        return leaf;
    }

    /** A field the source actually has, as opposed to the {@link Literal#NULL} a missing field binds to. */
    private static boolean isPresent(Expression field) {
        return DataType.isNull(field.dataType()) == false;
    }

    /**
     * Whether {@code value} can equal no value of an integral field type. The index path's numeric term/terms query
     * returns match-no-docs for a decimal or an out-of-range number rather than truncating or wrapping it; we reproduce
     * that by treating such a value as unmatchable. Non-number values fall through to the ordinary coercion path.
     */
    private static boolean cannotEqualIntegral(DataType type, Object value) {
        if (value instanceof Number n && (type == DataType.INTEGER || type == DataType.LONG)) {
            double d = n.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d) || d != Math.floor(d)) {
                return true; // a fractional (or non-finite) value equals no integer
            }
            return type == DataType.INTEGER ? (d < Integer.MIN_VALUE || d > Integer.MAX_VALUE) : (d < Long.MIN_VALUE || d > Long.MAX_VALUE);
        }
        return false;
    }

    /** Whole-number types have an exact predecessor/successor, so an exclusive bound can be rewritten as inclusive. */
    private static boolean isWholeNumbered(DataType type) {
        return type == DataType.INTEGER || type == DataType.LONG || type == DataType.DATETIME || type == DataType.DATE_NANOS;
    }

    private static Object increment(Object value, DataType type, long delta) {
        if (type == DataType.INTEGER) {
            return Math.toIntExact(Math.addExact(((Number) value).longValue(), delta));
        }
        return Math.addExact(((Number) value).longValue(), delta);
    }

    private Literal literalFor(Expression field, Object value) {
        return new Literal(Source.EMPTY, coerce(field, value), literalType(field, value));
    }

    private Literal listLiteralFor(Expression field, List<Object> values) {
        List<Object> coerced = new ArrayList<>(values.size());
        for (Object v : values) {
            coerced.add(coerce(field, v));
        }
        return new Literal(Source.EMPTY, coerced, literalType(field, values.get(0)));
    }

    /**
     * A literal compared against a field takes the FIELD's type — the graft runs after the analyzer, so no implicit
     * cast will fix a mismatch later; the evaluator would be handed the wrong block type. A missing field carries no
     * type, and every leaf folds it to false anyway, so fall back to the JSON value's own type there.
     */
    private static DataType literalType(Expression field, Object sample) {
        DataType type = field.dataType();
        return DataType.isNull(type) ? DataType.fromJava(sample) : type;
    }

    /**
     * Convert a JSON value from the DSL into the internal representation of the field's type. Date types never reach
     * here — {@code term}/{@code terms}/{@code range} on a date route through {@link #dateBound}, which owns the date
     * math and rounding — so a date field falls through to the {@code default} and would degrade, defensively.
     */
    private static Object coerce(Expression field, Object value) {
        DataType type = field.dataType();
        // Missing field: the leaf folds to false regardless of the literal, so keep the value as-is.
        if (DataType.isNull(type)) {
            return toInternalRepresentation(value);
        }
        try {
            return switch (type) {
                case KEYWORD, TEXT -> new BytesRef(String.valueOf(value));
                case BOOLEAN -> value instanceof Boolean b ? b : Booleans.parseBoolean(String.valueOf(value));
                case INTEGER -> value instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(value));
                case LONG -> value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
                case DOUBLE -> value instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(value));
                // ip, version, unsigned_long, dates and friends have encodings we do not reproduce here; rejecting keeps
                // us from handing the evaluator a value it cannot read.
                default -> throw new TranslationUnsupportedException("literal on " + type.typeName());
            };
        } catch (IllegalArgumentException e) {
            // An unparseable bound (a non-numeric string) cannot be translated faithfully.
            throw new TranslationUnsupportedException("literal on " + type.typeName());
        }
    }

    /** ES|QL literals hold {@code keyword}/{@code text} values as {@link BytesRef}, not {@link String}. Convert. */
    private static Object toInternalRepresentation(Object value) {
        if (value instanceof String s) {
            return new BytesRef(s);
        }
        if (value instanceof List<?> list) {
            return list.stream().map(QueryDslTranslator::toInternalRepresentation).toList();
        }
        return value;
    }
}
