/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

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
 * {@code exists}, {@code match_all}/{@code match_none}, and {@code match}/{@code match_phrase}/{@code multi_match} as
 * equality on an exact-typed field. We never mis-translate anything outside it — an unhonored option, or an analyzed
 * {@code text}-field construct — and the caller picks what happens instead. A fail-closed translator raises {@link
 * TranslationUnsupportedException} (a query function errors on it). A {@code partial} translator instead absorbs the
 * unsupported subtree with a polarity-aware superset substitution (TRUE in a conjunctive position, FALSE under
 * {@code must_not}) and records it in {@link #droppedClauses()}, so every supported clause still applies and the result
 * over-matches rather than silently dropping rows — the request filter's contract.
 */
public final class QueryDslTranslator {

    private final Function<String, Expression> fieldBinder;
    private final Set<String> fieldNames;
    private final long nowInMillis;
    private final boolean partial;
    private final List<DroppedClause> dropped = new ArrayList<>();

    /**
     * A clause the translator could not honor and dropped in partial mode, with the polarity of its position: {@code
     * positive} means it was a restriction (a conjunct) that was relaxed — more rows may be returned; {@code
     * positive=false} means it was under {@code must_not}, an exclusion that was relaxed — previously-excluded rows may
     * be returned. Either direction is a superset of the true filter, never a silent drop of matching rows.
     */
    public record DroppedClause(String construct, boolean positive) {}

    /** A fail-closed translator: an unsupported construct raises {@link TranslationUnsupportedException}. */
    public QueryDslTranslator(Function<String, Expression> fieldBinder, Set<String> fieldNames, long nowInMillis) {
        this(fieldBinder, fieldNames, nowInMillis, false);
    }

    /**
     * @param fieldBinder resolves a DSL field name to the ES|QL expression standing for it on this source — the
     *                    source's attribute when the field exists, {@link Literal#NULL} when it does not.
     * @param fieldNames  every field the source has, used to expand a {@code multi_match}'s field patterns (a bare
     *                    function binder cannot be enumerated). Leaves still bind through {@code fieldBinder}.
     * @param nowInMillis the query's start time, epoch millis — the anchor for {@code now} date math on date bounds, so
     *                    that {@code "now-15m"} resolves to the same instant the index path would use for this request.
     * @param partial     when true, an unsupported subtree is not fatal: it is replaced by a polarity-appropriate
     *                    constant (a superset substitution) and recorded in {@link #droppedClauses()}, so the rest of
     *                    the filter still applies. When false, it raises {@link TranslationUnsupportedException}.
     */
    public QueryDslTranslator(Function<String, Expression> fieldBinder, Set<String> fieldNames, long nowInMillis, boolean partial) {
        this.fieldBinder = fieldBinder;
        this.fieldNames = fieldNames;
        this.nowInMillis = nowInMillis;
        this.partial = partial;
    }

    /** The clauses dropped during a partial translation, in the order encountered. Empty after a full translation. */
    public List<DroppedClause> droppedClauses() {
        return dropped;
    }

    /** Translate a DSL query into an ES|QL boolean predicate. The top level sits in positive (conjunctive) polarity. */
    public Expression translate(QueryBuilder query) {
        return translate(query, true);
    }

    /**
     * Dispatch a query, and in partial mode absorb an unsupported subtree with a <em>superset substitution</em>: TRUE in
     * a positive (conjunctive) position removes a restriction, FALSE in a negative ({@code must_not}) position lifts an
     * exclusion — either way the result is a superset of the true filter, so it over-matches and never silently drops a
     * matching row. We only catch at this dispatch boundary, never inside a leaf, so a leaf's own exact-false paths (a
     * malformed value, an empty terms set) keep their exact meaning instead of being widened.
     */
    private Expression translate(QueryBuilder query, boolean positive) {
        if (partial == false) {
            return dispatch(query, positive);
        }
        try {
            return dispatch(query, positive);
        } catch (TranslationUnsupportedException e) {
            dropped.add(new DroppedClause(e.construct(), positive));
            return positive ? Literal.TRUE : Literal.FALSE;
        }
    }

    private Expression dispatch(QueryBuilder query, boolean positive) {
        if (query instanceof BoolQueryBuilder bool) {
            return bool(bool, positive);
        }
        if (query instanceof TermQueryBuilder term) {
            return term(term);
        }
        if (query instanceof TermsQueryBuilder terms) {
            return terms(terms);
        }
        if (query instanceof MatchQueryBuilder match) {
            return match(match);
        }
        if (query instanceof MatchPhraseQueryBuilder phrase) {
            return matchPhrase(phrase);
        }
        if (query instanceof MultiMatchQueryBuilder multiMatch) {
            return multiMatch(multiMatch);
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
        return equality(fieldBinder.apply(term.fieldName()), term.value(), false);
    }

    /**
     * The any-value exact-equality leaf shared by {@code term} and {@code match}. Three outcomes, faithful to the index:
     * a value that <em>equals</em> some value of the field's type → {@code mv_contains}; a well-formed value that no
     * value of the type can equal (a decimal or out-of-range number on an integral field) → {@code false}, matching the
     * index's match-no-docs; a value the field's type cannot even represent → degrade (the term precedent), unless the
     * caller is {@code lenient} <em>and</em> the type is one we fully encode, in which case a malformed value matches
     * nothing (the index's {@code newLenientFieldQuery}). Analyzed {@code text} and types we cannot encode at all (ip,
     * version, unsigned_long) always degrade — lenient's "skip a bad value" is not a licence to drop a whole capability.
     */
    private Expression equality(Expression field, Object value, boolean lenient) {
        DataType type = field.dataType();
        if (isDate(type)) {
            return lenient && isPresent(field)
                ? foldMalformedToFalse(() -> checkedLeaf(field, dateTermRange(field, type, value, null)))
                : checkedLeaf(field, dateTermRange(field, type, value, null));
        }
        if (type == DataType.INTEGER || type == DataType.LONG) {
            return integralEquality(field, type, value, lenient);
        }
        // keyword never fails to coerce; double/boolean fail only on a malformed value; the types coerce's default
        // rejects (ip, version, unsigned_long) and analyzed text are capability gaps, so they never lenient-fold.
        boolean encodable = type == DataType.KEYWORD || type == DataType.DOUBLE || type == DataType.BOOLEAN;
        if (lenient && encodable && isPresent(field)) {
            return foldMalformedToFalse(() -> checkedLeaf(field, new MvContains(Source.EMPTY, field, literalFor(field, value))));
        }
        return checkedLeaf(field, new MvContains(Source.EMPTY, field, literalFor(field, value)));
    }

    /** A lenient match over a value an encodable type cannot represent matches nothing, mirroring the index. */
    private static Expression foldMalformedToFalse(Supplier<Expression> build) {
        try {
            return build.get();
        } catch (TranslationUnsupportedException e) {
            return Literal.FALSE;
        }
    }

    /**
     * Equality against an integral field, computed on {@link BigDecimal} so no precision is lost. A whole in-range value
     * (including a string like {@code "300.0"}, which the index coerces to {@code 300}) is that integer; a well-formed
     * value with a fractional part or out of range equals no value of the type ({@code false}); a non-numeric value is
     * malformed — a lenient match matches nothing, a strict one degrades (the term precedent, matching the index's 400).
     */
    private Expression integralEquality(Expression field, DataType type, Object value, boolean lenient) {
        BigDecimal number;
        try {
            // Trim a string value: the index parses integral bounds via Double.parseDouble, which ignores surrounding
            // whitespace, so " 300" must resolve like "300" rather than degrading.
            number = value instanceof Number n ? new BigDecimal(n.toString()) : new BigDecimal(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            if (lenient && isPresent(field)) {
                return Literal.FALSE;
            }
            throw new TranslationUnsupportedException("match[integral value on " + type.typeName() + "]");
        }
        BigDecimal min = type == DataType.INTEGER ? BigDecimal.valueOf(Integer.MIN_VALUE) : BigDecimal.valueOf(Long.MIN_VALUE);
        BigDecimal max = type == DataType.INTEGER ? BigDecimal.valueOf(Integer.MAX_VALUE) : BigDecimal.valueOf(Long.MAX_VALUE);
        if (number.stripTrailingZeros().scale() > 0 || number.compareTo(min) < 0 || number.compareTo(max) > 0) {
            return Literal.FALSE;
        }
        // Box each branch to Number separately — a bare int/long ternary would promote the int to long.
        Number literal = type == DataType.INTEGER ? (Number) number.intValueExact() : (Number) number.longValueExact();
        return checkedLeaf(field, new MvContains(Source.EMPTY, field, new Literal(Source.EMPTY, literal, type)));
    }

    /**
     * A {@code match} on an exact-typed field IS a term — the index builds a term query there. Options that change what
     * matches (analyzer, fuzziness, minimum_should_match) are rejected; everything else is delegated to {@link
     * #equality}, which handles the lenient / malformed / unmatchable distinctions.
     */
    private Expression match(MatchQueryBuilder match) {
        if (match.analyzer() != null || match.fuzziness() != null || match.minimumShouldMatch() != null) {
            throw new TranslationUnsupportedException("match[unsupported option]");
        }
        return equality(fieldBinder.apply(match.fieldName()), match.value(), match.lenient());
    }

    /**
     * A {@code match_phrase} on an exact-typed field is the whole value — plain equality; a positional slop or an
     * analyzed text field cannot be honored. It has no lenient option.
     */
    private Expression matchPhrase(MatchPhraseQueryBuilder phrase) {
        if (phrase.analyzer() != null || phrase.slop() != 0) {
            throw new TranslationUnsupportedException("match_phrase[unsupported option]");
        }
        return equality(fieldBinder.apply(phrase.fieldName()), phrase.value(), false);
    }

    /**
     * A {@code multi_match} over exact-typed fields is an OR of per-field equality. Only {@code best_fields} and
     * {@code phrase} reduce this way; the other types fuse tokens/scores across fields. The field patterns are expanded
     * against the source schema (boost weights ignored — a boost never changes which rows a filter selects); a pattern
     * that matches no field, or an empty field list resolving to nothing, matches nothing. A text field among the
     * resolved set degrades the whole clause rather than under-matching by dropping it.
     */
    private Expression multiMatch(MultiMatchQueryBuilder multiMatch) {
        MultiMatchQueryBuilder.Type type = multiMatch.getType();
        if (type != MultiMatchQueryBuilder.Type.BEST_FIELDS && type != MultiMatchQueryBuilder.Type.PHRASE) {
            throw new TranslationUnsupportedException("multi_match[type=" + type + "]");
        }
        if (multiMatch.fuzziness() != null || multiMatch.minimumShouldMatch() != null || multiMatch.analyzer() != null) {
            throw new TranslationUnsupportedException("multi_match[unsupported option]");
        }
        Map<String, Float> fields = multiMatch.fields();
        Collection<String> patterns = fields.isEmpty() ? fieldNames : fields.keySet();
        List<String> resolved = new ArrayList<>();
        for (String name : fieldNames) {
            for (String pattern : patterns) {
                if (Regex.simpleMatch(pattern, name)) {
                    resolved.add(name);
                    break;
                }
            }
        }
        if (resolved.isEmpty()) {
            return Literal.FALSE;
        }
        // An all-fields multi_match is implicitly lenient on the index (MultiMatchQueryBuilder.doToQuery sets it), so a
        // value that cannot be a value of a given field's type just drops that field from the OR rather than failing
        // the whole clause — while a text field (or a type we cannot encode) still degrades it, since dropping that is
        // an under-match, not a skipped bad value.
        boolean lenient = fields.isEmpty() || multiMatch.lenient();
        Object value = multiMatch.value();
        Expression or = null;
        for (String name : resolved) {
            Expression leaf = equality(fieldBinder.apply(name), value, lenient);
            or = or == null ? leaf : new Or(Source.EMPTY, or, leaf);
        }
        return or;
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

    private Expression bool(BoolQueryBuilder bool, boolean positive) {
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

        // adjust_pure_negative=false makes a bool with ONLY must_not clauses match NOTHING (Lucene omits the implicit
        // match-all that the default true adds). We model the default; a pure-negative bool with the flag off would be a
        // silent over-match (we would emit NOT-of-the-excluded), so reject it. The flag has no effect in any other shape.
        if (bool.adjustPureNegative() == false
            && bool.mustNot().isEmpty() == false
            && bool.must().isEmpty()
            && bool.filter().isEmpty()
            && bool.should().isEmpty()) {
            throw new TranslationUnsupportedException("bool[adjust_pure_negative=false]");
        }

        // Polarity threads down for the superset substitution in partial mode: must/filter/should keep the parent's
        // polarity; a must_not child sits under a NOT, so its polarity flips.
        List<Expression> conjuncts = new ArrayList<>();
        for (QueryBuilder q : bool.must()) {
            conjuncts.add(translate(q, positive));
        }
        for (QueryBuilder q : bool.filter()) {
            conjuncts.add(translate(q, positive));
        }
        for (QueryBuilder q : bool.mustNot()) {
            conjuncts.add(new Not(Source.EMPTY, translate(q, positive == false)));
        }

        if (bool.should().isEmpty() == false) {
            Expression or = null;
            for (QueryBuilder q : bool.should()) {
                Expression e = translate(q, positive);
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

        // INTEGER/LONG bounds round INWARD like NumberFieldMapper — a fractional lower rounds up, a fractional upper
        // down, an exclusive whole bound nudges one — never truncate toward zero, which would silently over-match a
        // fractional bound (`gte 300.5` must not include 300). A MISSING field has NULL type, so it stays on the generic
        // path below and folds to false (leniency); only a present integral field takes this route.
        if (type == DataType.INTEGER || type == DataType.LONG) {
            return integralRange(field, type, range, hasLower, hasUpper);
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
     * A range over a present INTEGER/LONG field, resolved to one closed inclusive {@code [lo, hi]} the way {@code
     * NumberFieldMapper} does, then emitted as a single any-value {@link MvInRange} (exact on multivalue, pushdown
     * friendly, and two-valued so a null row folds to false). Each present bound is rounded inward by
     * {@link #effectiveIntegralBound}; a missing bound is the type's extreme. Bounds are clamped to the representable
     * range (an out-of-range bound on the far side just removes that side's restriction); an empty interval is {@code
     * FALSE} — the index's match-no-docs.
     */
    private Expression integralRange(Expression field, DataType type, RangeQueryBuilder range, boolean hasLower, boolean hasUpper) {
        BigDecimal min = type == DataType.INTEGER ? BigDecimal.valueOf(Integer.MIN_VALUE) : BigDecimal.valueOf(Long.MIN_VALUE);
        BigDecimal max = type == DataType.INTEGER ? BigDecimal.valueOf(Integer.MAX_VALUE) : BigDecimal.valueOf(Long.MAX_VALUE);
        BigDecimal lo = hasLower ? effectiveIntegralBound(type, range.from(), true, range.includeLower()) : min;
        BigDecimal hi = hasUpper ? effectiveIntegralBound(type, range.to(), false, range.includeUpper()) : max;
        lo = lo.max(min);
        hi = hi.min(max);
        if (lo.compareTo(hi) > 0) {
            return Literal.FALSE; // the interval is empty — no value of the type lies inside it
        }
        Number loValue = type == DataType.INTEGER ? (Number) lo.intValueExact() : (Number) lo.longValueExact();
        Number hiValue = type == DataType.INTEGER ? (Number) hi.intValueExact() : (Number) hi.longValueExact();
        return checkedLeaf(
            field,
            new MvInRange(Source.EMPTY, field, new Literal(Source.EMPTY, loValue, type), new Literal(Source.EMPTY, hiValue, type))
        );
    }

    /**
     * The effective INCLUSIVE integer bound for a range, mirroring {@code NumberFieldMapper}: the value is truncated
     * toward zero, then nudged one inward when it is an exclusive whole number, or a fractional value that rounds into
     * the interval (a positive-decimal lower / a negative-decimal upper). Computed on {@link BigDecimal} so a large or
     * fractional value loses no precision; an unparseable value degrades the clause.
     */
    private static BigDecimal effectiveIntegralBound(DataType type, Object value, boolean lower, boolean inclusive) {
        BigDecimal number;
        try {
            number = value instanceof Number n ? new BigDecimal(n.toString()) : new BigDecimal(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new TranslationUnsupportedException("range[bound on " + type.typeName() + "]");
        }
        boolean hasDecimal = number.stripTrailingZeros().scale() > 0;
        BigDecimal base = number.setScale(0, RoundingMode.DOWN); // truncate toward zero, as the index parse does
        if (lower && ((hasDecimal && number.signum() > 0) || (hasDecimal == false && inclusive == false))) {
            return base.add(BigDecimal.ONE);
        }
        if (lower == false && ((hasDecimal && number.signum() < 0) || (hasDecimal == false && inclusive == false))) {
            return base.subtract(BigDecimal.ONE);
        }
        return base;
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
                // The numeric bound is epoch-MILLIS (matching the index's epoch_millis parse). resolution.convert lands
                // on the FIRST nanosecond of that milli; when rounding up, a date_nanos bound must reach the LAST nano
                // (the index's epoch_millis round-up parser defaults NANOS_OF_MILLI to 999_999) or an upper bound would
                // under-match every sub-milli row. For DATETIME the milli is exact, so roundUp is a no-op there.
                long point = resolution.convert(Instant.ofEpochMilli(n.longValue()));
                return (type == DataType.DATE_NANOS && roundUp) ? point + 999_999L : point;
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
