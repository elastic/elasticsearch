/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvRLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class QueryDslTranslatorTests extends ESTestCase {

    // These fields exist; everything else is missing and binds to NULL.
    private static final Function<String, Expression> BINDER = name -> switch (name) {
        case "status" -> new ReferenceAttribute(Source.EMPTY, "status", DataType.INTEGER);
        case "tags" -> new ReferenceAttribute(Source.EMPTY, "tags", DataType.KEYWORD);
        case "bytes" -> new ReferenceAttribute(Source.EMPTY, "bytes", DataType.LONG);
        case "score" -> new ReferenceAttribute(Source.EMPTY, "score", DataType.DOUBLE);
        case "@timestamp" -> new ReferenceAttribute(Source.EMPTY, "@timestamp", DataType.DATETIME);
        case "ts_nanos" -> new ReferenceAttribute(Source.EMPTY, "ts_nanos", DataType.DATE_NANOS);
        case "active" -> new ReferenceAttribute(Source.EMPTY, "active", DataType.BOOLEAN);
        case "body" -> new ReferenceAttribute(Source.EMPTY, "body", DataType.TEXT);
        case "client_ip" -> new ReferenceAttribute(Source.EMPTY, "client_ip", DataType.IP);
        case "quota" -> new ReferenceAttribute(Source.EMPTY, "quota", DataType.UNSIGNED_LONG);
        case "release" -> new ReferenceAttribute(Source.EMPTY, "release", DataType.VERSION);
        case "location" -> new ReferenceAttribute(Source.EMPTY, "location", DataType.GEO_POINT);
        default -> Literal.NULL;
    };

    // A fixed query "now" so date-math bounds ("now-1d") resolve deterministically in tests.
    private static final long NOW = Instant.parse("2020-06-15T12:00:00Z").toEpochMilli();

    // The query configuration carrying that fixed now (and the locale used to case-fold a case_insensitive term).
    private static final Configuration CONFIG = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).now(Instant.ofEpochMilli(NOW)).build();

    // The schema field set (for multi_match expansion) — the names the BINDER resolves to a present attribute.
    // Every such name belongs here: a fieldless multi_match expands over the whole set, and that is the shape that
    // reaches a type's term path without naming it.
    private static final Set<String> FIELDS = Set.of(
        "status",
        "tags",
        "bytes",
        "score",
        "@timestamp",
        "ts_nanos",
        "active",
        "body",
        "client_ip",
        "quota",
        "release",
        "location"
    );

    private static Expression translate(QueryBuilder qb) {
        return new QueryDslTranslator(BINDER, FIELDS, CONFIG).translate(qb).applied();
    }

    private static Expression translate(QueryBuilder qb, Locale locale) {
        return new QueryDslTranslator(BINDER, FIELDS, new ConfigurationBuilder(CONFIG).locale(locale).build()).translate(qb).applied();
    }

    private static QueryDslTranslator.TranslationResult translateResult(QueryBuilder qb) {
        return new QueryDslTranslator(BINDER, FIELDS, CONFIG).translate(qb);
    }

    private static QueryDslTranslator.TranslationResult translateResult(QueryBuilder qb, Locale locale) {
        return new QueryDslTranslator(BINDER, FIELDS, new ConfigurationBuilder(CONFIG).locale(locale).build()).translate(qb);
    }

    /** An unsupported top-level construct is collected, not thrown; applied() is TRUE (no conjuncts applied). */
    public void testUnsupportedConstructCollected() {
        QueryDslTranslator.TranslationResult result = translateResult(QueryBuilders.fuzzyQuery("tags", "xyz"));
        assertFalse("a wholly unsupported filter is incomplete", result.isComplete());
        assertEquals(1, result.unsupported().size());
        assertEquals("fuzzy", result.unsupported().get(0).construct());
        assertEquals(Literal.TRUE, result.applied());
    }

    /** A bool with a supported must and an unsupported must_not: the supported conjunct is applied, the unsupported one is collected. */
    public void testPartialBoolCollectsUnsupportedAndAppliesRest() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).mustNot(QueryBuilders.fuzzyQuery("tags", "xyz"))
        );
        assertFalse("incomplete: mustNot arm is unsupported", result.isComplete());
        assertEquals(1, result.unsupported().size());
        assertEquals("fuzzy", result.unsupported().get(0).construct());
        // The supported term conjunct must still be present in applied.
        assertThat(result.applied(), instanceOf(MvContains.class));
    }

    /** A bool with two unsupported must clauses: both are collected. */
    public void testMultipleUnsupportedClausesAllCollected() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.wildcardQuery("tags", "a*").caseInsensitive(true))
                .must(QueryBuilders.fuzzyQuery("tags", "xyz"))
        );
        assertFalse(result.isComplete());
        assertThat(result.unsupported(), hasSize(2));
    }

    public void testTermBecomesAnyValueContains() {
        Expression e = translate(QueryBuilders.termQuery("status", 200));
        assertThat(e, instanceOf(MvContains.class));
        MvContains c = (MvContains) e;
        assertThat(c.children().get(0), instanceOf(ReferenceAttribute.class));
        assertThat(c.children().get(1), instanceOf(Literal.class));
    }

    public void testTermsBecomesIntersects() {
        Expression e = translate(QueryBuilders.termsQuery("status", java.util.List.of(200, 404)));
        assertThat(e, instanceOf(MvIntersects.class));
    }

    public void testExistsBecomesIsNotNull() {
        Expression e = translate(QueryBuilders.existsQuery("status"));
        assertThat(e, instanceOf(IsNotNull.class));
    }

    public void testMatchAllAndNoneBecomeLiterals() {
        assertEquals(Literal.TRUE, translate(new MatchAllQueryBuilder()));
        assertEquals(Literal.FALSE, translate(new MatchNoneQueryBuilder()));
    }

    public void testBoolMustBecomesAnd() {
        Expression e = translate(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 1)).must(QueryBuilders.existsQuery("status"))
        );
        assertThat(e, instanceOf(And.class));
    }

    /**
     * The design's sharp edge (rule 4): a negated clause over a field the source does not have must match everything.
     * It translates to {@code Not(mv_contains(NULL, value))}; {@code mv_contains(NULL, value)} is {@code false} and
     * {@code mv_contains} never returns null, so {@code Not(false)} is {@code true} — match-all — with no special code.
     */
    public void testNegatedMissingFieldStructureIsNotOverNullContains() {
        Expression e = translate(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("missing_field", "x")));
        assertThat(e, instanceOf(Not.class));
        Not not = (Not) e;
        assertThat(not.children().get(0), instanceOf(MvContains.class));
        MvContains contains = (MvContains) not.children().get(0);
        assertEquals(Literal.NULL, contains.children().get(0));
    }

    public void testUnsupportedConstructCollectedWithConstructName() {
        var result = translateResult(QueryBuilders.fuzzyQuery("status", "2"));
        assertFalse(result.isComplete());
        assertEquals("fuzzy", result.unsupported().get(0).construct());
    }

    public void testRangeTranslation() {
        // A one-sided range on a NON-integral field is mv_greater / mv_less — two-valued any-value predicates that keep
        // a missing field false so must_not-over-missing matches all, and that push as bare one-sided ranges. Integral
        // one-sided ranges take the mv_in_range path (tested below).
        Expression lower = translate(QueryBuilders.rangeQuery("score").gte(1));
        assertThat(lower, instanceOf(MvGreater.class));
        assertNotNull(((MvGreater) lower).options()); // inclusive → include_bound: true
        // single upper bound -> inclusive mv_less
        Expression upper = translate(QueryBuilders.rangeQuery("score").lte(10));
        assertThat(upper, instanceOf(MvLess.class));
        assertNotNull(((MvLess) upper).options());
        // exclusive single bound -> bare (strict) mv_greater
        Expression exclusive = translate(QueryBuilders.rangeQuery("score").gt(1));
        assertThat(exclusive, instanceOf(MvGreater.class));
        assertNull(((MvGreater) exclusive).options());
        // closed range -> the two-valued any-value range intrinsic
        assertThat(translate(QueryBuilders.rangeQuery("score").gte(1).lte(10)), instanceOf(MvInRange.class));
    }

    /**
     * An integral range folds every bound into ONE closed inclusive {@code mv_in_range} — including a one-sided bound,
     * whose open end is the type's extreme. The exclusive/inclusive distinction is baked into the bound, not the tree.
     */
    public void testIntegralOneSidedRangeIsMvInRangeOverTypeExtreme() {
        Expression lower = translate(QueryBuilders.rangeQuery("status").gt(200)); // int field
        assertThat(lower, instanceOf(MvInRange.class));
        assertEquals(201, ((Literal) ((MvInRange) lower).lower()).value()); // gt 200 -> [201, MAX]
        assertEquals(Integer.MAX_VALUE, ((Literal) ((MvInRange) lower).upper()).value());
        Expression upper = translate(QueryBuilders.rangeQuery("status").lte(200));
        assertThat(upper, instanceOf(MvInRange.class));
        assertEquals(Integer.MIN_VALUE, ((Literal) ((MvInRange) upper).lower()).value());
        assertEquals(200, ((Literal) ((MvInRange) upper).upper()).value());
    }

    /**
     * A fractional bound on an integral field rounds INWARD like the index (NumberFieldMapper), never truncates toward
     * zero — {@code gte 300.5} is {@code >= 301}, {@code lte 300.5} is {@code <= 300}, and the sign matters
     * ({@code lte -1.5} is {@code <= -2}, {@code gte -1.5} is {@code >= -1}). Regression pin for the silent over-match.
     */
    public void testFractionalIntegralRangeBoundRoundsInward() {
        assertEquals(301, ((Literal) ((MvInRange) translate(QueryBuilders.rangeQuery("status").gte(300.5))).lower()).value());
        assertEquals(300, ((Literal) ((MvInRange) translate(QueryBuilders.rangeQuery("status").lte(300.5))).upper()).value());
        assertEquals(-2, ((Literal) ((MvInRange) translate(QueryBuilders.rangeQuery("status").lte(-1.5))).upper()).value());
        assertEquals(-1, ((Literal) ((MvInRange) translate(QueryBuilders.rangeQuery("status").gte(-1.5))).lower()).value());
        // a two-bound fractional interval rounds both ends inward
        MvInRange both = (MvInRange) translate(QueryBuilders.rangeQuery("status").gte(300.5).lte(400.5));
        assertEquals(301, ((Literal) both.lower()).value());
        assertEquals(400, ((Literal) both.upper()).value());
    }

    /** A lower bound above the type's max (or an inverted interval) selects nothing — the index's match-no-docs. */
    public void testIntegralRangeBeyondTypeRangeMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.rangeQuery("status").gte(3_000_000_000L))); // > Integer.MAX
        assertEquals(Literal.FALSE, translate(QueryBuilders.rangeQuery("status").gte(500).lte(400))); // inverted
    }

    public void testShouldOnlyBoolBecomesOr() {
        // no must/filter present -> the should clauses are the required predicate (minimum_should_match defaults to 1)
        Expression e = translate(
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("status", 1)).should(QueryBuilders.termQuery("status", 2))
        );
        assertThat(e, instanceOf(Or.class));
    }

    public void testMustWithShouldDropsShould() {
        // should is scoring-only alongside a must/filter in a filter context -> dropped, leaving just the must
        Expression e = translate(
            QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("status")).should(QueryBuilders.termQuery("status", 1))
        );
        assertThat(e, instanceOf(IsNotNull.class));
    }

    /**
     * A two-bound range must be ONE any-value test. Splitting it into mv_max &gt;= lo AND mv_min &lt;= hi is an envelope
     * test that wrongly matches a multivalue field straddling the interval ([0,100] would satisfy (40,60)). Exclusive
     * bounds on a whole-number type are therefore normalized to the equivalent inclusive bounds and routed to
     * mv_in_range, which is exact.
     */
    public void testExclusiveTwoBoundRangeNormalizesToInclusiveMvInRange() {
        Expression e = translate(QueryBuilders.rangeQuery("status").gt(40).lt(60));
        assertThat(e, instanceOf(MvInRange.class));
        MvInRange r = (MvInRange) e;
        // (40, 60) exclusive == [41, 59] inclusive on a whole-number type
        assertEquals(41, ((Literal) r.lower()).value());
        assertEquals(59, ((Literal) r.upper()).value());

        // a mixed bound pair (the standard Kibana time range shape) normalizes the exclusive end only
        Expression mixed = translate(QueryBuilders.rangeQuery("status").gte(40).lt(60));
        assertThat(mixed, instanceOf(MvInRange.class));
        assertEquals(40, ((Literal) ((MvInRange) mixed).lower()).value());
        assertEquals(59, ((Literal) ((MvInRange) mixed).upper()).value());
    }

    /**
     * The rewrite runs after the analyzer, so no implicit cast fixes a literal typed from the JSON value. A date string
     * against a date column must become a datetime literal here, or the evaluator is handed a BytesRef block.
     */
    public void testDateStringBoundsAreCoercedToTheFieldType() {
        Expression e = translate(QueryBuilders.rangeQuery("@timestamp").gte("2024-01-01T00:00:00Z").lte("2024-12-31T00:00:00Z"));
        assertThat(e, instanceOf(MvInRange.class));
        MvInRange r = (MvInRange) e;
        assertEquals(DataType.DATETIME, r.lower().dataType());
        assertEquals(DataType.DATETIME, r.upper().dataType());
        assertThat(((Literal) r.lower()).value(), instanceOf(Long.class));
    }

    /** An int literal against a long column must become a long literal, or the evaluator element types disagree. */
    public void testIntLiteralIsCoercedToLongField() {
        Expression e = translate(QueryBuilders.termQuery("bytes", 62));
        assertThat(e, instanceOf(MvContains.class));
        Literal value = (Literal) ((MvContains) e).children().get(1);
        assertEquals(DataType.LONG, value.dataType());
        assertEquals(62L, value.value());
    }

    /**
     * {@code terms} must narrow integral values exactly as {@code term} does, on strings too. A string decimal equals
     * no integer, so it is dropped (match-nothing) rather than failing the query; a whole-valued string like "300.0"
     * coerces to 300, as the index does. Previously the string forms diverged from the single-value path.
     */
    public void testTermsNarrowsStringIntegralValuesLikeTerm() {
        // a string decimal matches nothing -> dropped; alone in the list, the clause matches nothing
        assertEquals(Literal.FALSE, translate(QueryBuilders.termsQuery("status", List.of("300.5"))));
        // and it agrees with the single-value path, which already folded to FALSE
        assertEquals(Literal.FALSE, translate(QueryBuilders.termQuery("status", "300.5")));

        // a whole-valued string coerces to the integer, exactly like term
        Expression e = translate(QueryBuilders.termsQuery("status", List.of("300.0", " 400")));
        assertThat(e, instanceOf(MvIntersects.class));
        Literal values = (Literal) ((MvIntersects) e).children().get(1);
        assertEquals(DataType.INTEGER, values.dataType());
        assertEquals(List.of(300, 400), values.value());

        // a mixed list drops only the unmatchable value
        Expression mixed = translate(QueryBuilders.termsQuery("status", List.of("300.5", 404)));
        assertThat(mixed, instanceOf(MvIntersects.class));
        assertEquals(List.of(404), ((Literal) ((MvIntersects) mixed).children().get(1)).value());
    }

    /**
     * A two-bound range on a KEYWORD field takes the generic
     * both-bounds path rather than the integral or date one. A closed interval emits no options map, because inclusive
     * on both ends is what mv_in_range already means.
     */
    public void testTwoBoundRangeOnKeyword() {
        Expression e = translate(QueryBuilders.rangeQuery("tags").gte("t1").lte("t3"));
        assertThat(e, instanceOf(MvInRange.class));
        MvInRange r = (MvInRange) e;
        assertEquals(DataType.KEYWORD, ((Literal) r.lower()).dataType());
        assertEquals(new BytesRef("t1"), ((Literal) r.lower()).value());
        assertEquals(new BytesRef("t3"), ((Literal) r.upper()).value());
        assertEquals(3, r.children().size()); // no options map: the closed interval is the default
    }

    /**
     * A type with no exact predecessor or successor — double, keyword, ip, version — still takes an exclusive bound:
     * mv_in_range carries the inclusivity in its options rather than needing the bound shifted onto its neighbour.
     * These used to be collected as untranslatable, which dropped a clause the index path answers exactly.
     */
    public void testExclusiveBoundsOnNonWholeTypesTranslate() {
        Map<String, RangeQueryBuilder> byField = Map.of(
            "score",
            QueryBuilders.rangeQuery("score").gt(1.5).lt(9.5),
            "tags",
            QueryBuilders.rangeQuery("tags").gt("t1").lt("t3"),
            "client_ip",
            QueryBuilders.rangeQuery("client_ip").gt("10.0.0.1").lt("10.0.0.9"),
            "release",
            QueryBuilders.rangeQuery("release").gt("1.0.0").lt("2.0.0")
        );
        for (var entry : byField.entrySet()) {
            Expression e = translate(entry.getValue());
            assertThat(entry.getKey(), e, instanceOf(MvInRange.class));
            assertEquals(entry.getKey(), Map.of("include_lower", false, "include_upper", false), optionsOf((MvInRange) e));
        }
    }

    /**
     * boolean and text were in the set the old whole-number rule rejected, but neither is a coverage gain: mv_in_range
     * has no boolean signature at all ("a range over booleans is not meaningful") and text is analyzed, so both still
     * degrade — now through checkedLeaf rather than the bound rule. Pinned so a later signature change cannot start
     * emitting an unresolved leaf unnoticed.
     */
    public void testRangeOnBooleanAndTextStillDegrades() {
        assertFalse("boolean", translateResult(QueryBuilders.rangeQuery("active").gt(false).lt(true)).isComplete());
        assertFalse("boolean inclusive", translateResult(QueryBuilders.rangeQuery("active").gte(false).lte(true)).isComplete());
        assertFalse("text", translateResult(QueryBuilders.rangeQuery("body").gt("a").lt("z")).isComplete());
    }

    /** One exclusive end spells out only that end; the other keeps mv_in_range's inclusive default. */
    public void testOneExclusiveBoundSpellsOutOnlyThatBound() {
        assertEquals(Map.of("include_lower", false), optionsOf((MvInRange) translate(QueryBuilders.rangeQuery("score").gt(1.5).lte(9.5))));
        assertEquals(Map.of("include_upper", false), optionsOf((MvInRange) translate(QueryBuilders.rangeQuery("score").gte(1.5).lt(9.5))));
    }

    /** The literal keys and values of an emitted mv_in_range options map. */
    private static Map<String, Object> optionsOf(MvInRange range) {
        return ((MapExpression) range.children().get(3)).toFoldedMap(FoldContext.small());
    }

    /** {@code prefix} is the wildcard {@code <literal>*}, which mv_like recognises as its prefix fast-path shape. */
    public void testPrefixBecomesMvLike() {
        Expression e = translate(QueryBuilders.prefixQuery("tags", "t1"));
        assertThat(e, instanceOf(MvLike.class));
        assertEquals(new BytesRef("t1*"), ((Literal) ((MvLike) e).right()).value());
    }

    /** A {@code prefix} value has no metacharacters, so a * or ? inside it must be escaped, not become a wildcard. */
    public void testPrefixEscapesWildcardMetacharactersInTheLiteral() {
        Expression e = translate(QueryBuilders.prefixQuery("tags", "a*b?c"));
        assertEquals(new BytesRef("a\\*b\\?c*"), ((Literal) ((MvLike) e).right()).value());
    }

    /** mv_like speaks the Lucene wildcard dialect, so a pattern in it crosses over verbatim. */
    public void testWildcardBecomesMvLikeVerbatim() {
        Expression e = translate(QueryBuilders.wildcardQuery("tags", "t?x*"));
        assertThat(e, instanceOf(MvLike.class));
        assertEquals(new BytesRef("t?x*"), ((Literal) ((MvLike) e).right()).value());
    }

    /**
     * Lucene reads an escape of a non-metacharacter as that character, and a trailing backslash as a literal one;
     * ES|QL's LIKE rejects both spellings. They translate through the wildcard-to-RegExp conversion instead, so the
     * clause is answered rather than dropped.
     */
    public void testLenientlyEscapedWildcardBecomesMvRLike() {
        Expression e = translate(QueryBuilders.wildcardQuery("tags", "a\\-b*"));
        assertThat(e, instanceOf(MvRLike.class));
        assertEquals(new BytesRef("a-b.*"), ((Literal) ((MvRLike) e).right()).value());

        Expression trailing = translate(QueryBuilders.wildcardQuery("tags", "ab\\"));
        assertThat(trailing, instanceOf(MvRLike.class));
        assertEquals(new BytesRef("ab\\\\"), ((Literal) ((MvRLike) trailing).right()).value());
    }

    /** {@code regexp} is Lucene RegExp syntax, which is what mv_rlike parses — the pattern crosses over verbatim. */
    public void testRegexpBecomesMvRLike() {
        Expression e = translate(QueryBuilders.regexpQuery("tags", "t[0-9]"));
        assertThat(e, instanceOf(MvRLike.class));
        assertEquals(new BytesRef("t[0-9]"), ((Literal) ((MvRLike) e).right()).value());
    }

    /** A narrowed flag set makes some of the syntax literal, which mv_rlike cannot express. */
    public void testRegexpWithNonDefaultFlagsIsCollected() {
        var result = translateResult(QueryBuilders.regexpQuery("tags", "t.").flags(RegexpFlag.EMPTY.value()));
        assertFalse(result.isComplete());
        assertEquals("regexp[flags]", result.unsupported().get(0).construct());
    }

    /** Neither mv_like nor mv_rlike takes a case-insensitivity option, so the clause degrades rather than mis-match. */
    public void testCaseInsensitivePatternsAreCollected() {
        assertEquals("wildcard[case_insensitive]", constructOf(QueryBuilders.wildcardQuery("tags", "a*").caseInsensitive(true)));
        assertEquals("prefix[case_insensitive]", constructOf(QueryBuilders.prefixQuery("tags", "a").caseInsensitive(true)));
        assertEquals("regexp[case_insensitive]", constructOf(QueryBuilders.regexpQuery("tags", "a").caseInsensitive(true)));
    }

    /** A pattern over a non-string field cannot resolve, and over analyzed text it would compare the raw string. */
    public void testPatternOnNonStringAndAnalyzedFieldsIsCollected() {
        assertFalse(translateResult(QueryBuilders.wildcardQuery("status", "2*")).isComplete());
        assertFalse(translateResult(QueryBuilders.regexpQuery("body", "a.")).isComplete());
    }

    /** A malformed regexp would fail the query at post-optimization verification; it must degrade the clause instead. */
    public void testMalformedRegexpIsCollectedNotThrown() {
        var result = translateResult(QueryBuilders.regexpQuery("tags", "["));
        assertFalse(result.isComplete());
        assertEquals("regexp[pattern]", result.unsupported().get(0).construct());
    }

    /**
     * The warning quotes these names back to whoever wrote the filter, and it introduces them as Query DSL
     * constructs — so every one of them must BE a Query DSL construct. A leaf-level failure knows only its reason
     * ("on analyzed text"), and it is the clause that supplies the name; reporting the ES|QL function that happened
     * to be under construction names something the DSL has no word for.
     */
    public void testReportedConstructsAreQueryDslNames() {
        var cases = List.of(
            new Object[] { QueryBuilders.wildcardQuery("body", "a*"), "wildcard[on analyzed text]" },
            new Object[] { QueryBuilders.prefixQuery("body", "a"), "prefix[on analyzed text]" },
            new Object[] { QueryBuilders.regexpQuery("body", "a."), "regexp[on analyzed text]" },
            new Object[] { QueryBuilders.termQuery("body", "a"), "term[on analyzed text]" },
            new Object[] { QueryBuilders.wildcardQuery("status", "2*"), "wildcard[on integer]" },
            new Object[] { QueryBuilders.regexpQuery("release", "1.*"), "regexp[on version]" },
            new Object[] { QueryBuilders.regexpQuery("tags", "["), "regexp[pattern]" },
            new Object[] { QueryBuilders.termQuery("client_ip", "not-an-address"), "term[literal on ip]" },
            new Object[] { QueryBuilders.termsQuery("client_ip", List.of("10.0.0.0/99")), "terms[literal on ip]" }
        );
        for (Object[] c : cases) {
            var result = translateResult((QueryBuilder) c[0]);
            assertFalse("expected " + c[1] + " to degrade", result.isComplete());
            assertEquals(c[1], result.unsupported().get(0).construct());
        }
    }

    /**
     * A rewrite method that only picks how the expanded terms are scored cannot change which rows match, so it
     * crosses unchanged; the top_terms_* family keeps only the N best-scoring terms, so the index matches a subset of
     * the pattern and applying all of it would be an unhonoured option. An unparseable method degrades rather than
     * raising the query-killing failure the index raises.
     */
    public void testPatternRewriteThatChangesMatchingDegrades() {
        for (String rewrite : List.of("top_terms_1", "top_terms_boost_2", "top_terms_blended_freqs_3", "not_a_method")) {
            assertFalse(rewrite, translateResult(QueryBuilders.wildcardQuery("tags", "t*").rewrite(rewrite)).isComplete());
            assertFalse(rewrite, translateResult(QueryBuilders.prefixQuery("tags", "t").rewrite(rewrite)).isComplete());
            assertFalse(rewrite, translateResult(QueryBuilders.regexpQuery("tags", "t.*").rewrite(rewrite)).isComplete());
        }
        // The score-only methods, and no method at all, still translate — so the assertions above are not vacuous.
        for (String rewrite : List.of("constant_score", "scoring_boolean", "constant_score_boolean", "constant_score_blended")) {
            assertThat(rewrite, translate(QueryBuilders.wildcardQuery("tags", "t*").rewrite(rewrite)), instanceOf(MvLike.class));
        }
        assertThat(translate(QueryBuilders.wildcardQuery("tags", "t*")), instanceOf(MvLike.class));
    }

    /**
     * dis_max matches the union of its arms, so an arm may be a bool like any other clause. The strict walk it runs
     * under had no bool arm at all, which dropped the whole filter and reported the unsupported construct as "bool".
     */
    public void testDisMaxOverBoolArms() {
        var disMax = QueryBuilders.disMaxQuery()
            .add(QueryBuilders.termQuery("tags", "a"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("tags", "b")));
        assertTrue("a bool arm translates", translateResult(disMax).isComplete());
        assertThat(translate(disMax), instanceOf(Or.class));
        // Nested one level further, and through a score-only wrapper, on the same strict path.
        assertTrue(
            translateResult(
                QueryBuilders.disMaxQuery()
                    .add(QueryBuilders.termQuery("tags", "a"))
                    .add(QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("tags", "b"))))
            ).isComplete()
        );
        // An arm that cannot translate at all still drops the whole dis_max — the union is all-or-nothing — and it
        // reports the clause that actually failed rather than the bool that contained it.
        var withBadArm = QueryBuilders.disMaxQuery()
            .add(QueryBuilders.termQuery("tags", "a"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.fuzzyQuery("tags", "b")));
        var result = translateResult(withBadArm);
        assertFalse(result.isComplete());
        assertEquals("fuzzy", result.unsupported().get(0).construct());
    }

    /**
     * The two-valued design exists so NOT composes for free, and a pattern is the newest leaf to depend on it: a
     * must_not over one must exclude exactly the rows the pattern matches, and over a MISSING field must exclude
     * nothing rather than everything. A wrapper inside the must_not takes the all-or-nothing path, where a dropped
     * sub-clause would over-exclude, so a failing one drops the whole NOT instead.
     */
    public void testPatternUnderMustNot() {
        var negatedPattern = QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery("tags", "t*"));
        assertTrue(translateResult(negatedPattern).isComplete());
        assertThat(translate(negatedPattern), instanceOf(Not.class));
        // Through a score-only wrapper, still on the all-or-nothing path.
        var negatedWrapped = QueryBuilders.boolQuery().mustNot(QueryBuilders.constantScoreQuery(QueryBuilders.prefixQuery("tags", "t")));
        assertTrue(translateResult(negatedWrapped).isComplete());
        assertThat(translate(negatedWrapped), instanceOf(Not.class));
        // A missing field binds to null and the leaf folds to false, so NOT(false) excludes nothing.
        var negatedMissing = QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery("missing_field", "a*"));
        assertTrue(translateResult(negatedMissing).isComplete());
        // An untranslatable pattern under must_not drops the whole NOT rather than over-excluding.
        var negatedBad = QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery("body", "a*"));
        assertFalse(translateResult(negatedBad).isComplete());
        assertEquals(Literal.TRUE, translate(negatedBad));
    }

    /**
     * boosting only reads its positive clause — the negative one moves the score and never removes a row — so an
     * untranslatable negative must not degrade anything. Nothing pinned that, and the natural reading of "a clause
     * failed" is to drop.
     */
    public void testBoostingIgnoresAnUntranslatableNegative() {
        var boosting = QueryBuilders.boostingQuery(QueryBuilders.termQuery("tags", "a"), QueryBuilders.fuzzyQuery("tags", "b"))
            .negativeBoost(0.1f);
        assertTrue("the negative clause is never visited", translateResult(boosting).isComplete());
        assertThat(translate(boosting), instanceOf(MvContains.class));
    }

    /** An explicitly chosen automaton budget is an option mv_rlike cannot be told, so the clause degrades. */
    public void testRegexpMaxDeterminizedStatesDegrades() {
        assertFalse(translateResult(QueryBuilders.regexpQuery("tags", "t.*").maxDeterminizedStates(100)).isComplete());
        // The default is accepted, so the assertion above is not vacuous.
        assertThat(translate(QueryBuilders.regexpQuery("tags", "t.*")), instanceOf(MvRLike.class));
    }

    /**
     * A JSON number above Long.MAX_VALUE arrives as a BigInteger, a different arm of the unsigned_long term rule from
     * the string form the other tests take. In range it is the value; above the type's maximum it can equal nothing.
     */
    /**
     * A value above Long.MAX_VALUE is the one an unsigned_long exists for, and JSON hands it to the builder as a
     * BigInteger. Which arm reads it depends on the clause: a term does not receive it as a BigInteger, because
     * AbstractQueryBuilder.maybeConvertToBytesRef turns one into a BytesRef at construction and value() converts that
     * back with maybeConvertToString, so the translator is handed the decimal string. A match keeps the value exactly
     * as given (MatchQueryBuilder.value returns the field unconverted), so the same number arrives as a BigInteger.
     * Both must answer the same thing, which is why the assertions are on the emitted expression.
     */
    public void testUnsignedLongTermAboveLongMaxValue() {
        assertThat(translate(QueryBuilders.termQuery("quota", new BigInteger("18446744073709551615"))), instanceOf(MvContains.class));
        assertEquals(Literal.FALSE, translate(QueryBuilders.termQuery("quota", new BigInteger("18446744073709551616"))));
        assertEquals(Literal.FALSE, translate(QueryBuilders.termQuery("quota", new BigInteger("-1"))));
        // The shapes the two builders actually deliver, asserted directly so the claim above is not taken on faith.
        assertEquals("18446744073709551615", QueryBuilders.termQuery("quota", new BigInteger("18446744073709551615")).value());
        assertEquals(
            new BigInteger("18446744073709551615"),
            QueryBuilders.matchQuery("quota", new BigInteger("18446744073709551615")).value()
        );
        // The BigInteger arm itself, reached through match: in range it is the value, out of range it matches nothing.
        assertThat(translate(QueryBuilders.matchQuery("quota", new BigInteger("18446744073709551615"))), instanceOf(MvContains.class));
        assertEquals(Literal.FALSE, translate(QueryBuilders.matchQuery("quota", new BigInteger("18446744073709551616"))));
        assertEquals(Literal.FALSE, translate(QueryBuilders.matchQuery("quota", new BigInteger("-1"))));
    }

    /**
     * unsigned_long takes every integral box, not just the two a test is likely to reach for: parseTerm's number arm
     * accepts Byte and Short exactly as it accepts Integer and Long, and a negative one of any width can equal no
     * unsigned_long.
     */
    public void testUnsignedLongTermFromEveryIntegralBox() {
        for (Object value : List.of((byte) 42, (short) 42, 42, 42L)) {
            assertThat(
                "[" + value.getClass().getSimpleName() + "] is an integral box unsigned_long accepts",
                translate(QueryBuilders.termQuery("quota", value)),
                instanceOf(MvContains.class)
            );
        }
        for (Object value : List.of((byte) -1, (short) -1)) {
            assertEquals("a negative can equal no unsigned_long", Literal.FALSE, translate(QueryBuilders.termQuery("quota", value)));
        }
    }

    /**
     * The empty pattern is the one spelling where the wildcard dialects part company, so the verbatim crossing does
     * not hold there. WildcardQuery.toAutomaton builds an EMPTY-language automaton for it — accepting no term, not
     * even the empty string — while mv_like accepts the empty string, which is exactly why MvLike.patternPushable
     * refuses the pattern. Emitting the leaf would match rows holding "" where the index matches none.
     */
    public void testEmptyWildcardPatternMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.wildcardQuery("tags", "")));
        // prefix cannot reach it: the appended "*" makes the pattern match every term, as it does on the index.
        assertThat(translate(QueryBuilders.prefixQuery("tags", "")), instanceOf(MvLike.class));
        assertEquals(
            "*",
            ((BytesRef) ((Literal) ((MvLike) translate(QueryBuilders.prefixQuery("tags", ""))).right()).value()).utf8ToString()
        );
        // An empty regexp needs no special case: RLikePattern and Lucene both read it as the empty string alone.
        assertThat(translate(QueryBuilders.regexpQuery("tags", "")), instanceOf(MvRLike.class));
    }

    /** A pattern over a MISSING field stays null-bound and folds to false, like every other leaf. */
    public void testPatternOnMissingFieldFoldsToFalse() {
        Expression e = translate(QueryBuilders.wildcardQuery("missing_field", "a*"));
        assertThat(e, instanceOf(MvLike.class));
        assertEquals(Literal.NULL, ((MvLike) e).left());
    }

    private static String constructOf(QueryBuilder qb) {
        var result = translateResult(qb);
        assertFalse(result.isComplete());
        return result.unsupported().get(0).construct();
    }

    /** constant_score and boosting replace the score, not the matching set, so each is its inner/positive query. */
    public void testScoreOnlyWrappersBecomeTheirInnerQuery() {
        Expression constantScore = translate(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("status", 200)));
        assertThat(constantScore, instanceOf(MvContains.class));
        Expression boosting = translate(
            QueryBuilders.boostingQuery(QueryBuilders.termQuery("status", 200), QueryBuilders.termQuery("tags", "t1"))
        );
        assertThat(boosting, instanceOf(MvContains.class));
    }

    /** A wrapper is unwrapped inside the collecting walk, so an inner bool still reports one leaf at a time. */
    public void testWrapperContentsReportPerLeaf() {
        var result = translateResult(
            QueryBuilders.constantScoreQuery(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).must(QueryBuilders.fuzzyQuery("tags", "x"))
            )
        );
        assertFalse(result.isComplete());
        assertEquals(1, result.unsupported().size());
        assertEquals("fuzzy", result.unsupported().get(0).construct());
        assertThat(result.applied(), instanceOf(MvContains.class)); // the term conjunct survived
    }

    /** dis_max is the union of its arms, and all-or-nothing: dropping an arm would exclude rows it alone matched. */
    public void testDisMaxIsAnAllOrNothingUnion() {
        Expression e = translate(
            QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery("status", 200)).add(QueryBuilders.termQuery("tags", "t1"))
        );
        assertThat(e, instanceOf(Or.class));
        assertEquals(Literal.FALSE, translate(QueryBuilders.disMaxQuery()));
        assertFalse(
            translateResult(
                QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery("status", 200)).add(QueryBuilders.fuzzyQuery("tags", "x"))
            ).isComplete()
        );
    }

    /**
     * A wrapper inside an all-or-nothing context reaches the strict walk, which carries its own wrapper arms — the
     * collecting walk unwraps a constant_score or a boosting long before a dis_max arm is dispatched. Without them
     * the caller would lose the whole clause to a wrapper that only replaces a score.
     */
    public void testScoreOnlyWrappersInsideAnAllOrNothingContext() {
        Expression boosting = translate(
            QueryBuilders.disMaxQuery()
                .add(QueryBuilders.boostingQuery(QueryBuilders.termQuery("status", 200), QueryBuilders.termQuery("tags", "t1")))
                .add(QueryBuilders.termQuery("status", 404))
        );
        assertThat(boosting, instanceOf(Or.class));
        // A single-armed dis_max is that arm, so the unwrapping is visible in the emitted node rather than inferred.
        assertThat(
            translate(
                QueryBuilders.disMaxQuery()
                    .add(QueryBuilders.boostingQuery(QueryBuilders.termQuery("status", 200), QueryBuilders.termQuery("tags", "t1")))
            ),
            instanceOf(MvContains.class)
        );
        assertThat(
            translate(QueryBuilders.disMaxQuery().add(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("status", 200)))),
            instanceOf(MvContains.class)
        );
        // The negative clause still only changes a score, so an untranslatable one costs nothing even here.
        assertThat(
            translate(
                QueryBuilders.disMaxQuery()
                    .add(QueryBuilders.boostingQuery(QueryBuilders.termQuery("status", 200), QueryBuilders.fuzzyQuery("tags", "x")))
            ),
            instanceOf(MvContains.class)
        );
    }

    /**
     * An ip value carrying a {@code /} is a block, not an address: IpFieldType.termQuery answers it with
     * InetAddressPoint.newPrefixQuery, which matches every address in the subnet. The block is contiguous in the
     * encoded ordering, so it is an inclusive range over its first and last address.
     */
    public void testIpCidrBlockBecomesAnInclusiveRange() {
        MvInRange block = (MvInRange) translate(QueryBuilders.termQuery("client_ip", "10.0.0.0/29"));
        assertEquals(EsqlDataTypeConverter.stringToIP("10.0.0.0"), ((Literal) block.lower()).value());
        assertEquals(EsqlDataTypeConverter.stringToIP("10.0.0.7"), ((Literal) block.upper()).value());
        // A /32 is the single address, so the block form and the address form select the same one row.
        MvInRange single = (MvInRange) translate(QueryBuilders.termQuery("client_ip", "10.0.0.1/32"));
        assertEquals(EsqlDataTypeConverter.stringToIP("10.0.0.1"), ((Literal) single.lower()).value());
        assertEquals(EsqlDataTypeConverter.stringToIP("10.0.0.1"), ((Literal) single.upper()).value());
        // An IPv6 block takes the same path, over the same 16-byte encoding.
        MvInRange v6 = (MvInRange) translate(QueryBuilders.termQuery("client_ip", "2001:db8::/126"));
        assertEquals(EsqlDataTypeConverter.stringToIP("2001:db8::"), ((Literal) v6.lower()).value());
        assertEquals(EsqlDataTypeConverter.stringToIP("2001:db8::3"), ((Literal) v6.upper()).value());
    }

    /**
     * A lenient match folds a value the type cannot represent to false. A CIDR block is not such a value — the index
     * matches the whole subnet — so folding it would return none of the rows the caller asked for, which is the one
     * direction this translator may never take.
     */
    public void testLenientMatchOnACidrBlockStillSelectsTheBlock() {
        assertThat(translate(QueryBuilders.matchQuery("client_ip", "10.0.0.0/29").lenient(true)), instanceOf(MvInRange.class));
        assertThat(translate(QueryBuilders.matchQuery("client_ip", "10.0.0.0/29")), instanceOf(MvInRange.class));
        // A malformed block is malformed like any other value: lenient folds it to false, strict degrades.
        assertEquals(Literal.FALSE, translate(QueryBuilders.matchQuery("client_ip", "10.0.0.0/99").lenient(true)));
        assertFalse(translateResult(QueryBuilders.termQuery("client_ip", "10.0.0.0/99")).isComplete());
        assertEquals("term[literal on ip]", constructOf(QueryBuilders.termQuery("client_ip", "10.0.0.0/99")));
    }

    /**
     * One block in a terms list makes the index abandon its set query for a disjunction of term queries
     * (IpFieldType.termsQuery), so the list is a union of blocks and whatever plain addresses remain.
     */
    public void testIpTermsMixesBlocksAndAddresses() {
        Expression mixed = translate(QueryBuilders.termsQuery("client_ip", List.of("10.0.0.0/29", "192.168.1.1")));
        assertThat(mixed, instanceOf(Or.class));
        assertThat(((Or) mixed).left(), instanceOf(MvInRange.class));
        assertThat(((Or) mixed).right(), instanceOf(MvIntersects.class));
        // Blocks only: still a union, with no set-membership leaf at all.
        Expression blocks = translate(QueryBuilders.termsQuery("client_ip", List.of("10.0.0.0/29", "10.1.0.0/29")));
        assertThat(blocks, instanceOf(Or.class));
        assertThat(((Or) blocks).left(), instanceOf(MvInRange.class));
        assertThat(((Or) blocks).right(), instanceOf(MvInRange.class));
        // No block: the plain set-membership leaf, unchanged.
        assertThat(translate(QueryBuilders.termsQuery("client_ip", List.of("10.0.0.1", "192.168.1.1"))), instanceOf(MvIntersects.class));
    }

    /**
     * A range may name the format its bounds are written in, and the bounds are then parsed with that formatter
     * rather than the field's default — so the same instant spelled two ways translates to the same bound. A pattern
     * that does not compile never arrives: RangeQueryBuilder.format compiles it when the builder is built, so a
     * malformed one is a 400 from request parsing. A bound the named format cannot read is a different matter, and
     * degrades on the bound rule.
     */
    public void testRangeFormatOptionIsHonoured() {
        MvInRange formatted = (MvInRange) translate(
            QueryBuilders.rangeQuery("@timestamp").gte("2024/01/01").lte("2024/01/31").format("yyyy/MM/dd")
        );
        MvInRange iso = (MvInRange) translate(QueryBuilders.rangeQuery("@timestamp").gte("2024-01-01").lte("2024-01-31"));
        assertEquals(((Literal) iso.lower()).value(), ((Literal) formatted.lower()).value());
        assertEquals(((Literal) iso.upper()).value(), ((Literal) formatted.upper()).value());
        assertEquals(
            "range[date bound on datetime]",
            constructOf(QueryBuilders.rangeQuery("@timestamp").gte("2024-01-01").format("yyyy/MM/dd"))
        );
    }

    /**
     * A date_nanos bound written as a string is parsed with the nanos formatter, so sub-millisecond precision in the
     * bound survives instead of being truncated to the millisecond.
     */
    public void testDateNanosStringBoundKeepsNanosecondPrecision() {
        MvInRange e = (MvInRange) translate(
            QueryBuilders.rangeQuery("ts_nanos").gte("2020-06-15T12:00:00.123456789Z").lte("2020-06-15T12:00:01Z")
        );
        assertEquals(DateUtils.toLong(Instant.parse("2020-06-15T12:00:00.123456789Z")), ((Literal) e.lower()).value());
    }

    /**
     * A percentage or an expression minimum_should_match is legal DSL that the OR rewrite cannot express. It parses
     * as neither 0 nor 1, so the clause degrades — naming the value, because that is what the caller wrote.
     */
    public void testUnparseableMinimumShouldMatchDegrades() {
        for (String msm : List.of("75%", "2<-1", "nonsense")) {
            var result = translateResult(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("status", 1))
                    .should(QueryBuilders.termQuery("status", 2))
                    .minimumShouldMatch(msm)
            );
            assertFalse("[" + msm + "] cannot be honoured", result.isComplete());
            assertEquals("bool[minimum_should_match=" + msm + "]", result.unsupported().get(0).construct());
        }
    }

    /**
     * A multi_match reaches an ip column alongside columns that cannot hold the value at all. Under leniency the
     * others fold to false, and the ip arm of the union must still select the block rather than folding with them —
     * which is what the fieldless form does over a whole schema (covered end to end by the conformance differential,
     * since a fieldless multi_match over this fixture's analyzed text field degrades before it reaches ip).
     */
    public void testMultiMatchOnACidrBlockKeepsTheBlockArm() {
        Expression e = translate(QueryBuilders.multiMatchQuery("10.0.0.0/29", "client_ip", "status").lenient(true));
        assertThat(e, instanceOf(Or.class));
        // By arm, not by side: multi_match holds its fields in a map, so which arm lands left is not fixed.
        assertEquals("only the ip arm survives, and it survives as a block range", 1, e.collect(n -> n instanceof MvInRange).size());
        assertEquals("the integer arm folds to false under leniency", 1, e.collect(Literal.FALSE::equals).size());
    }

    /** A boosting wrapper is unwrapped inside the collecting walk too, so an inner bool still reports one leaf. */
    public void testBoostingContentsReportPerLeaf() {
        var result = translateResult(
            QueryBuilders.boostingQuery(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).must(QueryBuilders.fuzzyQuery("tags", "x")),
                QueryBuilders.termQuery("tags", "t1")
            )
        );
        assertFalse(result.isComplete());
        assertEquals(1, result.unsupported().size());
        assertEquals("fuzzy", result.unsupported().get(0).construct());
        assertThat(result.applied(), instanceOf(MvContains.class));
    }

    /**
     * A type the translator has no encoding for is rejected rather than guessed at: handing the evaluator a value it
     * cannot read would answer a different question, and over-returning is the only safe direction.
     */
    public void testLiteralOnATypeWithNoEncodingDegrades() {
        assertEquals("term[literal on geo_point]", constructOf(QueryBuilders.termQuery("location", "POINT (1 2)")));
        assertEquals("terms[literal on geo_point]", constructOf(QueryBuilders.termsQuery("location", List.of("POINT (1 2)"))));
        assertEquals("match[literal on geo_point]", constructOf(QueryBuilders.matchQuery("location", "POINT (1 2)")));
        // Leniency does not buy an encoding: the capability is missing, which is not a malformed value.
        assertFalse(translateResult(QueryBuilders.matchQuery("location", "POINT (1 2)").lenient(true)).isComplete());
    }

    /** A terms-lookup has no values to translate (and values() is null — it used to NPE). */
    public void testTermsLookupIsUnsupported() {
        var lookup = new TermsQueryBuilder("status", new TermsLookup("idx", "1", "path"));
        assertFalse(translateResult(lookup).isComplete());
    }

    /** An empty terms list is legal DSL and matches nothing (it used to NPE on a null data type). */
    public void testEmptyTermsMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.termsQuery("status", List.of())));
    }

    /** These options change what the query means; translating without them would silently mis-match. */
    public void testUnhonorableOptionsAreUnsupported() {
        // n-of-m (anything but 0 or 1) cannot be expressed as an OR
        assertFalse(
            translateResult(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("status", 1))
                    .should(QueryBuilders.termQuery("status", 2))
                    .minimumShouldMatch(2)
            ).isComplete()
        );
        assertFalse(translateResult(QueryBuilders.rangeQuery("@timestamp").gte("2024-01-01T00:00:00Z").timeZone("+02:00")).isComplete());
    }

    /**
     * A case-insensitive term on a keyword folds both sides to lower case: {@code mv_contains(TO_LOWER(field), lowered)}.
     * {@code TO_LOWER} maps over each value, so the any-value shape holds; the exact value (whitespace included — keyword
     * is not analyzed) is lower-cased, not the field name.
     */
    public void testCaseInsensitiveTermOnKeyword() {
        Expression e = translate(QueryBuilders.termQuery("tags", "AbC").caseInsensitive(true));
        assertThat(e, instanceOf(MvContains.class));
        MvContains c = (MvContains) e;
        assertThat("the field side is lower-cased", c.children().get(0), instanceOf(ToLower.class));
        Literal value = (Literal) c.children().get(1);
        assertEquals(DataType.KEYWORD, value.dataType());
        assertEquals("the value side is lower-cased", new BytesRef("abc"), value.value());
    }

    /**
     * The index rejects {@code case_insensitive} on a non-string field, and analyzed {@code text} has no faithful
     * structural equality — both collect as unsupported rather than answering a narrower question.
     */
    public void testCaseInsensitiveTermOnNonKeywordIsUnsupported() {
        assertFalse(translateResult(QueryBuilders.termQuery("status", "1").caseInsensitive(true)).isComplete());
        assertFalse(translateResult(QueryBuilders.termQuery("body", "x").caseInsensitive(true)).isComplete());
    }

    /**
     * A large {@code terms} clause must not build a left-leaning OR chain: Lucene's terms query routinely carries
     * hundreds of values, and a chain would be one level deep per value for every recursive plan traversal to walk.
     * The balanced fold keeps it logarithmic.
     */
    public void testLargeTermsFoldsIntoABalancedTree() {
        List<Object> many = new java.util.ArrayList<>();
        for (int i = 0; i < 512; i++) {
            many.add("2020-06-" + String.format(java.util.Locale.ROOT, "%02d", (i % 28) + 1));
        }
        Expression e = translate(QueryBuilders.termsQuery("@timestamp", many));
        assertThat(e, instanceOf(Or.class));
        assertThat("512 disjuncts must fold to ~log2 depth, not a 512-deep chain", depth(e), lessThanOrEqualTo(12));
    }

    private static int depth(Expression e) {
        int max = 0;
        for (Expression c : e.children()) {
            max = Math.max(max, depth(c));
        }
        return max + 1;
    }

    /** A missing field is null-bound and folds to false regardless of case — the ordinary null-bound equality leaf. */
    public void testCaseInsensitiveTermOnMissingFieldFoldsLikeEquality() {
        Expression e = translate(QueryBuilders.termQuery("nope", "X").caseInsensitive(true));
        assertThat(e, instanceOf(MvContains.class));
        assertEquals(Literal.NULL, ((MvContains) e).children().get(0));
    }

    /**
     * The request locale's ASCII case-fold must match the index's locale-independent fold. A Turkish locale lower-cases
     * `I` to dotless `ı`, so `case_insensitive` there would silently under-match the index — collected as unsupported.
     */
    public void testCaseInsensitiveTermCollectedUnderDivergentLocale() {
        assertFalse(
            translateResult(QueryBuilders.termQuery("tags", "WINDOWS").caseInsensitive(true), Locale.forLanguageTag("tr-TR")).isComplete()
        );
        // The same value under a ROOT-equivalent ASCII locale is fine.
        assertThat(translate(QueryBuilders.termQuery("tags", "WINDOWS").caseInsensitive(true), Locale.US), instanceOf(MvContains.class));
    }

    /**
     * The subtle half: a LOWER-case term folds to itself even under a Turkish locale, so checking only the term would
     * let it through — but the field side folds STORED values with that same locale, so a stored "MIX" becomes "mıx"
     * and the row is dropped where an index matches it. Collected as unsupported on the term's upper-case image too.
     */
    public void testCaseInsensitiveTermCollectedWhenStoredUpperCaseWouldMisfold() {
        assertFalse(
            translateResult(QueryBuilders.termQuery("tags", "mix").caseInsensitive(true), Locale.forLanguageTag("tr-TR")).isComplete()
        );
        assertThat(translate(QueryBuilders.termQuery("tags", "mix").caseInsensitive(true), Locale.US), instanceOf(MvContains.class));
    }

    /** A non-ASCII value can fold differently from the index's per-codepoint automaton, so it is collected as unsupported. */
    public void testCaseInsensitiveTermCollectedOnNonAsciiValue() {
        assertFalse(translateResult(QueryBuilders.termQuery("tags", "café").caseInsensitive(true)).isComplete());
    }

    /** The case-insensitive leaf is two-valued, so it composes under negation like every other leaf: NOT over mv_contains. */
    public void testCaseInsensitiveTermUnderMustNotComposes() {
        Expression e = translate(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("tags", "A").caseInsensitive(true)));
        assertThat(e, instanceOf(Not.class));
        Expression inner = ((Not) e).children().get(0);
        assertThat(inner, instanceOf(MvContains.class));
        assertThat("the negated leaf still lower-cases the field", ((MvContains) inner).children().get(0), instanceOf(ToLower.class));
    }

    /**
     * An explicit {@code minimum_should_match: 1} is exactly what Kibana's "is one of" pill and every KQL {@code or}
     * emit. It means "at least one should clause must match" — a plain OR — and must be honored, not refused.
     */
    public void testExplicitMinimumShouldMatchOfOneIsAnOr() {
        Expression e = translate(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("status", 1))
                .should(QueryBuilders.termQuery("status", 2))
                .minimumShouldMatch(1)
        );
        assertThat(e, instanceOf(Or.class));

        // alongside a must, msm:1 makes the should REQUIRED (unlike the default, where it would drop)
        Expression withMust = translate(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.existsQuery("status"))
                .should(QueryBuilders.termQuery("status", 1))
                .minimumShouldMatch(1)
        );
        assertThat(withMust, instanceOf(And.class));
    }

    /** msm:0 makes the should clauses optional, so alongside a must/filter they drop out entirely. */
    public void testMinimumShouldMatchOfZeroDropsShould() {
        Expression e = translate(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.existsQuery("status"))
                .should(QueryBuilders.termQuery("status", 1))
                .minimumShouldMatch(0)
        );
        assertThat(e, instanceOf(IsNotNull.class));
    }

    /**
     * But msm:0 on a should-ONLY bool does NOT match everything: Lucene still requires one optional clause when there
     * is no required (must/filter) clause. So the should group stays required — a plain OR, not {@code Literal.TRUE}.
     */
    public void testMinimumShouldMatchOfZeroOnShouldOnlyStillRequiresOne() {
        Expression e = translate(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("status", 1))
                .should(QueryBuilders.termQuery("status", 2))
                .minimumShouldMatch(0)
        );
        assertThat(e, instanceOf(Or.class));
    }

    /**
     * A numeric term the field's integral type can never equal — a decimal, or a value outside the type's range —
     * matches nothing, exactly as the index path's term query returns match-no-docs (never a truncated/wrapped match).
     */
    public void testUnmatchableIntegralTermMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.termQuery("status", 2.5))); // decimal vs integer
        assertEquals(Literal.FALSE, translate(QueryBuilders.termQuery("status", 4294967298L))); // outside int range
        assertEquals(Literal.FALSE, translate(QueryBuilders.termQuery("bytes", 3.5))); // decimal vs long
        // a whole-number value in range is a normal contains
        assertThat(translate(QueryBuilders.termQuery("status", 200)), instanceOf(MvContains.class));
    }

    /**
     * A term outside the field type's range can equal no value of that type, at either end — the index's own parse
     * rejects it rather than clamping, so the clause matches nothing instead of matching the extreme.
     */
    public void testIntegralTermOutsideTheTypeRangeMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.termQuery("status", 3_000_000_000L)));
        assertEquals(Literal.FALSE, translate(QueryBuilders.termQuery("status", -3_000_000_000L)));
        // The same values on a long field are ordinary terms, so the assertions above are not vacuous.
        assertThat(translate(QueryBuilders.termQuery("bytes", 3_000_000_000L)), instanceOf(MvContains.class));
        assertThat(translate(QueryBuilders.termQuery("bytes", -3_000_000_000L)), instanceOf(MvContains.class));
    }

    /**
     * A bound arrives either as a number or as its string spelling depending on how the request was written, and the
     * index reads both to the same value. The two must therefore translate to the same bound literal.
     */
    public void testRangeBoundReadsANumberAndItsStringAlike() {
        MvInRange fromNumber = (MvInRange) translate(QueryBuilders.rangeQuery("bytes").gt(5));
        MvInRange fromString = (MvInRange) translate(QueryBuilders.rangeQuery("bytes").gt("5"));
        assertEquals(((Literal) fromNumber.lower()).value(), ((Literal) fromString.lower()).value());
        MvInRange toNumber = (MvInRange) translate(QueryBuilders.rangeQuery("bytes").lt(10));
        MvInRange toString = (MvInRange) translate(QueryBuilders.rangeQuery("bytes").lt("10"));
        assertEquals(((Literal) toNumber.upper()).value(), ((Literal) toString.upper()).value());
        // A fractional spelling is read the same way from either shape, and rounds inward from either.
        MvInRange decimalNumber = (MvInRange) translate(QueryBuilders.rangeQuery("bytes").gt(4.5));
        MvInRange decimalString = (MvInRange) translate(QueryBuilders.rangeQuery("bytes").gt("4.5"));
        assertEquals(((Literal) decimalNumber.lower()).value(), ((Literal) decimalString.lower()).value());
    }

    /** terms drops the values no value of an integral field can equal; an emptied set matches nothing. */
    public void testTermsDropsUnmatchableIntegralValues() {
        Expression e = translate(QueryBuilders.termsQuery("status", List.of(200, 2.5, 404)));
        assertThat(e, instanceOf(MvIntersects.class));
        assertEquals(List.of(200, 404), ((Literal) ((MvIntersects) e).children().get(1)).value());
        assertEquals(Literal.FALSE, translate(QueryBuilders.termsQuery("status", List.of(2.5, 7.5))));
    }

    /**
     * A construct the emitted function cannot type — a range over a boolean column, which mv_in_range does not support —
     * is collected as unsupported rather than sailing past the post-analysis rewrite into a compute-engine error.
     */
    public void testRangeOverUnsupportedFieldTypeDegrades() {
        assertFalse(translateResult(QueryBuilders.rangeQuery("active").gte(false).lte(true)).isComplete());
        assertFalse(translateResult(QueryBuilders.rangeQuery("active").gt(false)).isComplete());
    }

    /**
     * The DSL default for minimum_should_match is 1 when the bool has no must/filter — and must_not does NOT count
     * towards that. So must_not + should still requires a should clause to match.
     */
    public void testMustNotDoesNotSuppressTheDefaultShouldRequirement() {
        Expression e = translate(
            QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("status", 9))
                .should(QueryBuilders.termQuery("status", 1))
                .should(QueryBuilders.termQuery("status", 2))
        );
        // Not(term) AND (term OR term) — the should survives as a required conjunct
        assertThat(e, instanceOf(And.class));
        And and = (And) e;
        assertThat(and.left(), instanceOf(Not.class));
        assertThat(and.right(), instanceOf(Or.class));
    }

    private static long millis(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    /**
     * A coarse upper bound rounds UP to the last millis of its unit, exactly as the index path does — {@code lte
     * "2020-06-15"} means through the end of that day, not its start. Proves B3's round-up on the one-sided upper path.
     */
    public void testDateRangeCoarseUpperBoundRoundsUp() {
        Expression e = translate(QueryBuilders.rangeQuery("@timestamp").lte("2020-06-15"));
        assertThat(e, instanceOf(MvLess.class));
        Literal bound = (Literal) ((MvLess) e).bound();
        assertEquals(millis("2020-06-15T23:59:59.999Z"), bound.value());
    }

    /** A coarse lower bound rounds DOWN to the first millis of its unit — {@code gte "2020-06-15"} starts at midnight. */
    public void testDateRangeCoarseLowerBoundRoundsDown() {
        Expression e = translate(QueryBuilders.rangeQuery("@timestamp").gte("2020-06-15"));
        assertThat(e, instanceOf(MvGreater.class));
        Literal bound = (Literal) ((MvGreater) e).bound();
        assertEquals(millis("2020-06-15T00:00:00.000Z"), bound.value());
    }

    /**
     * A {@code now} date-math bound resolves against the query's start time, not wall-clock — the single most common
     * Kibana time filter shape. With a fixed test {@code now} of 2020-06-15T12:00Z, {@code gte "now-1d"} is
     * 2020-06-14T12:00Z. Used to degrade (the plain parser could not read date math) — B3.
     */
    public void testDateRangeNowMathResolvesAgainstQueryNow() {
        Expression e = translate(QueryBuilders.rangeQuery("@timestamp").gte("now-1d"));
        assertThat(e, instanceOf(MvGreater.class));
        Literal bound = (Literal) ((MvGreater) e).bound();
        assertEquals(millis("2020-06-14T12:00:00.000Z"), bound.value());
    }

    /**
     * A two-bound date range with exclusive ends folds to a closed inclusive {@code mv_in_range}: the rounding and the
     * one-unit nudge are baked into the bounds, so {@code gt}/{@code lt} on a date never needs the whole-number gate.
     */
    public void testDateTwoBoundExclusiveRangeIsClosedMvInRange() {
        Expression e = translate(QueryBuilders.rangeQuery("@timestamp").gt("2020-06-15T00:00:00.000Z").lt("2020-06-15T00:00:00.010Z"));
        assertThat(e, instanceOf(MvInRange.class));
        MvInRange r = (MvInRange) e;
        assertEquals(millis("2020-06-15T00:00:00.001Z"), ((Literal) r.lower()).value());
        assertEquals(millis("2020-06-15T00:00:00.009Z"), ((Literal) r.upper()).value());
    }

    /**
     * A {@code term} on a date field is the closed range spanning the value's rounding unit, not a point — {@code
     * term "2020-06-15"} matches any instant that whole day, mirroring the index path's term-as-range on dates.
     */
    public void testDateTermIsUnitRange() {
        Expression e = translate(QueryBuilders.termQuery("@timestamp", "2020-06-15"));
        assertThat(e, instanceOf(MvInRange.class));
        MvInRange r = (MvInRange) e;
        assertEquals(millis("2020-06-15T00:00:00.000Z"), ((Literal) r.lower()).value());
        assertEquals(millis("2020-06-15T23:59:59.999Z"), ((Literal) r.upper()).value());
    }

    /** {@code terms} on a date field is the union of its per-value unit ranges. */
    public void testDateTermsIsUnionOfUnitRanges() {
        Expression e = translate(QueryBuilders.termsQuery("@timestamp", List.of("2020-06-15", "2020-06-16")));
        assertThat(e, instanceOf(Or.class));
        Or or = (Or) e;
        assertThat(or.left(), instanceOf(MvInRange.class));
        assertThat(or.right(), instanceOf(MvInRange.class));
    }

    /** An exclusive bound sitting at the type's limit leaves an empty open interval beyond it, so it matches nothing. */
    public void testExclusiveBoundAtIntegerLimitMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.rangeQuery("status").gt(Integer.MAX_VALUE).lte(Integer.MAX_VALUE)));
    }

    /** A range with neither bound is a tautology — it matches everything. */
    public void testRangeWithNoBoundsMatchesEverything() {
        assertEquals(Literal.TRUE, translate(QueryBuilders.rangeQuery("status")));
    }

    /** When rounding pushes the lower bound past the upper (an exclusive one-day date range), it matches nothing. */
    public void testDateRangeCollapsedByRoundingMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.rangeQuery("@timestamp").gt("2020-06-15").lt("2020-06-15")));
    }

    /**
     * A range with an EXCLUSIVE bound over a MISSING field must still fold to false (leniency) — the index path's
     * unmapped-field range matches nothing. It used to degrade the whole filter (unfiltered) instead: the exclusive
     * bound tripped the whole-number check on the NULL type before leniency could apply.
     */
    public void testExclusiveBoundOnMissingFieldFoldsToFalseNotDegrade() {
        // The MvInRange is over a NULL-bound field and folds to false, exactly like the inclusive case — no throw.
        Expression e = translate(QueryBuilders.rangeQuery("missing_field").gte(0).lt(10));
        assertThat(e, instanceOf(MvInRange.class));
        assertEquals(Literal.NULL, ((MvInRange) e).children().get(0));
    }

    /** An analyzed text field matches on tokens in the index; a structural leaf would under-match, so they are collected. */
    public void testTermOnAnalyzedTextDegrades() {
        assertFalse(translateResult(QueryBuilders.termQuery("body", "quick")).isComplete());
        assertFalse(translateResult(QueryBuilders.termsQuery("body", List.of("quick", "brown"))).isComplete());
        assertFalse(translateResult(QueryBuilders.rangeQuery("body").gte("a").lt("z")).isComplete());
    }

    /** exists over an analyzed text field is fine — it is analysis-independent and does not go through the leaf chokepoint. */
    public void testExistsOnAnalyzedTextIsSupported() {
        assertThat(translate(QueryBuilders.existsQuery("body")), instanceOf(IsNotNull.class));
    }

    /**
     * A numeric date bound is epoch MILLIS on both date types (the index parses it via epoch_millis). On date_nanos the
     * internal unit is nanos, so it must be scaled up — not read as a raw nanos count (which would land in 1970).
     */
    public void testNumericBoundOnDateNanosIsMillisScaledToNanos() {
        long millis = millis("2020-06-15T00:00:00.000Z");
        Expression e = translate(QueryBuilders.rangeQuery("ts_nanos").gte(millis));
        Literal bound = (Literal) ((MvGreater) e).bound();
        assertEquals(millis * 1_000_000L, bound.value());
    }

    /** A numeric date_nanos bound before the epoch is outside the type's representable range; it is collected, not 500s. */
    public void testOutOfRangeNumericDateNanosBoundDegrades() {
        assertFalse(translateResult(QueryBuilders.rangeQuery("ts_nanos").gte(-5000)).isComplete());
    }

    /**
     * A numeric date_nanos bound that ROUNDS UP (an inclusive upper) reaches the last nanosecond of its milli, matching
     * the index's epoch_millis round-up parser (NANOS_OF_MILLI defaults to 999_999). Without this an upper bound would
     * under-match every sub-milli row. The round-DOWN direction (gte) is pinned above.
     */
    public void testNumericUpperBoundOnDateNanosRoundsUpToLastNano() {
        long millis = millis("2020-06-15T00:00:00.000Z");
        Expression e = translate(QueryBuilders.rangeQuery("ts_nanos").lte(millis));
        Literal bound = (Literal) ((MvLess) e).bound();
        assertEquals(millis * 1_000_000L + 999_999L, bound.value());
    }

    /**
     * adjust_pure_negative=false makes a bool of only must_not clauses match NOTHING on the index; we model the default
     * (match everything not excluded), so a pure-negative bool with the flag off is collected, not silently over-matching.
     */
    public void testPureNegativeBoolWithAdjustDisabledDegrades() {
        assertFalse(
            translateResult(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("tags", "x")).adjustPureNegative(false)).isComplete()
        );
        // the flag is harmless when the bool is not pure-negative (a must clause is present)
        assertThat(
            translate(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.existsQuery("tags"))
                    .mustNot(QueryBuilders.termQuery("tags", "x"))
                    .adjustPureNegative(false)
            ),
            instanceOf(And.class)
        );
    }

    /** A match on an exact-typed field IS a term — plain equality (mv_contains), same as the index builds. */
    public void testMatchOnExactFieldIsEquality() {
        assertThat(translate(QueryBuilders.matchQuery("status", 200)), instanceOf(MvContains.class));
        assertThat(translate(QueryBuilders.matchQuery("tags", "x")), instanceOf(MvContains.class));
    }

    /** A match on a date field is the rounding-unit range, like a term on a date. */
    public void testMatchOnDateIsUnitRange() {
        assertThat(translate(QueryBuilders.matchQuery("@timestamp", "2020-06-15")), instanceOf(MvInRange.class));
    }

    /** A match on a field the source lacks binds to null and folds to false — the same leniency as term. */
    public void testMatchOnMissingFieldFoldsViaNull() {
        Expression e = translate(QueryBuilders.matchQuery("nope", "x"));
        assertThat(e, instanceOf(MvContains.class));
        assertEquals(Literal.NULL, ((MvContains) e).children().get(0));
    }

    /** A match on an analyzed text field needs real analysis we do not do here — collected, never silently approximate. */
    public void testMatchOnAnalyzedTextDegrades() {
        assertFalse(translateResult(QueryBuilders.matchQuery("body", "quick")).isComplete());
    }

    /** Options that change what matches (analyzer, fuzziness, minimum_should_match) cannot be honored as equality. */
    public void testMatchWithMatchingOptionsDegrades() {
        assertFalse(translateResult(QueryBuilders.matchQuery("status", 200).fuzziness(Fuzziness.ONE)).isComplete());
        assertFalse(translateResult(QueryBuilders.matchQuery("status", 200).analyzer("standard")).isComplete());
        assertFalse(translateResult(QueryBuilders.matchQuery("status", 200).minimumShouldMatch("2")).isComplete());
    }

    /** A lenient match over a malformed value on an encodable type matches nothing; a strict one is collected. */
    public void testLenientMatchOnMalformedValueMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.matchQuery("status", "abc").lenient(true)));
        assertFalse(translateResult(QueryBuilders.matchQuery("status", "abc")).isComplete());
    }

    /** A whole-number STRING on an integral field is that integer, exactly as the index coerces "300.0" to 300. */
    public void testWholeNumberStringOnIntegerMatches() {
        Expression e = translate(QueryBuilders.matchQuery("status", "300.0"));
        assertThat(e, instanceOf(MvContains.class));
        assertEquals(300, ((Literal) ((MvContains) e).children().get(1)).value());
        // Surrounding whitespace is ignored, as the index's Double.parseDouble does.
        assertThat(translate(QueryBuilders.matchQuery("status", " 300 ")), instanceOf(MvContains.class));
    }

    /** A well-formed decimal (or out-of-range) value on an integral field equals nothing — the index's match-no-docs. */
    public void testDecimalStringOnIntegerMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.matchQuery("status", "300.5")));
        assertEquals(Literal.FALSE, translate(QueryBuilders.matchQuery("status", "3000000000"))); // out of int range
    }

    /**
     * A lenient match on a capability we do not have — analyzed text — is collected, not silently mapped to
     * match-nothing, because the index would actually match. Lenient means "skip a value this type cannot hold", never
     * "drop a whole capability". Regression guard for the lenient-swallows-everything bug.
     */
    public void testLenientMatchOnUnsupportedCapabilityDegrades() {
        assertFalse(translateResult(QueryBuilders.matchQuery("body", "hello").lenient(true)).isComplete());
    }

    /**
     * ip, version and unsigned_long literals encode through the planner's own converters, so a filter on a dataset
     * column of those types translates like any other. Each literal carries the FIELD's type: the rewrite runs after
     * the analyzer, so nothing downstream inserts the cast a user-written WHERE would get.
     */
    public void testIpVersionAndUnsignedLongLiteralsEncode() {
        record Case(QueryBuilder query, DataType type, Object encoded) {}
        for (Case each : List.of(
            new Case(QueryBuilders.termQuery("client_ip", "10.0.0.1"), DataType.IP, EsqlDataTypeConverter.stringToIP("10.0.0.1")),
            new Case(QueryBuilders.termQuery("release", "8.19.1"), DataType.VERSION, EsqlDataTypeConverter.stringToVersion("8.19.1")),
            new Case(QueryBuilders.termQuery("quota", "42"), DataType.UNSIGNED_LONG, EsqlDataTypeConverter.stringToUnsignedLong("42"))
        )) {
            Expression e = translate(each.query());
            assertThat(each.type().toString(), e, instanceOf(MvContains.class));
            Literal literal = (Literal) ((MvContains) e).children().get(1);
            assertEquals("the literal takes the field's type", each.type(), literal.dataType());
            // The value, not just the type: the encoding is the whole claim, and a plausible wrong one types the same.
            assertEquals("the literal is encoded as the compute engine reads it", each.encoded(), literal.value());
        }
    }

    /**
     * {@code terms} and {@code range} reach the same encodings as {@code term}, so they translate rather than degrade.
     * An unsigned_long is compared in its biased form, which is what makes the ordering below the true ordering.
     */
    public void testIpVersionAndUnsignedLongTermsAndRangesTranslate() {
        assertThat(translate(QueryBuilders.termsQuery("client_ip", List.of("10.0.0.1", "10.0.0.2"))), instanceOf(MvIntersects.class));
        assertThat(translate(QueryBuilders.termsQuery("release", List.of("8.1.0", "9.0.0"))), instanceOf(MvIntersects.class));
        MvIntersects quotaTerms = (MvIntersects) translate(QueryBuilders.termsQuery("quota", List.of(1, 2)));
        assertEquals(
            List.of(EsqlDataTypeConverter.stringToUnsignedLong("1"), EsqlDataTypeConverter.stringToUnsignedLong("2")),
            ((Literal) quotaTerms.children().get(1)).value()
        );

        MvInRange ips = (MvInRange) translate(QueryBuilders.rangeQuery("client_ip").gte("10.0.0.1").lte("10.0.0.9"));
        assertEquals(EsqlDataTypeConverter.stringToIP("10.0.0.1"), ((Literal) ips.lower()).value());
        MvInRange quotas = (MvInRange) translate(QueryBuilders.rangeQuery("quota").gte(1).lte(10));
        assertEquals(EsqlDataTypeConverter.stringToUnsignedLong("1"), ((Literal) quotas.lower()).value());
        assertEquals(EsqlDataTypeConverter.stringToUnsignedLong("10"), ((Literal) quotas.upper()).value());
        assertThat(translate(QueryBuilders.rangeQuery("release").gte("8.0.0").lte("9.0.0")), instanceOf(MvInRange.class));
    }

    /** A lenient match on an ip the type cannot represent matches nothing, mirroring the index's lenient field query. */
    public void testLenientMatchOnMalformedIpMatchesNothing() {
        assertEquals(Literal.FALSE, translate(QueryBuilders.matchQuery("client_ip", "not-an-ip").lenient(true)));
    }

    /**
     * unsigned_long is an integral type, so it takes the integral narrowing rather than the generic encoding: a
     * fractional, negative or over-range value can equal no unsigned_long and matches nothing, exactly as the index's
     * {@code UnsignedLongFieldType.parseTerm} returns NO_DOCS for each. Truncating instead — which the generic
     * encoding does, {@code new BigDecimal("42.9").toBigInteger()} being 42 — would silently match 42.
     */
    public void testUnmatchableUnsignedLongTermMatchesNothing() {
        for (Object value : List.of("-1", "18446744073709551616", 42.9d, "0.5", -5)) {
            assertEquals("[" + value + "] can equal no unsigned_long", Literal.FALSE, translate(QueryBuilders.termQuery("quota", value)));
        }
        // A whole in-range value still matches, so the assertions above are not vacuous.
        assertThat(translate(QueryBuilders.termQuery("quota", 42)), instanceOf(MvContains.class));
        assertThat(translate(QueryBuilders.termQuery("quota", "42")), instanceOf(MvContains.class));
        // A non-numeric value is malformed, not unmatchable: it degrades, as on every other integral type.
        assertFalse(translateResult(QueryBuilders.termQuery("quota", "not-a-number")).isComplete());
    }

    /**
     * unsigned_long does not coerce a term the way integer and long do: UnsignedLongFieldType.parseTerm takes only an
     * integral box, an in-range BigInteger, or what Long.parseUnsignedLong reads. A whole double, a "42.0" and a
     * padded " 42" are well-formed numbers no unsigned_long equals, so each matches nothing — where the same shapes
     * on a long field are 42.
     */
    public void testUnsignedLongTermDoesNotCoerceLikeLong() {
        for (Object value : List.of(42.0d, "42.0", " 42")) {
            assertEquals(
                "[" + value + "] is not a term unsigned_long accepts",
                Literal.FALSE,
                translate(QueryBuilders.termQuery("quota", value))
            );
            assertThat(
                "the same shape on a long field does coerce",
                translate(QueryBuilders.termQuery("bytes", value)),
                instanceOf(MvContains.class)
            );
        }
        // terms applies the same rule value by value: the unmatchable one is dropped, the rest still select.
        MvIntersects kept = (MvIntersects) translate(QueryBuilders.termsQuery("quota", List.of("42.0", 43)));
        assertEquals(List.of(EsqlDataTypeConverter.stringToUnsignedLong("43")), ((Literal) kept.children().get(1)).value());
    }

    /**
     * The index caps a numeric string at {@code Numbers.MAX_NUMERIC_STRING_LENGTH} and rejects a longer one with a
     * bare IllegalArgumentException — not the NumberFormatException subclass every other malformed value throws. It is
     * the shape that escapes a narrower catch: nothing on the way out catches it, so it leaves the collecting walk and
     * fails the whole query, which is the one outcome the drop-and-warn policy forbids. Every arm that parses an
     * integral value must degrade instead.
     */
    public void testOverLongNumericStringDegradesRatherThanThrowing() {
        String tooLong = "1".repeat(Numbers.MAX_NUMERIC_STRING_LENGTH + 1);
        assertFalse("term degrades", translateResult(QueryBuilders.termQuery("quota", tooLong)).isComplete());
        assertFalse("terms degrades", translateResult(QueryBuilders.termsQuery("quota", List.of(tooLong))).isComplete());
        assertFalse("match degrades", translateResult(QueryBuilders.matchQuery("quota", tooLong)).isComplete());
        assertFalse("range bound degrades", translateResult(QueryBuilders.rangeQuery("quota").gte(tooLong)).isComplete());
        assertFalse("range upper bound degrades", translateResult(QueryBuilders.rangeQuery("quota").lte(tooLong)).isComplete());
        // A lenient match matches nothing rather than degrading, the same policy every other malformed value takes.
        assertEquals(Literal.FALSE, translate(QueryBuilders.matchQuery("quota", tooLong).lenient(true)));
        // The reachable shape that names no field at all: multi_match expands over the schema, unsigned_long included.
        assertFalse("fieldless multi_match degrades", translateResult(QueryBuilders.multiMatchQuery(tooLong)).isComplete());
        // One character shorter is accepted by the index, so the assertions above are not vacuous.
        String longest = "1".repeat(Numbers.MAX_NUMERIC_STRING_LENGTH);
        assertTrue("the longest accepted string still translates", translateResult(QueryBuilders.termQuery("quota", longest)).isComplete());
    }

    /** An unsigned_long RANGE bound does coerce a decimal, but Numbers.newBigDecimal rejects padding. */
    public void testUnsignedLongRangeBoundRejectsPaddingButTakesDecimals() {
        assertThat(translate(QueryBuilders.rangeQuery("quota").gte("0.5")), instanceOf(MvInRange.class));
        assertFalse(translateResult(QueryBuilders.rangeQuery("quota").gte(" 42")).isComplete());
    }

    /** A bound outside the unsigned_long range clamps rather than degrading, so gte -5 keeps matching everything. */
    public void testUnsignedLongRangeBoundsClampAndRoundInward() {
        MvInRange all = (MvInRange) translate(QueryBuilders.rangeQuery("quota").gte(-5));
        assertEquals(EsqlDataTypeConverter.stringToUnsignedLong("0"), ((Literal) all.lower()).value());
        assertEquals(Literal.FALSE, translate(QueryBuilders.rangeQuery("quota").lte(-5)));
        // A fractional lower bound rounds UP into the interval — gte 0.5 must not admit 0.
        MvInRange rounded = (MvInRange) translate(QueryBuilders.rangeQuery("quota").gte(0.5));
        assertEquals(EsqlDataTypeConverter.stringToUnsignedLong("1"), ((Literal) rounded.lower()).value());
    }

    /** A match_phrase on an exact field is the whole value — plain equality; a slop or a text field are collected. */
    public void testMatchPhraseOnExactFieldIsEquality() {
        assertThat(translate(QueryBuilders.matchPhraseQuery("tags", "x")), instanceOf(MvContains.class));
        assertFalse(translateResult(QueryBuilders.matchPhraseQuery("tags", "x").slop(2)).isComplete());
        assertFalse(translateResult(QueryBuilders.matchPhraseQuery("body", "x")).isComplete());
    }

    /** A multi_match over exact fields is an OR of per-field equality; a single resolved field collapses to one leaf. */
    public void testMultiMatchIsOrOfPerFieldEquality() {
        Expression e = translate(QueryBuilders.multiMatchQuery(200, "status", "bytes"));
        assertThat(e, instanceOf(Or.class));
        assertThat(((Or) e).left(), instanceOf(MvContains.class));
        assertThat(((Or) e).right(), instanceOf(MvContains.class));
        assertThat(translate(QueryBuilders.multiMatchQuery(200, "status")), instanceOf(MvContains.class));
    }

    /** multi_match field patterns are expanded against the schema; a pattern matching nothing matches nothing. */
    public void testMultiMatchExpandsPatternsAndEmptyMatchesNothing() {
        assertThat(translate(QueryBuilders.multiMatchQuery(1000L, "byte*")), instanceOf(MvContains.class));
        assertEquals(Literal.FALSE, translate(QueryBuilders.multiMatchQuery("x", "no_such_field*")));
    }

    /** A text field among the resolved set is collected as unsupported rather than silently dropping it. */
    public void testMultiMatchOverTextFieldDegrades() {
        assertFalse(translateResult(QueryBuilders.multiMatchQuery(200, "status", "body")).isComplete());
    }

    /**
     * A lenient multi_match drops a field whose type cannot hold the value (rather than failing the clause), keeping the
     * fields that can — mirroring the index. Here "t2" is malformed for the integer field but fine for the keyword one.
     */
    public void testLenientMultiMatchSkipsFieldsThatCannotHoldTheValue() {
        Expression e = translate(QueryBuilders.multiMatchQuery("t2", "status", "tags").lenient(true));
        assertThat(e, instanceOf(Or.class));
        // status -> false (malformed integer), tags -> mv_contains; the OR keeps the keyword leaf. Field order is
        // unspecified (the schema is a Set), so assert the pair of sides regardless of order.
        List<Expression> sides = List.of(((Or) e).left(), ((Or) e).right());
        assertTrue("the integer field is dropped to false", sides.contains(Literal.FALSE));
        assertTrue("the keyword field keeps its match", sides.stream().anyMatch(s -> s instanceof MvContains));
    }

    /** Only best_fields and phrase reduce to an OR of equality; other types fuse tokens/scores across fields. */
    public void testMultiMatchUnsupportedTypeDegrades() {
        assertFalse(
            translateResult(QueryBuilders.multiMatchQuery("x", "status").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)).isComplete()
        );
    }

    /**
     * B1: when parseBoolOptions throws (e.g. minimum_should_match:2), the translator records the failure but continues
     * translating the must/filter arms — those are valid AND conjuncts regardless of the bool-level option that failed.
     * Before the fix: early return with applied=TRUE, abandoning all must conjuncts.
     */
    public void testBoolOptionsFailureStillTranslatesMustArms() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().minimumShouldMatch(2).must(QueryBuilders.termQuery("status", 200))
        );
        assertFalse("bool-options failure makes result incomplete", result.isComplete());
        assertEquals(1, result.unsupported().size());
        assertThat(result.unsupported().get(0).construct(), containsString("minimum_should_match"));
        assertNotEquals("must arm must be applied, not abandoned to TRUE", Literal.TRUE, result.applied());
    }

    /**
     * B2: a non-required should arm that cannot be translated is silently omitted — it is not added to unsupported,
     * so isComplete() returns true and fail-closed mode does not throw a spurious 400.
     */
    public void testNonRequiredShouldFailureNotReported() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).should(QueryBuilders.fuzzyQuery("tags", "xyz"))
        );
        assertTrue("non-required should failure must not be reported", result.isComplete());
        assertNotEquals("must arm is applied", Literal.TRUE, result.applied());
    }

    /**
     * B3: a nested bool with a non-required should arm that fails is applied as a must-conjunct in an outer bool —
     * the must arms of the nested bool are preserved, not dropped.
     * Before the fix: collectingBool set anyFailed=true for any should arm failure, and the failure leaked into
     * conjunctUnsupported, causing addConjunct to drop the whole nested bool.
     */
    public void testNestedBoolNonRequiredShouldFailureKeepsMustArms() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).should(QueryBuilders.fuzzyQuery("tags", "xyz"))
                )
        );
        assertTrue("non-required nested should failure must not propagate", result.isComplete());
        assertNotEquals("nested must arm must be preserved in applied", Literal.TRUE, result.applied());
    }

    /**
     * When a required OR group has any failed arm, the whole group is dropped (applied stays TRUE).
     * A partial OR would exclude rows that matched only the dropped arm — under-fetch, which is the wrong
     * direction for a pre-filter. Failures are still reported.
     */
    public void testRequiredShouldAnyFailureDropsWholeGroup() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("status", 200)).should(QueryBuilders.fuzzyQuery("tags", "xyz"))
        );
        assertFalse("fuzzy arm failure is reported", result.isComplete());
        assertEquals("whole OR group dropped — pre-filter must over-fetch", Literal.TRUE, result.applied());
    }

    /**
     * minimum_should_match>1 makes the should group untranslatable but does NOT suppress must_not arms —
     * their NOT-logic is semantically independent of the should constraint. Before the fix, a single
     * {@code boolOptionsOk} flag dropped must_not for both failure kinds.
     */
    public void testMsmFailureDoesNotSuppressMustNotArms() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("status", 200))
                .should(QueryBuilders.termQuery("tags", "a"))
                .minimumShouldMatch(2)
        );
        assertFalse("msm failure is reported", result.isComplete());
        assertThat(result.unsupported().get(0).construct(), containsString("minimum_should_match"));
        assertThat("must_not arm must be applied despite msm failure", result.applied(), instanceOf(Not.class));
    }

    /**
     * adjust_pure_negative=false makes a pure-negative bool match NOTHING (Lucene suppresses the implicit match_all).
     * In partial mode the must_not arms must NOT be applied as NOT(arm) — that would be a semantic inversion
     * (returning rows instead of none). The failure is recorded and applied stays TRUE (unfiltered).
     */
    public void testAdjustPureNegativeFalseDoesNotApplyMustNotArms() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("status", 200)).adjustPureNegative(false)
        );
        assertFalse("adjust_pure_negative=false is an unsupported bool option", result.isComplete());
        assertEquals("must_not arms must not be applied — applied must be TRUE (unfiltered, safe)", Literal.TRUE, result.applied());
    }

    // --- Nested-bool B1 / B4 parity with the top-level translate() walk ---

    /**
     * B1 for nested bools: a nested bool with a parseBoolOptions failure (e.g. minimum_should_match:2) still
     * contributes its must/filter arms to the outer applied expression instead of being dropped entirely.
     * Kibana wraps filters in outer bools; losing the whole nested bool because of an inner bool-option failure
     * would silently discard valid conjuncts.
     */
    public void testNestedBoolOptionsFailureStillAppliesMustArms() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().minimumShouldMatch(2).must(QueryBuilders.termQuery("status", 200)))
        );
        assertFalse("nested bool-options failure is still reported", result.isComplete());
        assertNotEquals("nested must arm must be applied, not dropped to TRUE", Literal.TRUE, result.applied());
    }

    /**
     * Nested required-OR group with any failed arm: the whole OR group is dropped (same safe over-fetch policy
     * as the top-level translate). The outer must arm is still empty, so applied is TRUE.
     */
    public void testNestedRequiredShouldAnyFailureDropsWholeGroup() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.boolQuery().should(QueryBuilders.termQuery("status", 200)).should(QueryBuilders.fuzzyQuery("tags", "xyz"))
                )
        );
        assertFalse("fuzzy arm failure is reported", result.isComplete());
        assertEquals("whole nested OR group dropped — pre-filter must over-fetch", Literal.TRUE, result.applied());
    }

    /**
     * must_not in a nested bool is all-or-nothing: if the arm cannot be fully translated, NOT is skipped entirely
     * rather than applying NOT(partial), which would over-exclude and under-fetch.
     * The must arm is still applied; the failed must_not arm produces no NOT expression in the result.
     * If must_not were incorrectly applied, applied() would be And(must_expr, Not(fuzzy_expr)).
     * With the correct all-or-nothing policy, applied() is just the single must_expr (no And wrapper).
     */
    public void testNestedMustNotAllOrNothingSkipsPartialArm() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).mustNot(QueryBuilders.fuzzyQuery("tags", "xyz"))
                )
        );
        assertFalse("wildcard must_not arm failure is reported", result.isComplete());
        assertNotEquals("must arm is applied", Literal.TRUE, result.applied());
        // Only one conjunct (the must arm) — no And combining it with a NOT for the dropped must_not arm.
        assertThat("no And wrapping: must_not arm was not applied", result.applied(), not(instanceOf(And.class)));
    }
}
