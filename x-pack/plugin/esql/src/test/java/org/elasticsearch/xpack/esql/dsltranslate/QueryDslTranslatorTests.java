/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

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
        default -> Literal.NULL;
    };

    // A fixed query "now" so date-math bounds ("now-1d") resolve deterministically in tests.
    private static final long NOW = Instant.parse("2020-06-15T12:00:00Z").toEpochMilli();

    // The query configuration carrying that fixed now (and the locale used to case-fold a case_insensitive term).
    private static final Configuration CONFIG = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).now(Instant.ofEpochMilli(NOW)).build();

    // The schema field set (for multi_match expansion) — the names the BINDER resolves to a present attribute.
    private static final Set<String> FIELDS = Set.of("status", "tags", "bytes", "score", "@timestamp", "ts_nanos", "active", "body");

    private static Expression translate(org.elasticsearch.index.query.QueryBuilder qb) {
        return new QueryDslTranslator(BINDER, FIELDS, CONFIG).translate(qb).applied();
    }

    private static Expression translate(org.elasticsearch.index.query.QueryBuilder qb, Locale locale) {
        return new QueryDslTranslator(BINDER, FIELDS, new ConfigurationBuilder(CONFIG).locale(locale).build()).translate(qb).applied();
    }

    private static QueryDslTranslator.TranslationResult translateResult(org.elasticsearch.index.query.QueryBuilder qb) {
        return new QueryDslTranslator(BINDER, FIELDS, CONFIG).translate(qb);
    }

    private static QueryDslTranslator.TranslationResult translateResult(org.elasticsearch.index.query.QueryBuilder qb, Locale locale) {
        return new QueryDslTranslator(BINDER, FIELDS, new ConfigurationBuilder(CONFIG).locale(locale).build()).translate(qb);
    }

    /** An unsupported top-level construct is collected, not thrown; applied() is TRUE (no conjuncts applied). */
    public void testUnsupportedConstructCollected() {
        QueryDslTranslator.TranslationResult result = translateResult(QueryBuilders.wildcardQuery("tags", "a*"));
        assertFalse("a wholly unsupported filter is incomplete", result.isComplete());
        assertEquals(1, result.unsupported().size());
        assertEquals("wildcard", result.unsupported().get(0).construct());
        assertEquals(Literal.TRUE, result.applied());
    }

    /** A bool with a supported must and an unsupported must_not: the supported conjunct is applied, the unsupported one is collected. */
    public void testPartialBoolCollectsUnsupportedAndAppliesRest() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).mustNot(QueryBuilders.wildcardQuery("tags", "a*"))
        );
        assertFalse("incomplete: mustNot arm is unsupported", result.isComplete());
        assertEquals(1, result.unsupported().size());
        assertEquals("wildcard", result.unsupported().get(0).construct());
        // The supported term conjunct must still be present in applied.
        assertThat(result.applied(), instanceOf(MvContains.class));
    }

    /** A bool with two unsupported must clauses: both are collected. */
    public void testMultipleUnsupportedClausesAllCollected() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().must(QueryBuilders.wildcardQuery("tags", "a*")).must(QueryBuilders.fuzzyQuery("tags", "xyz"))
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
        var result = translateResult(QueryBuilders.wildcardQuery("status", "2*"));
        assertFalse(result.isComplete());
        assertEquals("wildcard", result.unsupported().get(0).construct());
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

    /** There is no predecessor for a double, so an exclusive two-bound range over one cannot be expressed exactly. */
    public void testExclusiveTwoBoundRangeOnDoubleIsUnsupported() {
        assertFalse(translateResult(QueryBuilders.rangeQuery("score").gt(1.5).lt(9.5)).isComplete());
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
     * A two-bound range on a KEYWORD field is a supported combination per the behavior table, and it takes the generic
     * both-bounds path rather than the integral or date one. An exclusive bound there has no exact predecessor to
     * normalize against (keyword is not whole-numbered), so it fails closed instead of silently shifting the interval.
     */
    public void testTwoBoundRangeOnKeyword() {
        Expression e = translate(QueryBuilders.rangeQuery("tags").gte("t1").lte("t3"));
        assertThat(e, instanceOf(MvInRange.class));
        MvInRange r = (MvInRange) e;
        assertEquals(DataType.KEYWORD, ((Literal) r.lower()).dataType());
        assertEquals(new BytesRef("t1"), ((Literal) r.lower()).value());
        assertEquals(new BytesRef("t3"), ((Literal) r.upper()).value());

        // An exclusive bound on a non-whole-numbered type cannot be normalized to an inclusive one -> collected.
        assertFalse(translateResult(QueryBuilders.rangeQuery("tags").gt("t1").lte("t3")).isComplete());
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
     * A lenient match on a type we cannot encode (ip/version/unsigned_long) is collected — it must not be silently mapped
     * to match-nothing, because the index would actually match. Regression guard for the lenient-swallows-everything bug.
     */
    public void testLenientMatchOnUnsupportedTypeDegrades() {
        assertFalse(translateResult(QueryBuilders.matchQuery("client_ip", "10.0.0.1").lenient(true)).isComplete());
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
        assertThat(result.unsupported().get(0).construct(), org.hamcrest.Matchers.containsString("minimum_should_match"));
        assertNotEquals("must arm must be applied, not abandoned to TRUE", Literal.TRUE, result.applied());
    }

    /**
     * B2: a non-required should arm that cannot be translated is silently omitted — it is not added to unsupported,
     * so isComplete() returns true and fail-closed mode does not throw a spurious 400.
     */
    public void testNonRequiredShouldFailureNotReported() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).should(QueryBuilders.wildcardQuery("tags", "a*"))
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
                    QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", 200)).should(QueryBuilders.wildcardQuery("tags", "a*"))
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
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("status", 200)).should(QueryBuilders.wildcardQuery("tags", "a*"))
        );
        assertFalse("wildcard arm failure is reported", result.isComplete());
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
        assertThat(result.unsupported().get(0).construct(), org.hamcrest.Matchers.containsString("minimum_should_match"));
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
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.termQuery("status", 200))
                        .should(QueryBuilders.wildcardQuery("tags", "a*"))
                )
        );
        assertFalse("wildcard arm failure is reported", result.isComplete());
        assertEquals("whole nested OR group dropped — pre-filter must over-fetch", Literal.TRUE, result.applied());
    }

    /**
     * must_not in a nested bool is all-or-nothing: if the arm cannot be fully translated, NOT is skipped entirely
     * rather than applying NOT(partial), which would over-exclude and under-fetch.
     * The must arm is still applied; the failed must_not arm produces no NOT expression in the result.
     * If must_not were incorrectly applied, applied() would be And(must_expr, Not(wildcard_expr)).
     * With the correct all-or-nothing policy, applied() is just the single must_expr (no And wrapper).
     */
    public void testNestedMustNotAllOrNothingSkipsPartialArm() {
        QueryDslTranslator.TranslationResult result = translateResult(
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("status", 200))
                        .mustNot(QueryBuilders.wildcardQuery("tags", "a*"))
                )
        );
        assertFalse("wildcard must_not arm failure is reported", result.isComplete());
        assertNotEquals("must arm is applied", Literal.TRUE, result.applied());
        // Only one conjunct (the must arm) — no And combining it with a NOT for the dropped must_not arm.
        assertThat("no And wrapping: must_not arm was not applied", result.applied(), not(instanceOf(And.class)));
    }
}
