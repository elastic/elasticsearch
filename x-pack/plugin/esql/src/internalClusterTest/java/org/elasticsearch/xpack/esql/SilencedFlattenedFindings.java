/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Registry of csv-spec tests whose failure under the {@code field_extract()} variant
 * ({@link CsvFlattenedKeywordIT}) is a known limitation of the function or of an upstream
 * grammar/engine constraint, rather than a defect of the rewriter or of the strategy that
 * converts {@code keyword} fields to {@code flattened}.
 *
 * <p>Each entry is tagged with a {@link Finding} that names a single root cause and references
 * a tracking issue so the underlying behavior can be discovered, prioritised, and ultimately
 * fixed independently of the test infrastructure. The test class consults this registry from
 * its query-transform hook and, when the running test is silenced, throws an
 * {@link org.junit.AssumptionViolatedException} with the finding's identifier and description
 * &mdash; the JUnit XML {@code <skipped>} element therefore explains <em>why</em> the test was
 * skipped, while the test report no longer surfaces the failure as a regression that obscures
 * other progress.
 *
 * <h2>How to re-enable a silenced test</h2>
 *
 * <p>Silencing is purely advisory: the underlying limitations have not been fixed, and a
 * developer working on one of the {@link Finding} categories must be able to see the test fail
 * again locally so they can iterate against a real failure signal. Two system properties on
 * the test JVM control un-silencing:
 *
 * <ul>
 *   <li>{@code -Dtests.flattened.unsilence=<id>[,<id>...]} &mdash; un-silences the listed
 *       tests, identified as {@code <fileName>.<testName>} (the same form the
 *       {@code SILENCED} map below uses, e.g. {@code string.lessThanMultivalue}). Comma-
 *       separated, whitespace ignored.</li>
 *   <li>{@code -Dtests.flattened.unsilence_all=true} &mdash; un-silences every entry in the
 *       registry. Useful for confirming that a fix has resolved a whole {@link Finding}
 *       category, or for a one-shot full-run audit before moving entries between categories.</li>
 * </ul>
 *
 * <p>An un-silenced test runs through the variant's normal rewrite path and either passes (if
 * a fix has landed) or fails again with its original symptom. Either outcome lets the
 * developer iterate; whichever applies, the entry can then be deleted from the {@code
 * SILENCED} map (when the underlying issue is fixed) or moved to a different {@link Finding}
 * (when investigation reveals a different root cause).
 *
 * <h2>How to add an entry</h2>
 *
 * <p>Pick the {@link Finding} that matches the failure's root cause from the log analysis,
 * then add a single {@code SILENCED.put(...)} line for the failing csv-spec test. The
 * registry is intentionally a flat map of {@code <fileName>.<testName>} keys so a CI run can
 * trivially diff its before/after silencing surface; do not add globs or wildcards.
 */
public final class SilencedFlattenedFindings {

    /**
     * Coarse-grained category that names a single root cause behind a cluster of test
     * failures. Each value carries a tracking-issue placeholder &mdash; replace the
     * {@code TBD-*} string with the real issue id once the ticket is filed &mdash; and a
     * one-sentence description that surfaces in the {@link org.junit.AssumptionViolatedException}
     * message so the JUnit report explains the silence to whoever reads it next.
     */
    public enum Finding {
        /**
         * Multi-value flattened input causes {@code field_extract(...)} to return {@code null}
         * and to emit a runtime warning ("evaluation of [...] failed, treating result as
         * null"). The conversion strategy turns each keyword value list into a single flattened
         * object {@code {"v": [...]}}, and {@code field_extract(field, "v")} unwraps the
         * inner list as a single multi-value &mdash; but {@code field_extract} is single-
         * valued by design (per the function's signature), so the runtime falls back to null
         * and the test sees either a missing value or an unexpected warning that the spec did
         * not write.
         */
        FIELD_EXTRACT_MULTIVALUE_NULL(
            "TBD-F1",
            "field_extract returns null and emits a runtime warning on multi-value flattened input; the function is single-valued by design"
        ),

        /**
         * Bare-attribute-only commands ({@code MV_EXPAND}, {@code KEEP}, {@code DROP},
         * {@code RENAME}) operate on the flattened wrapper {@code {"v": [...]}} as a single
         * column rather than on the inner values. The rewriter cannot substitute
         * {@code field_extract(...)} in these grammar slots (the parser only accepts a bare
         * identifier), so the wrapped value flows through unmodified and downstream commands
         * see the wrapper instead of the original list.
         */
        BARE_ATTRIBUTE_COMMAND_ON_FLATTENED(
            "TBD-F2",
            "MV_EXPAND/KEEP/RENAME/DROP operate on the flattened wrapper {\"v\": [...]} instead of the inner values "
                + "because their grammar accepts only an attribute, not field_extract(...)"
        ),

        /**
         * The {@code MATCH}/{@code MatchPhrase}/{@code KQL}/{@code KNN} function family and
         * the {@code :} match-operator reject {@code field_extract(...)} as their first
         * argument. ES|QL's verifier requires those positions to bind directly to a field
         * from an index mapping ("function cannot operate on [...], which is not a field
         * from an index mapping"), and the function form cannot be made to accept a synthetic
         * expression even though the operator form would otherwise. The rewriter has no
         * legal substitute on the LHS that preserves test semantics, so the variant cannot
         * exercise these tests at all.
         */
        MATCH_FAMILY_REJECTS_FIELD_EXTRACT(
            "TBD-A2",
            "MATCH/MatchPhrase/KQL/KNN function family and the `:` match-operator reject field_extract(...) "
                + "as an argument; verifier requires a direct mapped-field reference"
        ),

        /**
         * {@code ENRICH ... ON} accepts only the standard mapped-field types
         * ({@code keyword}, {@code text}, {@code ip}, {@code long}, {@code integer},
         * {@code float}, {@code double}, {@code datetime}). A {@code flattened} match field
         * fails verification with "Unsupported type [flattened] for enrich matching field"
         * before any rewrite can run.
         */
        ENRICH_ON_REJECTS_FLATTENED(
            "TBD-A4",
            "ENRICH ... ON does not accept flattened match fields; the verifier rejects them with "
                + "'Unsupported type [flattened] for enrich matching field'"
        ),

        /**
         * {@code LOOKUP JOIN} cannot reconcile a {@code flattened}/{@code keyword} type
         * mismatch when the join key is converted to flattened by an upstream
         * {@code EVAL field = field_extract(...)} on one side of the join. The rewriter
         * already preserves bare-name join keys as {@code keyword} via the dataset-level
         * scope exclusion, but a join key that comes from an EVAL on a converted column
         * still leaks the {@code flattened} type through and the analyzer rejects it with
         * "JOIN left field [...] of type [FLATTENED] is incompatible with right field
         * [...] of type [KEYWORD]".
         */
        LOOKUP_JOIN_TYPE_MISMATCH(
            "TBD-A5",
            "LOOKUP JOIN rejects flattened/keyword type mismatch when the join key is converted to flattened "
                + "by an upstream EVAL on one side of the join"
        );

        private final String issueRef;
        private final String description;

        Finding(String issueRef, String description) {
            this.issueRef = issueRef;
            this.description = description;
        }

        /** Tracking issue identifier (currently a {@code TBD-*} placeholder until the real issue is filed). */
        public String issueRef() {
            return issueRef;
        }

        /** Single-sentence description of the root cause; surfaces in the assumption-failure message. */
        public String description() {
            return description;
        }
    }

    /**
     * System property listing csv-spec test ids ({@code <fileName>.<testName>}) to un-silence
     * for the running JVM. The accepted form is comma-separated, whitespace-tolerant; an
     * empty or unset value silences everything in {@link #SILENCED}. See class-level Javadoc
     * for the workflow this property is built for.
     */
    public static final String UNSILENCE_TESTS_PROPERTY = "tests.flattened.unsilence";

    /**
     * System property that un-silences every entry in {@link #SILENCED} when set to
     * {@code true} (case-insensitive). Used to audit the silencing surface end-to-end or to
     * confirm that a fix has resolved a whole {@link Finding} category.
     */
    public static final String UNSILENCE_ALL_PROPERTY = "tests.flattened.unsilence_all";

    private static final Map<String, Finding> SILENCED;

    static {
        Map<String, Finding> m = new HashMap<>();

        m.put("date.dateDiffTestWarnings", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("dedup.dedupMultivalueGroupKey", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("dissect.multivalueInput", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("docs.docsRound", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("docs.docsSortDesc", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("docs.docsSortTie", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("drop.whereWithEvalGeneratedValue_DropHeight", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("enrich.fieldsInOtherIndicesBug", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("folding.keyword_GreaterThan_MultiValueConstant", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("fork.forkAfterMvExpand", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("fork.forkBeforeMvExpand", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("fork.forkBranchWithMvExpand", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("grok.multivalueInput", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("grok.optionalMatchMv", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("inlinestats.byMultivaluedMvExpand", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("inlinestats.byMvExpand", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("inlinestats.groupingFilterIsAlwaysFalse", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("inlinestats.groupingFilterIsAlwaysTrue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("inlinestats.groupingFilterSometimesMatches", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("inlinestats.overridingExpressionGroupings", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("inlinestats.overridingGroupings", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("ints.convertStringToBaseIndexGroup3", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("ints.convertStringToBaseIndexGroup5", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("ints.convertStringToBaseIndexGroup7", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("ip.cdirMatchEqualsInsOrs", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("ip.cidrMatchEqualsInsOrsIPs", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("keep.projectMultiValueKeywords", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("keep.whereWithEvalGeneratedValue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("kql-function.kqlWithBoostOption", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("kql-function.kqlWithCaseInsensitiveOption", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("kql-function.kqlWithDefaultFieldOption", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("kql-function.kqlWithMultipleOptions", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("limit.limitByMultivalueGroupKey", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("limit.sortLimitByMultipleMultivalueGroupKeys", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("limit.sortLimitByMultivalueGroupKey", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("limit.sortLimitByMultivalueGroupKeyFiltered", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("limit.sortLimitByMultivalueGroupKeyWithIntersection", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("mv_difference.mvDifference_from_keyword", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("rename.renameIntertwinedWithSort", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("spatial.simpleLoad", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("stats.filterOrdinalValues", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("stats.groupingFilterIsAlwaysFalse", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("stats.groupingFilterIsAlwaysTrue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("stats.groupingFilterSometimesMatches", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("stats.shadowingTheGroup", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("stats_first_last.Test passing foldables in the sort field", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("stats_first_last.Test passing int foldables in the sort field", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.concatOfText", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.containsWarnings", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.convertFromString", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.equalToMultivalue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.equalToOrEqualToMultivalue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.greaterThanMultivalue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.in", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.inMultivalue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.lengthOfMvPushed", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.lengthOfText", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.lessThanMultivalue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.locateWarnings", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.mvAppendStrings", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.mvAppendStringsWhere", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.mvKeywordEqualsMultiValueConstant", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.mvKeywordNotEqualsMultiValueConstant", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.mvKeyword_IN_MultiValueConstant", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.mvSortEmp", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.mvZipEmp", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.notEqualToMultivalue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.notGreaterThanMultivalue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.notLessThanMultivalue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.repeatNegative", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.replaceWarnings", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.reverseMultiValue", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.spaceNegative", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.startsWithText", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("string.substringOfText", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("topN.complexMultiSortingFields_SameFieldAscAndDesc", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("topN.sortingOnNumbersFromStrings", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("views.airportsLookupJoin", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("where-like.multiValueLike", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);
        m.put("where-like.multiValueRLike", Finding.FIELD_EXTRACT_MULTIVALUE_NULL);

        m.put("mv_expand.doubleLimitWithSort", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.doubleLimit_expandLimitGreaterThanAvailable", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.doubleLimit_expandLimitLowerThanAvailable", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.doubleSort_OnDifferentThan_MvExpandedFields", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.expandAfterSort1", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.expandAfterSort2", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.expandWithMultiSort", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.filterAfterMvExpandOnExpandedAndUnexpandedFieldsFromSource", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.filterAfterMvExpandOnUnexpandedFieldFromSource", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.filterMvExpanded", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.keepStarMvExpand", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.returnMultiValueFieldsWithMultivalueTypeConversions", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.returnMultiValueFieldsWithoutExpansion", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.sortBeforeAndAfterMvExpand", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);
        m.put("mv_expand.tripleLimit_WithWhere_InBetween_MvExpand_And_Limit", Finding.BARE_ATTRIBUTE_COMMAND_ON_FLATTENED);

        m.put("inlinestats.inlineStatsAfterPruningAggregate6", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("knn-function.testKnnWithSemanticTextMultiValueField", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("lookup-join.whereFalseBeforeLookupJoinWithMatchOnEmployees", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-function.matchMultivaluedField", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-function.matchWithFunctionPushedToLucene", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-function.testMatchAndQueryStringFunctions", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-function.testMatchInStatsWithGroupingBy", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-function.testMatchWithSemanticTextAndKeyword", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-function.testMatchWithSemanticTextMultiValueField", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-function.testMatchWithSemanticTextWithEvalsAndOtherFunctionsAndStats", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-function.testMultiValuedFieldWithConjunction", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-operator.matchMultivaluedField", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-operator.matchWithFunctionPushedToLucene", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-operator.matchWithMultivaluedKeywordField", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-operator.testMatchAndQueryStringFunctions", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-operator.testMatchWithSemanticTextAndKeyword", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-operator.testMatchWithSemanticTextMultiValueField", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-operator.testMatchWithSemanticTextWithEvalsAndOtherFunctionsAndStats", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-operator.testMultiValuedFieldWithConjunction", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-phrase-function.matchPhraseMultivaluedField", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-phrase-function.matchPhraseWithFunctionPushedToLucene", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-phrase-function.testMatchPhraseAndQueryStringFunctions", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);
        m.put("match-phrase-function.testMultiValuedFieldWithConjunction", Finding.MATCH_FAMILY_REJECTS_FIELD_EXTRACT);

        m.put("enrich.nullInput", Finding.ENRICH_ON_REJECTS_FLATTENED);
        m.put("enrich.spatialEnrichmentKeywordMatch", Finding.ENRICH_ON_REJECTS_FLATTENED);
        m.put("inlinestats.afterEnrich", Finding.ENRICH_ON_REJECTS_FLATTENED);
        m.put("inlinestats.beforeAndAfterEnrich", Finding.ENRICH_ON_REJECTS_FLATTENED);
        m.put("inlinestats.beforeEnrich", Finding.ENRICH_ON_REJECTS_FLATTENED);
        m.put("lookup-join.joinMaskingRegex", Finding.ENRICH_ON_REJECTS_FLATTENED);

        m.put("lookup-join.dropAgainWithWildcardAfterEval2", Finding.LOOKUP_JOIN_TYPE_MISMATCH);
        m.put("lookup-join.lookupIPAndMessageFromIndexChainedRenameKeep", Finding.LOOKUP_JOIN_TYPE_MISMATCH);

        SILENCED = Map.copyOf(m);
    }

    private SilencedFlattenedFindings() {}

    /**
     * Returns the {@link Finding} that the registry classifies {@code testId} under, or
     * {@link Optional#empty()} when {@code testId} is not silenced. The system properties
     * documented at the class level (the un-silence list and the un-silence-all toggle)
     * are honoured here: an entry that is in {@link #SILENCED} but listed for un-silencing
     * is reported as not-silenced from this call so the test runs through normally.
     *
     * @param testId  csv-spec test identifier in the form {@code <fileName>.<testName>}, e.g.
     *                {@code string.lessThanMultivalue}; the leading {@code csv-spec:} prefix
     *                used in log lines should be stripped before calling
     */
    public static Optional<Finding> findingFor(String testId) {
        if (Boolean.parseBoolean(System.getProperty(UNSILENCE_ALL_PROPERTY, "false"))) {
            return Optional.empty();
        }
        if (parseUnsilenceList(System.getProperty(UNSILENCE_TESTS_PROPERTY, "")).contains(testId)) {
            return Optional.empty();
        }
        return Optional.ofNullable(SILENCED.get(testId));
    }

    /**
     * Returns an unmodifiable view of the full silencing registry, exposed only for the
     * accompanying tests (which assert on the registry's invariants &mdash; non-empty
     * description, recognised finding values, and so on).
     */
    static Map<String, Finding> registryView() {
        return Collections.unmodifiableMap(SILENCED);
    }

    private static Set<String> parseUnsilenceList(String value) {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(value.split(",")).map(s -> s.trim()).filter(s -> s.isEmpty() == false).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Builds the message that {@link CsvFlattenedKeywordIT} attaches to its
     * {@link org.junit.AssumptionViolatedException} when {@code testId} is silenced. Surfaces
     * both the issue identifier and the description so the JUnit XML {@code <skipped>}
     * element makes the silence self-explanatory in CI tooling that does not link back to
     * this class.
     */
    public static String assumptionMessage(String testId, Finding finding) {
        return String.format(
            Locale.ROOT,
            "silenced known field_extract() limitation [%s / %s]: %s",
            testId,
            finding.issueRef(),
            finding.description()
        );
    }
}
