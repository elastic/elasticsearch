/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.GenerativeFeature;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;

import java.util.List;
import java.util.Set;

/**
 * Tests the predicates that classify known generative-test failures as allowed failures,
 * and the schema-propagation helpers used by the generative pipeline.
 */
public class GenerativeRestTestTests extends ESTestCase {

    public void testFullTextAfterSubqueryMatchesLimitInsideSubquery() {
        String query = "FROM books, (FROM books | LIMIT 1) | WHERE match(title, \"quick\")";
        String error = "verification_exception: line 1:13: [MATCH] function cannot be used after LIMIT";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesMultiSourceSubqueryMessage() {
        String query = "FROM all_types, (FROM colors | MV_EXPAND hex_code) | WHERE match_phrase(hex_code, \"world search\")";
        String error = "verification_exception: line 1:973: [MATCH_PHRASE] function cannot be used after "
            + "all_types,(from colors | mv_expand hex_code)";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesSubqueryFirstMultiSourceMessage() {
        String query = "FROM (FROM message_types | KEEP type | DROP type),no_mapping_sample_data,service_owners "
            + "| WHERE match_phrase(service_id, \"fox world\")";
        String error = "verification_exception: line 1:91: [MATCH_PHRASE] function cannot be used after "
            + "(from message_types | keep type | drop type),no_mapping_sample_data,service_owners";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesTruncatedUnionAllSourceMessage() {
        String query = "from (from all_types_short_as_long | enrich languages_policy on wildcard "
            + "| dissect language_name \"%{a} %{b}\"),countries_bbox,(from dense_vector_arithmetic | keep id) "
            + "| where match_phrase(registered_domain, \"test data\")";
        // The UnionAll source text in the verifier message is truncated to Node.TO_STRING_MAX_WIDTH chars + "...",
        // so it can be cut off mid-branch, before the comma separating the union branches.
        String error = "verification_exception: line 1:1800: [MATCH_PHRASE] function cannot be used after "
            + "(from all_types_short_as_long | enrich languages_policy on wildcard | dissect language_name \"%{HkOuTBPphONE} %...";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryRequiresKnownErrorShape() {
        String query = "FROM all_types, (FROM colors | MV_EXPAND hex_code) | WHERE match_phrase(hex_code, \"world search\")";
        String error = "verification_exception: line 1:973: [MATCH_PHRASE] function cannot be used after field "
            + "with details (from an unrelated diagnostic)";

        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesLookupMessage() {
        String query = "FROM logs, (FROM messages | LOOKUP JOIN message_types_lookup ON message) | WHERE qstr(\"text:hello\")";
        String error = "verification_exception: line 1:34: [QSTR] function cannot be used after LOOKUP";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesDedupMessage() {
        String query = "FROM employees, (FROM employees | DEDUP first_name) | WHERE first_name : \"world\"";
        String error = "verification_exception: line 1:18: [:] operator cannot be used after DEDUP";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryRequiresSubqueryInQuery() {
        String query = "FROM logs | LOOKUP JOIN message_types_lookup ON message | WHERE qstr(\"text:hello\")";
        String error = "verification_exception: line 1:34: [QSTR] function cannot be used after LOOKUP";

        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testMatchOptionsOnNonIndexMappedFieldIsAllowed() {
        String error = "Options are not supported for [MATCH] function call on non-index-mapped field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchOptionsOnIndexMappedFieldIsNotAllowed() {
        String error = "Options are not supported for [MATCH] function call on non-index-mapped field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), true));

        assertFalse(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchOptionsOnNonIndexMappedNonTextFieldIsAllowed() {
        String error = "Options are not supported for [MATCH] function call on non-index-mapped, non-TEXT field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchUnsupportedOptionOnNonIndexMappedNonTextFieldIsAllowed() {
        String error = "[operator] option is not supported for [MATCH] on non-index-mapped, non-TEXT field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchUnsupportedOptionOnIndexMappedNonTextFieldIsNotAllowed() {
        String error = "[operator] option is not supported for [MATCH] on non-index-mapped, non-TEXT field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), true));

        assertFalse(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchPhraseOptionsOnNonIndexMappedFieldIsAllowed() {
        String error = "Options are not supported for [MATCH_PHRASE] function call on non-index-mapped field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchPhraseOptionsOnNonIndexMappedNonTextFieldIsAllowed() {
        String error = "Options are not supported for [MATCH_PHRASE] function call on non-index-mapped, non-TEXT field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchPhraseOptionsOnIndexMappedFieldIsNotAllowed() {
        String error = "Options are not supported for [MATCH_PHRASE] function call on non-index-mapped field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), true));

        assertFalse(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testFullTextAfterSubqueryMatchesSortMessage() {
        String query = "FROM (FROM languages | SORT language_name | LIMIT 5), alerts | WHERE kql(\"language_name: English\")";
        String error = "verification_exception: line 1:36: [KQL] function cannot be used after SORT";
        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesSortAndLimitMessage() {
        String query = "FROM (FROM languages | SORT language_name | LIMIT 5), alerts | WHERE kql(\"language_name: English\")";
        String error = "verification_exception: line 1:36: [KQL] function cannot be used after SORT and LIMIT";
        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
        assertTrue(
            GenerativeRestTest.isFullTextAfterSubqueryInFromBug(
                "verification_exception: line 1:80: [:] operator cannot be used after SORT and LIMIT",
                query
            )
        );
        assertTrue(
            GenerativeRestTest.isFullTextAfterSubqueryInFromBug(
                "verification_exception: line 1:80: [MATCH] function cannot be used after SORT and LIMIT",
                query
            )
        );
        assertTrue(
            GenerativeRestTest.isFullTextAfterSubqueryInFromBug(
                "verification_exception: line 1:80: [MATCH_PHRASE] function cannot be used after SORT and LIMIT",
                query
            )
        );
    }

    public void testFullTextAfterSortRequiresSubqueryInQuery() {
        String query = "FROM languages | SORT language_name | LIMIT 5 | WHERE kql(\"language_name: English\")";
        String error = "verification_exception: line 1:36: [KQL] function cannot be used after SORT";
        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testQstrAfterSubqueryRenameIsTolerated() {
        // Real GenerativeIT {feature:SUBQUERIES} failure: the subquery ends with a RENAME, which the flattened plan
        // pushes ahead of the outer QSTR. RENAME is not one of FROM/WHERE/SORT, so QSTR is rejected.
        String query = "from (from employees_gender_text | limit 808 | rename `os.version` AS v) | where qstr(\"os.name:world\")";
        String error = "verification_exception: line 1:481: [QSTR] function cannot be used after RENAME";
        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testKqlAfterSubqueryReportsFromIsTolerated() {
        // KQL/QSTR after a multi-source FROM whose subquery branch has extra commands. After opt the
        // Filter can sit above a node whose source is the FROM command, so the verifier says "after FROM".
        String query = "from (from hash_algorithms | dissect algorithm \"%{algorithm} %{algorithm}\" "
            + "| ip_location geo = algorithm | DEDUP),k8s-downs* | where kql(\"world\")";
        String error = "verification_exception: line 1:64: [KQL] function cannot be used after FROM";
        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
        String errorWithSubquerySource = "verification_exception: line 1:64: [KQL] function cannot be used after "
            + "FROM (from hash_algorithms | dissect algorithm \"%{algorithm} %{algorithm}\" "
            + "| ip_location geo = algorithm | DEDUP),k8s-downs*";
        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(errorWithSubquerySource, query));
    }

    public void testKqlAfterFromRequiresSubqueryInQuery() {
        String query = "FROM languages | WHERE kql(\"language_name: English\")";
        String error = "verification_exception: line 1:18: [KQL] function cannot be used after FROM";
        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testKqlAfterSubqueryEvalIsTolerated() {
        String query = "FROM books, (FROM books | EVAL title2 = concat(title, \"x\")) | WHERE kql(\"title2: world\")";
        String error = "verification_exception: line 1:64: [KQL] function cannot be used after EVAL";
        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testQstrAfterRenameRequiresSubqueryInQuery() {
        String query = "FROM employees | RENAME first_name AS fn | WHERE qstr(\"fn:world\")";
        String error = "verification_exception: line 1:44: [QSTR] function cannot be used after RENAME";
        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testMatchAfterSubqueryRenameIsNotToleratedByQstrKqlBranch() {
        // The broadened branch is scoped to QSTR/KQL only: RENAME is legal before MATCH/MatchPhrase, so a MATCH error
        // naming RENAME is not a known bug and must not be tolerated here.
        String query = "FROM books, (FROM books | RENAME title AS title2) | WHERE match(title2, \"world\")";
        String error = "verification_exception: line 1:64: [MATCH] function cannot be used after RENAME";
        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFillNullUnmappedFieldsPatternIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from colors_unmapped,date_nanos_union_types | fillnull DEFAULT ON _unmapped_*";
        String error = "verification_exception: Found 1 problem\nline 1:98: No matches found for pattern [_unmapped_*]";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(error, query));
    }

    public void testFillNullUnmappedFieldStarPatternIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from network_direction_networks | fillnull DEFAULT ON _unmapped_field*";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, "_unmapped_field*"), query));
    }

    public void testFillNullShorterPrefixesThatCoverUnmappedFieldsAreAllowed() {
        String unm = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON _unm*";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(unm, "_unm*"), unm));
        String un = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON _un*";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(un, "_un*"), un));
        String star = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON _*";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(star, "_*"), star));
    }

    public void testFillNullQuotedPrefixStarCoversUnmappedFields() {
        String pattern = "`_unmapped_`*";
        String query = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON " + pattern;
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, pattern), query));
    }

    public void testFillNullPatternInsideSubqueryIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";FROM (FROM colors | fillnull DEFAULT ON _un*)";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, "_un*"), query));
    }

    public void testFillNullUnrelatedPatternIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";FROM test | FILLNULL 0 ON no_such_prefix_*";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, "no_such_prefix_*"), query));
    }

    public void testFillNullPatternThatDoesNotCoverUnmappedFieldsIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON _unmapped_other*";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, "_unmapped_other*"), query));
    }

    public void testFillNullCoveringPatternWithoutLoadAllIsNotAllowed() {
        String query = "FROM colors | fillnull DEFAULT ON _unmapped_*";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, "_unmapped_*"), query));
        String load = "SET unmapped_fields=\"load\";from colors | fillnull DEFAULT ON _unmapped_*";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(load, "_unmapped_*"), load));
    }

    public void testLaterSetOverridesLoadAllIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";SET unmapped_fields=\"load\";from colors | fillnull DEFAULT ON _unmapped_*";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, "_unmapped_*"), query));
        String spaced = "SET unmapped_fields=\"load_all\";SET  `unmapped_fields`=\"load\";from colors | fillnull DEFAULT ON _unmapped_*";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(spaced, "_unmapped_*"), spaced));
    }

    public void testResponseExceptionBodyIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON _un*";
        int column = query.indexOf("_un*") + 1;
        String reason = "reason: \"Found 1 problem\\nline 1:" + column + ": No matches found for pattern [_un*]\"";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(reason, query));
        String stack = "line 1:"
            + column
            + ": No matches found for pattern [_un*]\\r\\n\\tat org.elasticsearch.xpack.esql.analysis.Analyzer.verify";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(stack, query));
        String two = "reason: \"Found 2 problems\\nline 1:"
            + column
            + ": No matches found for pattern [_un*]\\nline 1:10: Unknown column [foo]\"";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(two, query));
    }

    public void testFillNullInForkBranchIsAllowedAndDropBranchIsNot() {
        String pattern = "_unmapped_*";
        String query = "SET unmapped_fields=\"load_all\";FROM t | FORK (FILLNULL DEFAULT ON " + pattern + ") (DROP " + pattern + ")";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, pattern), query));
        int dropPattern = query.lastIndexOf(pattern);
        String dropError = "line 1:" + (dropPattern + 1) + ": No matches found for pattern [" + pattern + "]";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(dropError, query));
    }

    public void testUnmappedFieldsPatternWithoutFillNullIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from colors | drop _unmapped_*";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, "_unmapped_*"), query));
    }

    public void testSamePatternOnDropIsNotAllowedWhenFillNullAlsoUsesIt() {
        String pattern = "_unmapped_*";
        String query = "SET unmapped_fields=\"load_all\";from colors | drop " + pattern + " | fillnull DEFAULT ON " + pattern;
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(noMatches(query, pattern), query));
        int fillNullPattern = query.lastIndexOf(pattern);
        String error = "line 1:" + (fillNullPattern + 1) + ": No matches found for pattern [" + pattern + "]";
        assertTrue(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(error, query));
    }

    public void testFillNullWithAnotherProblemIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON _un*";
        String error = "Found 2 problems\n" + noMatches(query, "_un*") + "\nline 1:10: Unknown column [foo]";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(error, query));
    }

    public void testFillNullErrorForADifferentPatternIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON _unmapped_*";
        String error = "line 1:" + (query.indexOf("_unmapped_*") + 1) + ": No matches found for pattern [no_such_prefix_*]";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(error, query));
    }

    public void testFillNullPatternWithAProblemWithoutALocationIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from colors | fillnull DEFAULT ON _un*";
        String error = "Found 2 problems\n" + noMatches(query, "_un*") + "\nline -1:-1: Unknown column [foo]";
        assertFalse(GenerativeRestTest.isFillNullUnmappedFieldsPatternBug(error, query));
    }

    private static String noMatches(String query, String pattern) {
        return "line 1:" + (query.indexOf(pattern) + 1) + ": No matches found for pattern [" + pattern + "]";
    }

    private static final String NAMED_AFTER_KEEP_QUERY = "SET unmapped_fields=\"load_all\";SET approximation=null;"
        + "from distances*,k8s_nonexistent | enrich languages_policy on cluster | keep event_city, *p, `event_city`, event_city_boundary "
        + "| rename network.eth0.up AS FeXjbRwDdext "
        + "| drop `event_city`, `_unmapped_fields`, FeXjbRwDdext, `event_city_boundary`, `@timestamp`";

    private static final List<Column> LOAD_ALL_SCHEMA = List.of(
        new Column("event_city", "keyword", List.of()),
        new Column("_unmapped_fields", DataType.SOURCE.outputType(), List.of())
    );

    public void testUnmappedFieldsColumnNamedAfterKeepIsAllowed() {
        String error = "verification_exception: Found 1 problem\nline 1:243: Unknown column [_unmapped_fields]";
        assertTrue(allowsUnknownUnmappedFields(error, NAMED_AFTER_KEEP_QUERY));
    }

    public void testUnmappedFieldsColumnNamedAfterKeepInCborBodyIsAllowed() {
        String error = "eerror jroot_cause dtypevverification_exceptionfreasonx=Found 1 problem\n"
            + "line 1:243: Unknown column [_unmapped_fields]kstack_tracey?org.elasticsearch.xpack.esql.VerificationException: "
            + "Found 1 problem\nline 1:243: Unknown column [_unmapped_fields]\n"
            + "\tat org.elasticsearch.xpack.esql.analysis.Analyzer.verify(Analyzer.java:393)\n"
            + "\tat org.elasticsearch.xpack.esql.analysis.Analyzer.analyze(Analyzer.java:384)";
        assertTrue(allowsUnknownUnmappedFields(error, NAMED_AFTER_KEEP_QUERY));
    }

    public void testUnmappedFieldsColumnNamedWithoutTheLoadAllColumnInTheSchemaIsNotAllowed() {
        String error = "line 1:243: Unknown column [_unmapped_fields]";
        assertFalse(GenerativeRestTest.isUnknownNamedUnmappedFieldsColumn(error, NAMED_AFTER_KEEP_QUERY, List.of()));
        List<Column> renamed = List.of(new Column("_unmapped_fields", "integer", List.of()));
        assertFalse(GenerativeRestTest.isUnknownNamedUnmappedFieldsColumn(error, NAMED_AFTER_KEEP_QUERY, renamed));
    }

    public void testUnquotedUnmappedFieldsColumnNamedAfterKeepIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";FROM employees | DROP *name, gender, salary*, *h*, j*, lang* | KEEP e* "
            + "| DROP _unmapped_fields";
        assertTrue(allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
    }

    public void testUnmappedFieldsColumnKeptByNameAfterKeepIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_*, first_name* | keep emp_no, `_unmapped_fields`";
        assertTrue(allowsUnknownUnmappedFields(unknownColumn(query, "`_unmapped_fields`"), query));
    }

    public void testUnmappedFieldsColumnNamedAfterCoveringDropIsAllowed() {
        for (String drop : List.of("drop *s", "drop `name`, _unmapped_fiel*", "drop `_unmapped_`*")) {
            String query = "SET unmapped_fields=\"load_all\";from apps* | " + drop + " | drop _unmapped_fields";
            assertTrue(query, allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
        }
    }

    public void testUnmappedFieldsColumnNamedAfterItWasRenamedAwayIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from dense_vector_text "
            + "| rename `text_field` AS `_unmapped_fields`, `_unmapped_fields` AS CfwuVptDvxZY "
            + "| rename _unmapped_fields AS `aTLYegNM`, CfwuVptDvxZY AS KfkUczACS";
        String error = "verification_exception: Found 1 problem\nline 1:144: Unknown column [_unmapped_fields]";
        assertTrue(allowsUnknownUnmappedFields(error, query));
        for (String rename : List.of(
            "rename emp_no AS _unmapped_fields, _unmapped_fields AS x",
            "rename emp_no AS a, _unmapped_fields AS b | rename a AS c"
        )) {
            String chained = "SET unmapped_fields=\"load_all\";from employees | " + rename + " | drop _unmapped_fields";
            assertTrue(chained, allowsUnknownUnmappedFields(unknownColumn(chained, "_unmapped_fields"), chained));
        }
    }

    public void testUnmappedFieldsColumnNamedAfterARenameThatDoesNotRemoveItIsNotAllowed() {
        for (String rename : List.of(
            "rename emp_no AS _unmapped_fields",
            "rename _unmapped_fields AS _unmapped_fields",
            "rename `_unmapped_fields` AS `_unmapped_fields`"
        )) {
            String query = "SET unmapped_fields=\"load_all\";from employees | " + rename + " | drop _unmapped_fields";
            assertFalse(query, allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
        }
    }

    public void testUnmappedFieldsColumnRecreatedEarlierInTheSameRenameIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_* "
            + "| rename emp_no AS _unmapped_fields, _unmapped_fields AS x";
        assertFalse(allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
    }

    public void testUnmappedFieldsColumnNamedWithSuggestionIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_* | where `_unmapped_fields` is null";
        assertTrue(allowsUnknownUnmappedFields(unknownColumn(query, "`_unmapped_fields`") + ", did you mean [emp_no]?", query));
    }

    public void testUnmappedFieldsColumnNamedInAFunctionAfterKeepIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_no* "
            + "| eval l = length(to_string(emp_no)) + length(_unmapped_fields)";
        assertTrue(allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
    }

    public void testUnmappedFieldsColumnNamedAfterKeepBeforeForkIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_* | fork (where true) (where true) "
            + "| drop _unmapped_fields";
        assertTrue(allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
    }

    public void testUnmappedFieldsColumnNamedInAForkBranchAfterKeepIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_* | fork (where _unmapped_fields is null) (where true)";
        assertTrue(allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
    }

    public void testUnmappedFieldsColumnNamedInAForkBranchAfterASiblingKeepIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | fork (where true | keep emp_no) (where _unmapped_fields is null)";
        assertFalse(allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
        String nested = "SET unmapped_fields=\"load_all\";from employees | fork (where abs(emp_no) > 0 | keep emp_no) (where true) "
            + "| drop _unmapped_fields";
        assertFalse(allowsUnknownUnmappedFields(unknownColumn(nested, "_unmapped_fields"), nested));
    }

    public void testUnmappedFieldsColumnNamedWithoutAnEarlierRemovalIsNotAllowed() {
        for (String pipeline : List.of(
            "from employees | drop _unmapped_fields",
            "from employees | where _unmapped_fields is null",
            "from employees | where length(_unmapped_fields) > 0",
            "from employees | eval y = _unmapped_fields",
            "from employees | keep * | drop _unmapped_fields",
            "from employees | keep emp_no, _unmapped_f* | drop _unmapped_fields",
            "from employees | keep `_unmapped_fields`, emp_no | drop _unmapped_fields",
            "from employees | drop emp_*, `first_name` | drop _unmapped_fields",
            "from employees | eval x = \"| keep emp_no\" | drop _unmapped_fields",
            "from employees | fork (where true | drop _unmapped_fi*) (where true) | drop _unmapped_fields",
            "from employees | fork (where true | keep emp_no) (where true) | drop _unmapped_fields"
        )) {
            String query = "SET unmapped_fields=\"load_all\";" + pipeline;
            assertFalse(query, allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
        }
    }

    public void testUnmappedFieldsColumnNamedTwiceWithOnlyOneEarlierRemovalIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | eval y = _unmapped_fields | keep emp_no | drop _unmapped_fields";
        String first = "line 1:" + (query.indexOf("_unmapped_fields") + 1) + ": Unknown column [_unmapped_fields]";
        String error = "Found 2 problems\n" + first + "\n" + unknownColumn(query, "_unmapped_fields");
        assertFalse(allowsUnknownUnmappedFields(error, query));
    }

    public void testUnmappedFieldsColumnNamedWithoutLoadAllIsNotAllowed() {
        for (String prefix : List.of("", "SET unmapped_fields=\"load\";", "SET unmapped_fields=\"nullify\";")) {
            String query = prefix + "from employees | keep emp_no | drop _unmapped_fields";
            assertFalse(query, allowsUnknownUnmappedFields(unknownColumn(query, "_unmapped_fields"), query));
        }
        String overridden = "SET unmapped_fields=\"load_all\";SET unmapped_fields=\"load\";from employees | keep emp_no "
            + "| drop _unmapped_fields";
        assertFalse(allowsUnknownUnmappedFields(unknownColumn(overridden, "_unmapped_fields"), overridden));
    }

    public void testUnmappedFieldsColumnReportedAwayFromItsNameIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_no | drop _unmapped_fields";
        assertFalse(allowsUnknownUnmappedFields(unknownColumn(query, "emp_no", "_unmapped_fields"), query));
    }

    public void testUnmappedFieldsColumnInsideALongerNameIsNotAllowed() {
        for (String mention : List.of("_unmapped_fields*", "_unmapped_fields_x", "_unmapped_fields.x", "`_unmapped_fields`*")) {
            String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_no | drop " + mention;
            assertFalse(mention, allowsUnknownUnmappedFields(unknownColumn(query, mention), query));
        }
    }

    public void testUnmappedFieldsColumnWithAnotherUnknownColumnIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_no | drop _unmapped_fields, foo";
        String error = "Found 2 problems\n" + unknownColumn(query, "_unmapped_fields") + "\n" + unknownColumn(query, "foo", "foo");
        assertFalse(allowsUnknownUnmappedFields(error, query));
    }

    public void testUnmappedFieldsColumnWithAnotherProblemIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_no, x* | drop _unmapped_fields";
        String error = "Found 2 problems\n" + unknownColumn(query, "_unmapped_fields") + "\n" + noMatches(query, "x*");
        assertFalse(allowsUnknownUnmappedFields(error, query));
    }

    public void testUnmappedFieldsColumnWithAProblemWithoutALocationIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_no | drop _unmapped_fields";
        String error = "Found 2 problems\n" + unknownColumn(query, "_unmapped_fields") + "\nline -1:-1: Unknown column [foo]";
        assertFalse(allowsUnknownUnmappedFields(error, query));
    }

    public void testUnmappedFieldsColumnWithAnInjectedUnmappedFieldIsAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_* | drop _unmapped_fields, unmapped_field_foo";
        String error = "Found 2 problems\n"
            + unknownColumn(query, "_unmapped_fields")
            + "\n"
            + unknownColumn(query, "unmapped_field_foo", "unmapped_field_foo");
        assertTrue(allowsUnknownUnmappedFields(error, query));
    }

    public void testInjectedUnmappedFieldWithoutUnmappedFieldsColumnIsNotAllowed() {
        String query = "SET unmapped_fields=\"load_all\";from employees | keep emp_no | drop unmapped_field_foo";
        assertFalse(allowsUnknownUnmappedFields(unknownColumn(query, "unmapped_field_foo", "unmapped_field_foo"), query));
    }

    private static boolean allowsUnknownUnmappedFields(String error, String query) {
        return GenerativeRestTest.isUnknownNamedUnmappedFieldsColumn(error, query, LOAD_ALL_SCHEMA);
    }

    private static String unknownColumn(String query, String mention) {
        return unknownColumn(query, mention, "_unmapped_fields");
    }

    private static String unknownColumn(String query, String mention, String column) {
        return "line 1:" + (query.lastIndexOf(mention) + 1) + ": Unknown column [" + column + "]";
    }

    public void testEqlUnconvertibleFieldTypeIsAllowedUnderTheEqlFeature() {
        assertTrue(allowsEqlFieldTypeError(Set.of(GenerativeFeature.EQL), "cartesian_point"));
        assertTrue(allowsEqlFieldTypeError(Set.of(GenerativeFeature.EQL), "date_nanos"));
        assertTrue(allowsEqlFieldTypeError(Set.of(GenerativeFeature.EQL), "double_range"));
    }

    public void testEqlConvertibleFieldTypeIsNotAllowed() {
        for (DataType type : EqlRelation.CONVERTIBLE_TYPES) {
            assertFalse(type.typeName(), allowsEqlFieldTypeError(Set.of(GenerativeFeature.EQL), type.typeName()));
        }
    }

    public void testEqlUnconvertibleFieldTypeIsNotAllowedWithoutTheEqlFeature() {
        assertFalse(allowsEqlFieldTypeError(Set.of(), "cartesian_point"));
    }

    public void testLookupJoinAfterBareEqlSourceIsAllowed() {
        String query = "SET unmapped_fields=\"nullify\";EQL cloned_sample_data \"any where true\" METADATA _id"
            + " | rename message as message | lookup join message_types_lookup on message";
        assertTrue(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "message_types_lookup"), query));
    }

    public void testLookupJoinAfterEqlOnlySubqueriesIsAllowed() {
        String query =
            "FROM (EQL sample_data \"any where true\"), (EQL sample_data \"sample by client_ip [any where true] [any where true]\")"
                + " | rename client_ip as message | lookup join message_types_lookup on message";
        assertTrue(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "message_types_lookup"), query));
    }

    public void testLookupJoinAfterALoneEqlSubqueryIsAllowed() {
        String query =
            "FROM (EQL sample_data \"any where true\") | rename message as message | lookup join message_types_lookup on message";
        assertTrue(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "message_types_lookup"), query));
    }

    public void testLookupJoinInAForkBranchAfterEqlIsAllowed() {
        String query = "EQL sample_data \"any where true\" | FORK (where true) (where true | rename message as message"
            + " | lookup join message_types_lookup on message)";
        assertTrue(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "message_types_lookup"), query));
    }

    public void testCoordinatorLookupJoinAfterEqlIsNotAllowed() {
        String query = "EQL sample_data \"any where true\" | rename message as message"
            + " | lookup join _coordinator:message_types_lookup on message";
        assertFalse(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "message_types_lookup"), query));
    }

    public void testLookupJoinAfterEqlNextToAnIndexIsNotAllowed() {
        String query = "FROM sample_data, (EQL sample_data \"any where true\")"
            + " | rename message as message | lookup join message_types_lookup on message";
        assertFalse(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "message_types_lookup"), query));
    }

    public void testLookupJoinAfterFromIsNotAllowed() {
        String query = "FROM sample_data | rename message as message | lookup join message_types_lookup on message";
        assertFalse(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "message_types_lookup"), query));
    }

    public void testUnknownIndexThatIsNotTheJoinedLookupIsNotAllowed() {
        String query = "EQL sample_data \"any where true\" | rename message as message | lookup join message_types_lookup on message";
        assertFalse(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "sample_data"), query));
    }

    public void testLookupJoinAfterEqlWithAnotherProblemIsNotAllowed() {
        String query = "EQL sample_data \"any where true\" | rename message as message | lookup join message_types_lookup on message";
        String error = "Found 2 problems\n" + unknownIndex(query, "message_types_lookup") + "\nline 1:1: Unknown column [message]";
        assertFalse(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(error, query));
    }

    public void testLookupJoinAfterEqlWithAnotherErrorIsNotAllowed() {
        String query = "EQL sample_data \"any where true\" | rename message as message | lookup join message_types_lookup on message";
        assertFalse(GenerativeRestTest.isLookupJoinAfterEqlSourceBug("line 1:1: Unknown column [message]", query));
    }

    public void testRemoteEnrichAfterBareEqlSourceIsAllowed() {
        String query = "EQL sample_data \"any where true\" | enrich _remote:languages_policy on message";
        assertTrue(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(unresolvablePolicy(query, "languages_policy"), query));
    }

    public void testRemoteEnrichAfterALoneEqlSubqueryIsAllowed() {
        String query = "FROM (EQL sample_data \"any where true\") | enrich _remote:languages_policy on message";
        assertTrue(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(unresolvablePolicy(query, "languages_policy"), query));
    }

    public void testRemoteEnrichAndLookupJoinAfterEqlAreAllowedTogether() {
        String query = "EQL sample_data \"any where true\" | enrich _remote:languages_policy on message"
            + " | rename message as message | lookup join message_types_lookup on message";
        String error = "Found 2 problems\n"
            + "line 1:"
            + (query.indexOf("languages_policy") + 1)
            + ": enrich policy [languages_policy] cannot be resolved since remote"
            + " clusters are unavailable\n"
            + unknownIndex(query, "message_types_lookup").substring("Found 1 problem\n".length());
        assertTrue(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(error, query));
        assertTrue(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(error, query));
    }

    public void testRemoteEnrichAfterFromIsNotAllowed() {
        String query = "FROM sample_data | enrich _remote:languages_policy on message";
        assertFalse(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(unresolvablePolicy(query, "languages_policy"), query));
    }

    public void testUnresolvablePolicyWithoutRemoteModeIsNotAllowed() {
        String query = "EQL sample_data \"any where true\" | enrich languages_policy on message";
        assertFalse(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(unresolvablePolicy(query, "languages_policy"), query));
    }

    public void testUnresolvableRemotePolicyIsNotTheLookupJoinBug() {
        String query = "EQL sample_data \"any where true\" | enrich _remote:languages_policy on message";
        assertFalse(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unresolvablePolicy(query, "languages_policy"), query));
    }

    public void testRemoteEnrichAfterEqlWithAnotherProblemIsNotAllowed() {
        String query = "EQL sample_data \"any where true\" | enrich _remote:languages_policy on message";
        String error = unresolvablePolicy(query, "languages_policy") + "\nline 1:1: Unknown column [message]";
        assertFalse(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(error, query));
    }

    public void testTooManyIdsForAnEqlSequenceWithoutSizeIsAllowed() {
        String query = "SET unmapped_fields=\"nullify\";FROM many_numbers, (EQL many_numbers \"sequence by sv [any where sv > 12]"
            + " [any where true]\" WITH {\"timestamp_field\": \"@timestamp\"}) | fillnull DEFAULT ON *";
        assertTrue(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testTooManyIdsForAnEqlSampleWithoutSizeIsAllowed() {
        String query = "EQL many_numbers \"sample by sv [any where true] [any where true]\" METADATA _id | where sv > 1";
        assertTrue(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testTooManyIdsWithAnExplicitSizeIsNotAllowed() {
        String query = "EQL many_numbers \"sequence [any where true] [any where true]\" WITH {\"size\": 20, \"fetch_size\": 100}"
            + " | where sv > 1";
        assertFalse(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testTooManyIdsForAnEqlSequenceWithOnlyAFetchSizeIsAllowed() {
        String query = "EQL many_numbers \"sequence [any where true] [any where true]\" WITH {\"fetch_size\": 100} | where sv > 1";
        assertTrue(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testTooManyIdsWithOneUnsizedEqlBranchIsAllowed() {
        String query = "FROM (EQL many_numbers \"sequence [any where true] [any where true]\" WITH {\"size\": 5}),"
            + " (EQL many_numbers \"sample by sv [any where sv == \\\"1\\\"] [any where true]\") | where sv > 1";
        assertTrue(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testTooManyIdsWithEveryEqlBranchSizedIsNotAllowed() {
        String query = "FROM (EQL many_numbers \"sequence [any where true] [any where true]\" WITH {\"size\": 5}),"
            + " (EQL many_numbers \"sample by sv [any where true] [any where true]\" WITH {\"size\": 7}) | where sv > 1";
        assertFalse(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testTooManyIdsWithAPushedLimitIsNotAllowed() {
        String query = "EQL many_numbers \"sequence [any where true] [any where true]\" | limit 50";
        assertFalse(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testTooManyIdsWithAnEqlHeadPipeIsNotAllowed() {
        String query = "EQL many_numbers \"sequence [any where true] [any where true] | head 5\" | where sv > 1";
        assertFalse(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testUnresolvableRemotePolicyThatIsNotInTheQueryIsNotAllowed() {
        String query = "EQL sample_data \"any where true\" | enrich _remote:languages_policy on message";
        assertFalse(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(unresolvablePolicy(query, "message"), query));
    }

    public void testTooManyIdsForAnEqlEventQueryIsNotAllowed() {
        String query = "EQL many_numbers \"any where true\" WITH {\"fetch_size\": 100} | where sv > 1";
        assertFalse(GenerativeRestTest.isEqlTruncationCapSizeBug(TOO_MANY_IDS, query));
    }

    public void testOtherErrorsForAnEqlSequenceWithoutSizeAreNotAllowed() {
        String query = "EQL many_numbers \"sequence [any where true] [any where true]\" | where sv > 1";
        assertFalse(GenerativeRestTest.isEqlTruncationCapSizeBug("failed to create query: For input string: \"x\"", query));
    }

    public void testRemoteEnrichPolicyAsUnknownColumnUnderNullifyIsAllowed() {
        String query = "SET unmapped_fields=\"nullify\";EQL mv_expand_mapped \"sample [\\\"c\\\" where id == \\\"m2\\\"] by id"
            + " [\\\"c\\\" where startsWith(tags, \\\"\\\")] by id\" WITH {\"event_category_field\": \"tags\", \"max_samples_per_key\": 5}"
            + " | enrich _remote:languages_policy on id";
        String error = "{\"error\":{\"root_cause\":[{\"type\":\"verification_exception\",\"reason\":\"Found 1 problem\\nline 1:"
            + (query.lastIndexOf("languages_policy") + 1)
            + ": Unknown column [languages_policy]\"}]}}";
        assertTrue(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(error, query));
        assertFalse(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(error, query));
    }

    public void testInexistentPolicyAsUnknownColumnUnderLoadAllIsAllowed() {
        String query = "SET unmapped_fields=\"LOAD_ALL\";FROM partial_mapping_sample_data | EVAL lc = language_code::integer"
            + " | ENRICH foo ON lc | SORT @timestamp | LIMIT 3";
        assertTrue(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(unknownColumn(query, "foo", "foo"), query));
    }

    public void testInexistentPolicyAsUnknownColumnUnderLoadAfterApproximationIsAllowed() {
        String query = "SET approximation=true;SET unmapped_fields=\"load\";FROM sample_data | ENRICH foo ON message";
        assertTrue(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(unknownColumn(query, "foo", "foo"), query));
    }

    public void testExistingLocalPolicyAsUnknownColumnIsNotAllowed() {
        String query = "SET unmapped_fields=\"nullify\";FROM sample_data | ENRICH languages_policy ON message";
        String error = unknownColumn(query, "languages_policy", "languages_policy");
        assertFalse(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(error, query));
    }

    public void testExistingLocalPolicyAsUnknownColumnAfterEqlIsNotAllowed() {
        String query =
            "SET unmapped_fields=\"nullify\";EQL sample_data \"any where true\" | ENRICH _coordinator:languages_policy ON message";
        String error = unknownColumn(query, "languages_policy", "languages_policy");
        assertFalse(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(error, query));
    }

    public void testPolicyAsUnknownColumnUnderDefaultUnmappedFieldsIsNotAllowed() {
        String query = "SET unmapped_fields=\"default\";FROM sample_data | ENRICH foo ON message";
        assertFalse(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(unknownColumn(query, "foo", "foo"), query));
    }

    public void testLookupJoinAfterFromUnderUnmappedFieldsIsNotAllowed() {
        String query = "SET unmapped_fields=\"nullify\";FROM sample_data | rename message as message"
            + " | lookup join message_types_lookup on message";
        assertFalse(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(unknownIndex(query, "message_types_lookup"), query));
    }

    public void testRemoteEnrichAfterFromUnderUnmappedFieldsIsNotAllowed() {
        String query = "SET unmapped_fields=\"nullify\";FROM sample_data | enrich _remote:languages_policy on message";
        assertFalse(GenerativeRestTest.isRemoteEnrichAfterEqlSourceBug(unresolvablePolicy(query, "languages_policy"), query));
        String error = unknownColumn(query, "languages_policy", "languages_policy");
        assertFalse(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(error, query));
    }

    public void testPolicyAsUnknownColumnAndLookupJoinAfterEqlAreAllowedTogether() {
        String query = "SET unmapped_fields=\"nullify\";EQL sample_data \"any where true\" | enrich _remote:languages_policy on message"
            + " | rename message as message | lookup join message_types_lookup on message";
        String error = "Found 2 problems\n"
            + unknownColumn(query, "languages_policy", "languages_policy")
            + "\n"
            + unknownIndex(query, "message_types_lookup").substring("Found 1 problem\n".length());
        assertTrue(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(error, query));
        assertTrue(GenerativeRestTest.isLookupJoinAfterEqlSourceBug(error, query));
    }

    public void testPolicyAsUnknownColumnWithoutUnmappedFieldsIsNotAllowed() {
        String query = "FROM partial_mapping_sample_data | ENRICH foo ON message";
        assertFalse(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(unknownColumn(query, "foo", "foo"), query));
    }

    public void testUnknownColumnThatIsNotAPolicyIsNotTheEnrichPolicyBug() {
        String query = "SET unmapped_fields=\"nullify\";FROM sample_data | ENRICH languages_policy ON message | KEEP language_name";
        assertFalse(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(unknownColumn(query, "language_name", "language_name"), query));
    }

    public void testPolicyAsUnknownColumnWithAnotherProblemIsNotAllowed() {
        String query = "SET unmapped_fields=\"load\";FROM sample_data | ENRICH foo ON message";
        String error = unknownColumn(query, "foo", "foo") + "\nline 1:1: Unknown column [message]";
        assertFalse(GenerativeRestTest.isEnrichPolicyAsUnknownColumnBug(error, query));
    }

    private static final String TOO_MANY_IDS = "\"type\" : \"query_shard_exception\", \"reason\" : \"failed to create query: Too many ids"
        + " specified, allowed max result window is [10000]\", \"index\" : \"many_numbers\"";

    private static String unresolvablePolicy(String query, String policy) {
        return "Found 1 problem\nline 1:"
            + (query.lastIndexOf(policy) + 1)
            + ": enrich policy ["
            + policy
            + "] cannot be resolved since remote clusters are unavailable";
    }

    private static String unknownIndex(String query, String index) {
        return "Found 1 problem\nline 1:" + (query.lastIndexOf(index) + 1) + ": Unknown index [" + index + "]";
    }

    private static boolean allowsEqlFieldTypeError(Set<GenerativeFeature> features, String type) {
        GenerativeRestTest runner = new GenerativeRestTest() {
            @Override
            protected boolean supportsSourceFieldMapping() {
                throw new AssertionError("no cluster is started for these tests");
            }

            @Override
            protected Set<GenerativeFeature> enabledFeatures() {
                return features;
            }
        };
        String query = "EQL k8s \"any where true\" | KEEP event_location";
        String error = "Found 1 problem\nline 1:33: EQL command does not yet support [" + type + "] fields";
        return runner.isAllowedFailure(new QueryExecuted(query, 0, null, null, new RuntimeException(error)), List.of(), List.of());
    }
}
