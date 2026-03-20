/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzeStatement;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultEnrichResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class AnalyzerUnmappedTests extends ESTestCase {
    public void testFailKeepAndNonMatchingStar() {
        var query = """
            FROM test
            | KEEP does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailKeepAndMatchingAndNonMatchingStar() {
        var query = """
            FROM test
            | KEEP emp_*, does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailAfterKeep() {
        var query = """
            FROM test
            | KEEP emp_*
            | EVAL x = does_not_exist_field + 1
            """;
        var failure = "Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailDropWithNonMatchingStar() {
        var query = """
            FROM test
            | DROP does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailDropWithMatchingAndNonMatchingStar() {
        var query = """
            FROM test
            | DROP emp_*, does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailEvalAfterDrop() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | EVAL x = does_not_exist_field + 1
            """;

        var failure = "3:12: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailFilterAfterDrop() {
        var query = """
            FROM test
            | WHERE emp_no > 1000
            | DROP emp_no
            | WHERE emp_no < 2000
            """;

        var failure = "line 4:9: Unknown column [emp_no]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailDropThenKeep() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | KEEP does_not_exist_field
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailDropThenEval() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | EVAL does_not_exist_field + 2
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailEvalThenDropThenEval() {
        var query = """
            FROM test
            | KEEP does_not_exist_field
            | EVAL x = does_not_exist_field::LONG + 1
            | WHERE x IS NULL
            | DROP does_not_exist_field
            | EVAL does_not_exist_field::LONG + 2
            """;
        var failure = "line 6:8: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailStatsThenKeep() {
        var query = """
            FROM test
            | STATS cnd = COUNT(*)
            | KEEP does_not_exist_field
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailStatsThenKeepShadowing() {
        var query = """
            FROM employees
            | STATS count(*)
            | EVAL foo = emp_no
            """;

        var failure = "line 3:14: Unknown column [emp_no]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailStatsThenEval() {
        var query = """
            FROM test
            | STATS cnt = COUNT(*)
            | EVAL x = does_not_exist_field + cnt
            """;
        var failure = "line 3:12: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailAfterUnionAllOfStats() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM employees
                 | STATS c = COUNT(*))
            | SORT does_not_exist
            """;
        var failure = "line 4:8: Unknown column [does_not_exist]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    // unmapped_fields="load" disallows subqueries and LOOKUP JOIN (see #142033)
    public void testSubquerysMixAndLookupJoinLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var e = expectThrows(VerificationException.class, () -> analyzeStatement(setUnmappedLoad("""
            FROM test,
                (FROM languages
                 | WHERE language_code > 10
                 | RENAME language_name as languageName),
                (FROM sample_data
                | STATS max(@timestamp)),
                (FROM test
                | EVAL language_code = languages
                | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE emp_no > 10000 OR does_not_exist1::LONG < 10
            | STATS COUNT(*) BY emp_no, language_code, does_not_exist2
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """)));
        String msg = e.getMessage();
        assertThat(msg, containsString("Found 4 problems"));
        assertThat(msg, containsString("Subqueries and views are not supported with unmapped_fields=\"load\""));
        assertThat(msg, containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\""));
        assertThat(msg, not(containsString("FORK is not supported")));
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyNullify() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """;
        var failure = "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """;
        var failure = "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailAfterForkOfStats() {
        var query = """
            FROM test
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary))
                   (DISSECT hire_date::KEYWORD "%{year}-%{month}-%{day}T"
                    | STATS x = MIN(year::LONG), y = MAX(month::LONG) WHERE year::LONG > 1000 + does_not_exist2::DOUBLE)
            | EVAL e = does_not_exist3 + 1
            """;
        var failure = "line 7:12: Unknown column [does_not_exist3]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailMetadataFieldInKeep() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | KEEP " + field;
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInEval() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | EVAL x = " + field;
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInWhere() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | WHERE " + field + " IS NOT NULL";
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInSort() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | SORT " + field;
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInStats() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | STATS x = COUNT(" + field + ")";
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInRename() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | RENAME " + field + " AS renamed";
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldAfterStats() {
        var query = """
            FROM test
            | STATS c = COUNT(*)
            | KEEP _score
            """;
        var failure = "Unknown column [_score]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailMetadataFieldInFork() {
        var query = """
            FROM test
            | FORK (WHERE _score > 1)
                   (WHERE salary > 50000)
            """;
        var failure = "Unknown column [_score]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailMetadataFieldInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM test
                 | WHERE _score > 1)
            """;
        var failure = "Unknown column [_score]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_score{m}#5]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, ...]
     */
    public void testMetadataFieldDeclaredNullify() {
        // This isn't gilded since it would just create a bunch of clutter due to nesting.
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var plan = analyzeStatement(setUnmappedNullify("FROM test METADATA " + field + " | KEEP " + field));

            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(FoldContext.small()), is(1000));

            var project = as(limit.child(), Project.class);
            assertThat(project.projections(), hasSize(1));
            assertThat(Expressions.name(project.projections().getFirst()), is(field));
            assertThat(project.projections().getFirst(), instanceOf(MetadataAttribute.class));

            // No Eval(NULL) — the field was resolved via METADATA, not nullified
            var relation = as(project.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("test"));
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_score{m}#5]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, ...]
     */
    public void testMetadataFieldDeclaredLoad() {
        // This isn't gilded since it would just create a bunch of clutter due to nesting.
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var plan = analyzeStatement(setUnmappedLoad("FROM test METADATA " + field + " | KEEP " + field));

            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(FoldContext.small()), is(1000));

            var project = as(limit.child(), Project.class);
            assertThat(project.projections(), hasSize(1));
            assertThat(Expressions.name(project.projections().getFirst()), is(field));
            assertThat(project.projections().getFirst(), instanceOf(MetadataAttribute.class));

            // The field was resolved via METADATA, not loaded as an unmapped field into EsRelation
            var relation = as(project.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("test"));
        }
    }

    public void testChangedTimestmapFieldWithRate() {
        verificationFailure(setUnmappedNullify("""
            TS k8s
            | RENAME @timestamp AS newTs
            | STATS max(rate(network.total_cost)) BY tbucket = BUCKET(newTs, 1hour)
            """), "3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX);

        verificationFailure(setUnmappedNullify("""
            TS k8s
            | DROP @timestamp
            | STATS max(rate(network.total_cost))
            """), "3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX);
    }

    public void testLoadModeDisallowsFork() {
        verificationFailure(
            setUnmappedLoad("FROM test | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)"),
            "FORK is not supported with unmapped_fields=\"load\""
        );
    }

    public void testLoadModeDisallowsForkWithStats() {
        verificationFailure(
            setUnmappedLoad("FROM test | FORK (STATS c = COUNT(*)) (STATS d = AVG(salary))"),
            "FORK is not supported with unmapped_fields=\"load\""
        );
    }

    public void testLoadModeDisallowsForkWithMultipleBranches() {
        verificationFailure(setUnmappedLoad("""
            FROM test
            | FORK (WHERE emp_no > 1)
                   (WHERE emp_no < 100)
                   (WHERE salary > 50000)
            """), "FORK is not supported with unmapped_fields=\"load\"");
    }

    public void testLoadModeDisallowsLookupJoin() {
        verificationFailure(
            setUnmappedLoad("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"),
            "LOOKUP JOIN is not supported with unmapped_fields=\"load\""
        );
    }

    public void testLoadModeDisallowsLookupJoinAfterFilter() {
        verificationFailure(setUnmappedLoad("""
            FROM test
            | WHERE emp_no > 1
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | KEEP emp_no, language_name
            """), "LOOKUP JOIN is not supported with unmapped_fields=\"load\"");
    }

    public void testLoadModeDisallowsSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        verificationFailure(
            setUnmappedLoad("FROM test, (FROM languages | WHERE language_code > 1)"),
            "Subqueries and views are not supported with unmapped_fields=\"load\""
        );
    }

    public void testLoadModeDisallowsMultipleSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        verificationFailure(setUnmappedLoad("""
            FROM test,
                (FROM languages | WHERE language_code > 1),
                (FROM sample_data | STATS max(@timestamp))
            """), "Subqueries and views are not supported with unmapped_fields=\"load\"");
    }

    public void testLoadModeDisallowsNestedSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        verificationFailure(
            setUnmappedLoad("FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)"),
            "Subqueries and views are not supported with unmapped_fields=\"load\""
        );
    }

    public void testLoadModeDisallowsSubqueryWithLookupJoin() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        verificationFailure(setUnmappedLoad("""
            FROM test,
                (FROM test
                | EVAL language_code = languages
                | LOOKUP JOIN languages_lookup ON language_code)
            """), "Subqueries and views are not supported with unmapped_fields=\"load\"");
    }

    public void testLoadModeDisallowsForkAndLookupJoin() {
        var query = setUnmappedLoad("""
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)
            """);
        verificationFailure(query, "FORK is not supported with unmapped_fields=\"load\"");
        verificationFailure(query, "LOOKUP JOIN is not supported with unmapped_fields=\"load\"");
    }

    public void testLoadModeDisallowsSubqueryAndFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = setUnmappedLoad("""
            FROM test, (FROM languages | WHERE language_code > 1)
            | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)
            """);
        verificationFailure(query, "Subqueries and views are not supported with unmapped_fields=\"load\"");
        verificationFailure(query, "FORK is not supported with unmapped_fields=\"load\"");
    }

    private static final String UNMAPPED_TIMESTAMP_SUFFIX = UnresolvedTimestamp.UNRESOLVED_SUFFIX + Verifier.UNMAPPED_TIMESTAMP_SUFFIX;

    public void testTbucketWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTrangeWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | WHERE trange(1 hour)", "[trange(1 hour)] ");
    }

    public void testTbucketAndTrangeWithUnmappedTimestamp() {
        unmappedTimestampFailure(
            "FROM test | WHERE trange(1 hour) | STATS c = COUNT(*) BY tbucket(1 hour)",
            "[tbucket(1 hour)] ",
            "[trange(1 hour)] "
        );
    }

    public void testRateWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS rate(salary)", "[rate(salary)] ");
    }

    public void testIrateWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS irate(salary)", "[irate(salary)] ");
    }

    public void testDeltaWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS delta(salary)", "[delta(salary)] ");
    }

    public void testIdeltaWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS idelta(salary)", "[idelta(salary)] ");
    }

    public void testIncreaseWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS increase(salary)", "[increase(salary)] ");
    }

    public void testDerivWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS deriv(salary)", "[deriv(salary)] ");
    }

    public void testFirstOverTimeWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS first_over_time(salary)", "[first_over_time(salary)] ");
    }

    public void testLastOverTimeWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS last_over_time(salary)", "[last_over_time(salary)] ");
    }

    public void testRateAndTbucketWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS rate(salary) BY tbucket(1 hour)", "[rate(salary)] ", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampAfterWhere() {
        unmappedTimestampFailure("FROM test | WHERE emp_no > 10 | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampAfterEval() {
        unmappedTimestampFailure("FROM test | EVAL x = salary + 1 | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampMultipleGroupings() {
        unmappedTimestampFailure("FROM test | STATS c = COUNT(*) BY tbucket(1 hour), emp_no", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampAfterRename() {
        unmappedTimestampFailure("FROM test | RENAME emp_no AS e | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampAfterDrop() {
        unmappedTimestampFailure("FROM test | DROP emp_no | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTrangeWithUnmappedTimestampCompoundWhere() {
        unmappedTimestampFailure("FROM test | WHERE trange(1 hour) AND emp_no > 10", "[trange(1 hour)] ");
    }

    public void testTrangeWithUnmappedTimestampAfterEval() {
        unmappedTimestampFailure("FROM test | EVAL x = salary + 1 | WHERE trange(1 hour)", "[trange(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampInInlineStats() {
        unmappedTimestampFailure("FROM test | INLINE STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampWithFork() {
        var query = "FROM test | FORK (STATS c = COUNT(*) BY tbucket(1 hour)) (STATS d = COUNT(*) BY emp_no)";
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var e = expectThrows(VerificationException.class, () -> analyzeStatement(statement));
            assertThat(e.getMessage(), containsString("[tbucket(1 hour)] "));
            assertThat(e.getMessage(), not(containsString("FORK is not supported")));
        }
    }

    public void testTbucketWithUnmappedTimestampWithLookupJoin() {
        var query = """
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | STATS c = COUNT(*) BY tbucket(1 hour)
            """;
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var e = expectThrows(VerificationException.class, () -> analyzeStatement(statement));
            assertThat(e.getMessage(), containsString("[tbucket(1 hour)] "));
            assertThat(e.getMessage(), not(containsString("LOOKUP JOIN is not supported")));
        }
    }

    public void testTbucketWithTimestampPresent() {
        var query = "FROM sample_data | STATS c = COUNT(*) BY tbucket(1 hour)";
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var plan = analyzeStatement(statement);
            var limit = as(plan, Limit.class);
            var aggregate = as(limit.child(), Aggregate.class);
            var relation = as(aggregate.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("sample_data"));
            assertTimestampInOutput(relation);
        }
    }

    public void testTrangeWithTimestampPresent() {
        var query = "FROM sample_data | WHERE trange(1 hour)";
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var plan = analyzeStatement(statement);
            var limit = as(plan, Limit.class);
            var filter = as(limit.child(), Filter.class);
            var relation = as(filter.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("sample_data"));
            assertTimestampInOutput(relation);
        }
    }

    public void testTbucketTimestampPresentButDroppedNullify() {
        var e = expectThrows(
            VerificationException.class,
            () -> analyzeStatement(setUnmappedNullify("FROM sample_data | DROP @timestamp | STATS c = COUNT(*) BY tbucket(1 hour)"))
        );
        assertThat(e.getMessage(), containsString(UnresolvedTimestamp.UNRESOLVED_SUFFIX));
        assertThat(e.getMessage(), not(containsString(Verifier.UNMAPPED_TIMESTAMP_SUFFIX)));
    }

    public void testTbucketTimestampPresentButRenamedNullify() {
        var e = expectThrows(
            VerificationException.class,
            () -> analyzeStatement(setUnmappedNullify("FROM sample_data | RENAME @timestamp AS ts | STATS c = COUNT(*) BY tbucket(1 hour)"))
        );
        assertThat(e.getMessage(), containsString(UnresolvedTimestamp.UNRESOLVED_SUFFIX));
        assertThat(e.getMessage(), not(containsString(Verifier.UNMAPPED_TIMESTAMP_SUFFIX)));
    }

    private static void assertTimestampInOutput(EsRelation relation) {
        assertTrue(
            "@timestamp field should be present in the EsRelation output",
            relation.output().stream().anyMatch(a -> MetadataAttribute.TIMESTAMP_FIELD.equals(a.name()))
        );
    }

    private void unmappedTimestampFailure(String query, String... expectedFailures) {
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var e = expectThrows(VerificationException.class, () -> analyzeStatement(statement));
            for (String expected : expectedFailures) {
                assertThat(e.getMessage(), containsString(expected + UNMAPPED_TIMESTAMP_SUFFIX));
            }
        }
    }

    /**
     * Verify that referencing a sub-field of a flattened field (e.g. "foo.bar" when "foo" is flattened) is rejected
     */
    public void testFlattenedSubFieldRejectionWithSimpleCases() {
        EsIndex index = createIndex1();
        unmappedLoadAndFlattenedSubfieldHelper("FROM test | KEEP field.a", index, "field.a", "field");
        unmappedLoadAndFlattenedSubfieldHelper("FROM test | STATS x = SAMPLE(field.a, 1)", index, "field.a", "field");
        unmappedLoadAndFlattenedSubfieldHelper("FROM test | EVAL x = TO_STRING(field.a)", index, "field.a", "field");
        unmappedLoadAndFlattenedSubfieldHelper("FROM test | KEEP field.a.b", index, "field.a.b", "field");
        unmappedLoadAndFlattenedSubfieldHelper("FROM test | KEEP field.a.b.c", index, "field.a.b.c", "field");
        unmappedLoadAndFlattenedSubfieldHelper("FROM test | SORT field.x, field.z", index, "field.x", "field");
        unmappedLoadAndFlattenedSubfieldHelper("FROM test | SORT field.x | KEEP field.z", index, "field.x", "field", "field.z", "field");
        verificationFailure(setUnmappedLoad("FROM test | KEEP field | KEEP field.a"), index, "Unknown column [field.a]");
        verificationFailure(
            setUnmappedLoad("FROM test | KEEP field | WHERE field.sub.subfield == \"x\""),
            index,
            "Unknown column [field.sub.subfield]"
        );
        verificationFailure(
            setUnmappedLoad("FROM test | KEEP field | WHERE field.a.b == \"x\" | KEEP field.a"),
            index,
            "Unknown column [field.a.b], did you mean [field]?"
        );
    }

    /**
     * Verify that referencing a sub-field of a flattened field is rejected in a FORK
     */
    public void testFlattenedSubFieldRejectionWithFork() {
        EsIndex index = createIndex1();
        verificationFailure(
            setUnmappedLoad("""
                FROM test
                | eval aaa = field.aaa
                | FORK (eval x = resource.attributes.host.name) (eval y = attributes.xxx) (eval z = field.bbb)
                """),
            index,
            "Found 5 problems",
            "line 3:3: FORK is not supported with unmapped_fields=\"load\"",
            // this error appears 3 times thanks to the FORK branching out
            "line 2:14: Loading subfield [field.aaa] when parent [field] is of flattened field type is not supported with "
                + "unmapped_fields=\"load\"",
            "line 3:85: Loading subfield [field.bbb] when parent [field] is of flattened field type is not supported with "
                + "unmapped_fields=\"load\""
        );
    }

    /**
     * Verify that referencing a sub-field of a flattened field is rejected in a LOOKUP JOIN
     */
    public void testFlattenedSubFieldRejectionWithLookupJoin() {
        EsIndex index = createIndex1();
        verificationFailure(
            setUnmappedLoad("""
                FROM test
                | EVAL language_code = 1
                | LOOKUP JOIN languages_lookup ON language_code
                | EVAL x = field.languages
                """),
            index,
            "Found 2 problems",
            "line 3:15: LOOKUP JOIN is not supported with unmapped_fields=\"load\"",
            "line 4:12: Loading subfield [field.languages] when parent [field] is of flattened field type is not supported with "
                + "unmapped_fields=\"load\""
        );
    }

    private void unmappedLoadAndFlattenedSubfieldHelper(String query, EsIndex index, String... pairs) {
        assert pairs.length % 2 == 0;
        String errorMessage =
            "Loading subfield [%s] when parent [%s] is of flattened field type is not supported with unmapped_fields=\"load\"";
        String[] errors = new String[pairs.length / 2];
        int j = 0;

        for (int i = 0; i < pairs.length; i += 2) {
            errors[j++] = String.format(Locale.ROOT, errorMessage, pairs[i], pairs[i + 1]);
        }

        verificationFailure(setUnmappedLoad(query), index, errors);
    }

    private void verificationFailure(String query, EsIndex index, String... expectedFailures) {
        EsqlStatement statement = TEST_PARSER.createStatement(query);
        Map<IndexPattern, IndexResolution> indexResolutions = Map.of(
            new IndexPattern(Source.EMPTY, index.name()),
            IndexResolution.valid(index)
        );
        Analyzer analyzer = AnalyzerTestUtils.analyzer(indexResolutions, TEST_VERIFIER, configuration(query), statement);
        var e = expectThrows(VerificationException.class, () -> analyzer.analyze(statement.plan()));
        for (String expectedFailure : expectedFailures) {
            assertThat(e.getMessage(), containsString(expectedFailure));
        }
    }

    private void verificationFailure(String statement, String expectedFailure) {
        var e = expectThrows(VerificationException.class, () -> analyzeStatement(statement));
        assertThat(e.getMessage(), containsString(expectedFailure));
    }

    private static String setUnmappedNullify(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_V2", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V2.isEnabled());
        return "SET unmapped_fields=\"load\"; " + query;
    }

    /**
     * Reproducer for #141927: with unmapped_fields=load, full-text search (MATCH, match operator, MATCH_PHRASE, etc.)
     * must fail at analysis instead of returning empty results.
     * <p>
     * One assertion per forbidden full-text function so that re-enabling any of them (e.g. QSTR, KNN, MATCH_PHRASE)
     * would cause this test to fail. When full-text function support grows, this test will need updates; see #144121.
     */
    public void testUnmappedFieldsLoadWithFullTextSearchFails() {
        // Assert the new message format and that the specific full-text function is named in brackets
        // Function names in error messages use Function.functionName() (class simple name upper-cased) or override (e.g. QSTR, :)
        verificationFailure(
            setUnmappedLoad("FROM test | WHERE first_name:\"foo\" | KEEP first_name"),
            "does not support full-text search function [:]"
        );
        verificationFailure(
            setUnmappedLoad("FROM test | WHERE match(first_name, \"foo\") | KEEP first_name"),
            "does not support full-text search function [MATCH]"
        );
        verificationFailure(
            setUnmappedLoad("FROM test | WHERE match_phrase(first_name, \"foo bar\") | KEEP first_name"),
            "does not support full-text search function [MatchPhrase]"
        );
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            verificationFailure(
                setUnmappedLoad("FROM test | WHERE multi_match(\"foo\", first_name) | KEEP first_name"),
                "does not support full-text search function [MultiMatch]"
            );
        }
        if (EsqlCapabilities.Cap.QSTR_FUNCTION.isEnabled()) {
            verificationFailure(
                setUnmappedLoad("FROM test | WHERE qstr(\"first_name: foo\") | KEEP first_name"),
                "does not support full-text search function [QSTR]"
            );
        }
        if (EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled()) {
            verificationFailure(
                setUnmappedLoad("FROM test | WHERE kql(\"first_name: foo\") | KEEP first_name"),
                "does not support full-text search function [KQL]"
            );
        }
        verificationFailureWithMapping(
            "mapping-full_text_search.json",
            setUnmappedLoad("FROM test | WHERE knn(vector, [1, 2, 3]) | KEEP vector"),
            "does not support full-text search function [KNN]"
        );
    }

    private void verificationFailureWithMapping(String mapping, String statement, String expectedFailure) {
        var st = TEST_PARSER.createStatement(statement);
        var indexResolutions = Map.of(new IndexPattern(Source.EMPTY, "test"), loadMapping(mapping, "test"));
        var analyzer = analyzer(
            indexResolutions,
            defaultLookupResolution(),
            defaultEnrichResolution(),
            TEST_VERIFIER,
            configuration(statement),
            st.setting(UNMAPPED_FIELDS)
        );
        var e = expectThrows(VerificationException.class, () -> analyzer.analyze(st.plan()));
        assertThat(e.getMessage(), containsString(expectedFailure));
    }

    private static EsIndex createIndex1() {
        Map<String, EsField> mapping = Map.of("field", new UnsupportedEsField("field", List.of("flattened")));
        return new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD), Map.of(), Map.of(), Set.of());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
