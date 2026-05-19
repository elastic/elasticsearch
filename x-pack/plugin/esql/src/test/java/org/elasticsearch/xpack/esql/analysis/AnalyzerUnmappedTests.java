/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.fieldCapabilitiesIndexResponse;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.fieldResponseMap;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.mergedResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class AnalyzerUnmappedTests extends ESTestCase {

    /**
     * Query suffixes that use the unsupported type-conflict field [message] in different commands.
     * Each type conflict test iterates over these to verify the error is raised regardless of how the field is used.
     */
    private static final String[] TYPE_CONFLICT_QUERY_SUFFIXES = new String[] {
        "| SORT message",
        "| EVAL x = message",
        "| WHERE message IS NOT NULL" };

    public void testFailKeepAndNonMatchingStar() {
        assertUnmappedFailure(test(), """
            FROM test
            | KEEP does_not_exist_field*
            """, "No matches found for pattern [does_not_exist_field*]");
    }

    public void testFailKeepAndMatchingAndNonMatchingStar() {
        assertUnmappedFailure(test(), """
            FROM test
            | KEEP emp_*, does_not_exist_field*
            """, "No matches found for pattern [does_not_exist_field*]");
    }

    public void testFailAfterKeep() {
        assertUnmappedFailure(test(), """
            FROM test
            | KEEP emp_*
            | EVAL x = does_not_exist_field + 1
            """, "Unknown column [does_not_exist_field]");
    }

    public void testFailDropWithNonMatchingStar() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field*
            """, "No matches found for pattern [does_not_exist_field*]");
    }

    public void testFailDropWithMatchingAndNonMatchingStar() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP emp_*, does_not_exist_field*
            """, "No matches found for pattern [does_not_exist_field*]");
    }

    public void testFailEvalAfterDrop() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field
            | EVAL x = does_not_exist_field + 1
            """, "3:12: Unknown column [does_not_exist_field]");
    }

    public void testFailFilterAfterDrop() {
        assertUnmappedFailure(test(), """
            FROM test
            | WHERE emp_no > 1000
            | DROP emp_no
            | WHERE emp_no < 2000
            """, "line 4:9: Unknown column [emp_no]");
    }

    public void testFailDropThenKeep() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field
            | KEEP does_not_exist_field
            """, "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testFailDropThenEval() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field
            | EVAL does_not_exist_field + 2
            """, "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testFailEvalThenDropThenEval() {
        assertUnmappedFailure(test(), """
            FROM test
            | KEEP does_not_exist_field
            | EVAL x = does_not_exist_field::LONG + 1
            | WHERE x IS NULL
            | DROP does_not_exist_field
            | EVAL does_not_exist_field::LONG + 2
            """, "line 6:8: Unknown column [does_not_exist_field]");
    }

    public void testFailStatsThenKeep() {
        assertUnmappedFailure(test(), """
            FROM test
            | STATS cnd = COUNT(*)
            | KEEP does_not_exist_field
            """, "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testFailStatsThenKeepShadowing() {
        assertUnmappedFailure(test(), """
            FROM test
            | STATS count(*)
            | EVAL foo = emp_no
            """, "line 3:14: Unknown column [emp_no]");
    }

    public void testFailStatsThenEval() {
        assertUnmappedFailure(test(), """
            FROM test
            | STATS cnt = COUNT(*)
            | EVAL x = does_not_exist_field + cnt
            """, "line 3:12: Unknown column [does_not_exist_field]");
    }

    public void testFailAfterUnionAllOfStats() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedFailure(test(), """
            FROM
                (FROM test
                 | STATS c = COUNT(*))
            | SORT does_not_exist
            """, "line 4:8: Unknown column [does_not_exist]");
    }

    // unmapped_fields="load" disallows subqueries and LOOKUP JOIN (see #142033)
    public void testSubquerysMixAndLookupJoinLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        test().addLanguages()
            .addSampleData()
            .addLanguagesLookup()
            .statementError(
                setUnmappedLoad("""
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
                    """),
                allOf(
                    containsString("Found 4 problems"),
                    containsString("line 2:5: Subqueries and views are not supported with unmapped_fields=\"load\""),
                    containsString("line 5:5: Subqueries and views are not supported with unmapped_fields=\"load\""),
                    containsString("line 7:5: Subqueries and views are not supported with unmapped_fields=\"load\""),
                    containsString("line 9:19: LOOKUP JOIN is not supported with unmapped_fields=\"load\""),
                    not(containsString("FORK is not supported"))
                )
            );
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyNullify() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedFailure(analyzer().addLanguages(), """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """, "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?");
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedFailure(analyzer().addLanguages(), """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """, "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?");
    }

    public void testFailAfterForkOfStats() {
        assertUnmappedFailure(test(), """
            FROM test
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary))
                   (DISSECT hire_date::KEYWORD "%{year}-%{month}-%{day}T"
                    | STATS x = MIN(year::LONG), y = MAX(month::LONG) WHERE year::LONG > 1000 + does_not_exist2::DOUBLE)
            | EVAL e = does_not_exist3 + 1
            """, "line 7:12: Unknown column [does_not_exist3]");
    }

    public void testFailMetadataFieldInKeep() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | KEEP " + field, "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInEval() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | EVAL x = " + field, "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInWhere() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | WHERE " + field + " IS NOT NULL", "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInSort() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | SORT " + field, "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInStats() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | STATS x = COUNT(" + field + ")", "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInRename() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | RENAME " + field + " AS renamed", "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldAfterStats() {
        assertUnmappedFailure(test(), """
            FROM test
            | STATS c = COUNT(*)
            | KEEP _score
            """, "Unknown column [_score]");
    }

    public void testFailMetadataFieldInFork() {
        assertUnmappedFailure(test(), """
            FROM test
            | FORK (WHERE _score > 1)
                   (WHERE salary > 50000)
            """, "Unknown column [_score]");
    }

    public void testFailMetadataFieldInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedFailure(test(), """
            FROM
                (FROM test
                 | WHERE _score > 1)
            """, "Unknown column [_score]");
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_score{m}#5]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, ...]
     * }
     */
    public void testMetadataFieldDeclaredNullify() {
        // This isn't gilded since it would just create a bunch of clutter due to nesting.
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var plan = test().statement(setUnmappedNullify("FROM test METADATA " + field + " | KEEP " + field));

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

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_score{m}#5]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, ...]
     * }
     */
    public void testMetadataFieldDeclaredLoad() {
        // This isn't gilded since it would just create a bunch of clutter due to nesting.
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var plan = test().statement(setUnmappedLoad("FROM test METADATA " + field + " | KEEP " + field));

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
        analyzer().addK8sDownsampled().statementError(setUnmappedNullify("""
            TS k8s
            | RENAME @timestamp AS newTs
            | STATS max(rate(network.total_cost)) BY tbucket = BUCKET(newTs, 1hour)
            """), containsString("3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX));

        analyzer().addK8sDownsampled().statementError(setUnmappedNullify("""
            TS k8s
            | DROP @timestamp
            | STATS max(rate(network.total_cost))
            """), containsString("3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX));
    }

    public void testLoadModeDisallowsFork() {
        test().statementError(
            setUnmappedLoad("FROM test | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)"),
            containsString("line 1:41: FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsForkWithStats() {
        test().statementError(
            setUnmappedLoad("FROM test | FORK (STATS c = COUNT(*)) (STATS d = AVG(salary))"),
            containsString("line 1:41: FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsForkWithMultipleBranches() {
        test().statementError(
            setUnmappedLoad("FROM test | FORK (WHERE emp_no > 1) (WHERE emp_no < 100) (WHERE salary > 50000)"),
            containsString("line 1:41: FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsForkAfterLinearPipeline() {
        test().statementError(
            setUnmappedLoad("FROM test | WHERE emp_no > 1 | FORK (WHERE salary > 50000) (WHERE salary < 30000)"),
            containsString("line 1:60: FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsForkWithUnmappedFieldInBranch() {
        test().statementError(
            setUnmappedLoad("FROM test | FORK (KEEP emp_no, does_not_exist) (WHERE salary > 50000)"),
            containsString("line 1:41: FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsLookupJoin() {
        assertUnmappedLoadError(
            test().addLanguagesLookup(),
            "FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code",
            containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testNullifyLookupJoinUnknownLeftField() {
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify("FROM test | LOOKUP JOIN languages_lookup ON language_code"),
                containsString("Unknown column [language_code] in left side of join")
            );
    }

    public void testNullifyLookupJoinUnknownRightField() {
        test().addLanguagesLookup()
            .statementError(
                setUnmappedNullify("FROM test | LOOKUP JOIN languages_lookup ON does_not_exist"),
                containsString("Unknown column [does_not_exist] in right side of join")
            );
    }

    public void testNullifyLookupJoinExpressionWithNullifiedFields() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        for (var onClauseAndError : List.of(
            Tuple.tuple("does_not_exist == does_not_exist2", null),
            Tuple.tuple("emp_no == does_not_exist", null),
            Tuple.tuple("languages == language_code AND emp_no == does_not_exist", "emp_no == does_not_exist")
        )) {
            test().addLanguagesLookup()
                .statementError(
                    setUnmappedNullify("FROM test | LOOKUP JOIN languages_lookup ON " + onClauseAndError.v1()),
                    containsString(
                        "Unsupported join filter expression:"
                            + (onClauseAndError.v2() == null ? onClauseAndError.v1() : onClauseAndError.v2())
                    )
                );
        }
    }

    public void testLoadModeDisallowsLookupJoinAfterFilter() {
        assertUnmappedLoadError(test().addLanguagesLookup(), """
            FROM test
            | WHERE emp_no > 1
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | KEEP emp_no, language_name
            """, containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\""));
    }

    public void testLoadModeDisallowsForkAndLookupJoin() {
        test().addLanguagesLookup()
            .statementError(
                setUnmappedLoad("""
                    FROM test
                    | EVAL language_code = languages
                    | LOOKUP JOIN languages_lookup ON language_code
                    | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)
                    """),
                allOf(
                    containsString("Found 3 problems"),
                    containsString("line 4:3: FORK is not supported with unmapped_fields=\"load\""),
                    containsString("line 3:15: LOOKUP JOIN is not supported with unmapped_fields=\"load\""),
                    containsString("line 3:15: LOOKUP JOIN is not supported with unmapped_fields=\"load\"")
                )
            );
    }

    public void testLoadMode_AllowsSingleSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(setUnmappedLoad("FROM (FROM test)"));
    }

    public void testLoadMode_AllowsSingleSubqueryInFrom_WithWhere() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(setUnmappedLoad("FROM (FROM test | WHERE emp_no > 1)"));
    }

    public void testLoadMode_AllowsSingleSubqueryInFrom_WithEval() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(setUnmappedLoad("FROM (FROM test | EVAL x = emp_no + 1)"));
    }

    public void testLoadMode_AllowsSingleSubqueryInFrom_WithStats() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(setUnmappedLoad("FROM (FROM test | STATS c = COUNT(*))"));
    }

    public void testLoadMode_AllowsSingleSubqueryInFrom_WithSort() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(setUnmappedLoad("FROM (FROM test | SORT emp_no | LIMIT 10)"));
    }

    public void testLoadModeDisallowsMainIndexPlusSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguages()
            .statementError(
                setUnmappedLoad("FROM test, (FROM languages | WHERE language_code > 1)"),
                containsString("line 1:40: Subqueries and views are not supported with unmapped_fields=\"load\"")
            );
    }

    public void testLoadModeDisallowsTwoSubqueriesWithoutMainIndex() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statementError(
            setUnmappedLoad("FROM (FROM test),(FROM test)"),
            allOf(
                containsString("Found 2 problems"),
                containsString("line 1:34: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:46: Subqueries and views are not supported with unmapped_fields=\"load\"")
            )
        );
    }

    public void testLoadModeDisallowsThreeSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statementError(
            setUnmappedLoad("FROM (FROM test),(FROM test),(FROM test)"),
            allOf(
                containsString("Found 3 problems"),
                containsString("line 1:34: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:46: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:58: Subqueries and views are not supported with unmapped_fields=\"load\"")
            )
        );
    }

    public void testLoadModeDisallowsNestedSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguages()
            .addSampleData()
            .statementError(
                setUnmappedLoad("FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)"),
                allOf(
                    containsString("Found 2 problems"),
                    containsString("line 1:40: Subqueries and views are not supported with unmapped_fields=\"load\""),
                    containsString("line 1:57: Subqueries and views are not supported with unmapped_fields=\"load\"")
                )
            );
    }

    public void testLoadModeDisallowsSubqueryWithLookupJoin() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedLoadError(
            test().addLanguagesLookup(),
            """
                FROM test,
                    (FROM test
                    | EVAL language_code = languages
                    | LOOKUP JOIN languages_lookup ON language_code)
                """,
            allOf(
                containsString("Found 2 problems"),
                containsString("line 2:5: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 4:19: LOOKUP JOIN is not supported with unmapped_fields=\"load\"")
            )
        );
    }

    public void testLoadModeDisallowsSingleSubqueryPlusFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statementError(
            setUnmappedLoad("FROM (FROM test) | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)"),
            containsString("line 1:48: FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsMultipleSubqueriesPlusFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statementError(
            setUnmappedLoad("FROM (FROM test),(FROM test) | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)"),
            allOf(
                containsString("Found 7 problems"),
                containsString("line 1:60: FORK is not supported with unmapped_fields=\"load\""),
                containsString("line 1:34: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:46: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:34: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:46: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:34: FORK after subquery is not supported"),
                containsString("line 1:34: FORK after subquery is not supported")
            )
        );
    }

    public void testLoadModeDisallowsSubqueryAndFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var query = setUnmappedLoad("""
            FROM test, (FROM languages | WHERE language_code > 1)
            | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)
            """);
        test().addLanguages()
            .statementError(
                query,
                allOf(
                    containsString("Found 5 problems"),
                    containsString("line 2:3: FORK is not supported with unmapped_fields=\"load\""),
                    // error below appears twice
                    containsString("line 1:40: Subqueries and views are not supported with unmapped_fields=\"load\""),
                    // error below appears twice
                    containsString("line 1:34: FORK after subquery is not supported")
                )
            );
    }

    public void testLoadModeAllowsNonBranchingViewEquivalent() {
        test().statement(setUnmappedLoad("FROM test | WHERE emp_no > 1 | KEEP emp_no, does_not_exist"));
    }

    public void testLoadModeAllowsNonBranchingViewEquivalentWithEval() {
        test().statement(setUnmappedLoad("FROM test | WHERE emp_no > 1 | EVAL x = does_not_exist | KEEP emp_no, x"));
    }

    public void testLoadModeAllowsNonBranchingViewEquivalentWithStats() {
        test().statement(setUnmappedLoad("FROM test | WHERE emp_no > 1 | STATS c = COUNT(*) BY does_not_exist"));
    }

    public void testLoadModeAllowsNonBranchingViewEquivalentWithSort() {
        test().statement(setUnmappedLoad("FROM test | WHERE emp_no > 1 | SORT does_not_exist | KEEP emp_no, does_not_exist"));
    }

    public void testLoadModeAllowsNonBranchingViewEquivalentWithRename() {
        test().statement(setUnmappedLoad("FROM test | RENAME first_name AS fname | KEEP fname, does_not_exist"));
    }

    public void testLoadModeDisallowsBranchingViewEquivalent() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statementError(
            setUnmappedLoad("FROM (FROM test | WHERE emp_no > 1),(FROM test | WHERE emp_no < 100)"),
            allOf(
                containsString("Found 2 problems"),
                containsString("line 1:34: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:65: Subqueries and views are not supported with unmapped_fields=\"load\"")
            )
        );
    }

    public void testLoadModeDisallowsBranchingViewEquivalentWithUnmappedField() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statementError(
            setUnmappedLoad("FROM (FROM test | WHERE emp_no > 1),(FROM test | WHERE emp_no < 100) | KEEP emp_no, does_not_exist"),
            allOf(
                containsString("Found 2 problems"),
                containsString("line 1:34: Subqueries and views are not supported with unmapped_fields=\"load\""),
                containsString("line 1:65: Subqueries and views are not supported with unmapped_fields=\"load\"")
            )
        );
    }

    public void testTypeConflictLongUnmappedNoCast() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("bar", Map.of())
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("foo,bar", caps, true));
        for (String suffix : TYPE_CONFLICT_QUERY_SUFFIXES) {
            typeConflictVerificationFailure(setUnmappedLoad("FROM foo, bar " + suffix), resolutions);
        }
    }

    public void testTypeConflictLongKeywordUnmappedNoCast() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("bar", fieldResponseMap("message", "keyword")),
                fieldCapabilitiesIndexResponse("baz", Map.of())
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("foo,bar,baz", caps, true));
        for (String suffix : TYPE_CONFLICT_QUERY_SUFFIXES) {
            typeConflictVerificationFailure(setUnmappedLoad("FROM foo, bar, baz " + suffix), resolutions);
        }
    }

    public void testTypeConflictLongIntUnmappedNoCast() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("bar", fieldResponseMap("message", "integer")),
                fieldCapabilitiesIndexResponse("baz", Map.of())
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("foo,bar,baz", caps, true));
        for (String suffix : TYPE_CONFLICT_QUERY_SUFFIXES) {
            typeConflictVerificationFailure(setUnmappedLoad("FROM foo, bar, baz " + suffix), resolutions);
        }
    }

    public void testTypeConflictTextUnmappedNoCast() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("message", "text")),
                fieldCapabilitiesIndexResponse("bar", Map.of())
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("foo,bar", caps, true));
        for (String suffix : TYPE_CONFLICT_QUERY_SUFFIXES) {
            typeConflictVerificationFailure(setUnmappedLoad("FROM foo, bar " + suffix), resolutions);
        }
    }

    public void testSameMappingHashNotPartiallyUnmapped() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("bar", fieldResponseMap("message", "long"))
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("foo,bar", caps, true));
        TestAnalyzer ta = analyzer();
        for (var entry : resolutions.entrySet()) {
            ta.addIndex(entry.getKey().indexPattern(), entry.getValue());
        }
        var plan = ta.statement(setUnmappedLoad("FROM foo, bar | EVAL x = message + 1"));
        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Eval.class);
        var attr = eval.output().stream().filter(a -> a.name().equals("message")).findFirst().orElseThrow();
        assertThat(attr.dataType(), is(DataType.LONG));
    }

    public void testSameMappingHashWithUnmappedIndex() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("bar", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("baz", Map.of())
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("foo,bar,baz", caps, true));
        TestAnalyzer ta = analyzer();
        for (var entry : resolutions.entrySet()) {
            ta.addIndex(entry.getKey().indexPattern(), entry.getValue());
        }
        var e = expectThrows(VerificationException.class, () -> ta.statement(setUnmappedLoad("FROM foo, bar, baz | SORT message")));
        assertThat(e.getMessage(), partiallyUnmappedNonKeywordError("message"));
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
            test().statementError(statement, allOf(containsString("[tbucket(1 hour)] "), not(containsString("FORK is not supported"))));
        }
    }

    /**
     * Verify that partially-mapped fields of ALL non-keyword types are NOT converted to
     * {@link PotentiallyUnmappedKeywordEsField}, but are instead marked as potentially unmapped via {@link InvalidMappedField}.
     * This iterates over all {@link DataType} values that can appear as ES mapped field types.
     */
    public void testPartiallyMappedNonKeywordFieldsMarkedAsPotentiallyUnmapped() {
        // Types that cannot appear as regular ES mapped fields in an EsIndex mapping
        Set<DataType> excludedTypes = Set.of(
            DataType.KEYWORD,           // this is the type we DO convert — not a negative test case
            DataType.NULL,              // not a real mapped field type
            DataType.UNSUPPORTED,       // not a real mapped field type
            DataType.DOC_DATA_TYPE,     // internal _doc type
            DataType.TSID_DATA_TYPE,    // internal _tsid type
            DataType.SOURCE,            // internal _source type
            DataType.DATE_PERIOD,       // ESQL-internal, not an ES mapping type
            DataType.TIME_DURATION,     // ESQL-internal, not an ES mapping type
            DataType.OBJECT,            // not a leaf field type
            DataType.GEOHASH,           // ESQL-internal grid type, not a real ES mapped field type
            DataType.GEOTILE,           // ESQL-internal grid type, not a real ES mapped field type
            DataType.GEOHEX             // ESQL-internal grid type, not a real ES mapped field type
        );

        for (DataType dataType : DataType.values()) {
            if (excludedTypes.contains(dataType)) {
                continue;
            }
            // Build a minimal mapping: one keyword field (emp_no stand-in for SORT) and one field of the type under test,
            // with the latter wrapped as InvalidMappedField.potentiallyUnmapped (as IndexResolver would do in production).
            Map<String, EsField> mapping = Map.of(
                "sort_field",
                new EsField("sort_field", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
                "test_field",
                InvalidMappedField.potentiallyUnmapped("test_field", Map.of(dataType.widenSmallNumeric().typeName(), Set.of("test1")))
            );

            var plan = analyzer().addIndex(
                new EsIndex("test*", mapping, Map.of("test1", IndexMode.STANDARD, "test2", IndexMode.STANDARD), Map.of(), Map.of())
            ).statement(setUnmappedLoad("""
                FROM test*
                | SORT sort_field
                """));

            var limit = as(plan, Limit.class);
            var order = as(limit.child(), OrderBy.class);
            var relation = as(order.child(), EsRelation.class);

            var testFieldAttr = relation.output().stream().filter(a -> a.name().equals("test_field")).findFirst().orElseThrow();
            var fieldAttr = as(testFieldAttr, FieldAttribute.class);
            assertThat(
                "Partially-mapped " + dataType + " field should not be converted to PotentiallyUnmappedKeywordEsField",
                fieldAttr.field(),
                not(instanceOf(PotentiallyUnmappedKeywordEsField.class))
            );
            assertThat(
                "Partially-mapped " + dataType + " field should be reverted to a regular field with its original type",
                fieldAttr.dataType(),
                is(dataType.widenSmallNumeric())
            );
        }
    }

    public void testWrapPartiallyUnmappedFieldWidensSmallNumerics() {
        Set<String> mappedIndices = Set.of("idx_mapped");
        for (DataType smallNumeric : List.of(DataType.SHORT, DataType.BYTE, DataType.FLOAT, DataType.HALF_FLOAT, DataType.SCALED_FLOAT)) {
            EsField field = new EsField("f", smallNumeric, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
            InvalidMappedField wrapped = (InvalidMappedField) IndexResolver.wrapPartiallyUnmappedField(field, "f", "f", mappedIndices);
            assertThat(
                "Partially-unmapped " + smallNumeric + " field should be stored under its widened type name",
                wrapped.getTypesToIndices(),
                equalTo(Map.of(smallNumeric.widenSmallNumeric().typeName(), mappedIndices))
            );
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
            test().addLanguagesLookup()
                .statementError(
                    statement,
                    allOf(
                        containsString("Found 1 problem"),
                        containsString(
                            "line 4:25: [tbucket(1 hour)] requires the [@timestamp] "
                                + "field, which was either not present in the source index, "
                                + "or has been dropped or renamed; the [unmapped_fields] "
                                + "setting does not apply to the implicit @timestamp reference"
                        ),
                        not(containsString("LOOKUP JOIN is not supported"))
                    )
                );
        }
    }

    public void testTbucketWithTimestampPresent() {
        var query = "FROM sample_data | STATS c = COUNT(*) BY tbucket(1 hour)";
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var plan = analyzer().addSampleData().statement(statement);
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
            var plan = analyzer().addSampleData().statement(statement);
            var limit = as(plan, Limit.class);
            var filter = as(limit.child(), Filter.class);
            var relation = as(filter.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("sample_data"));
            assertTimestampInOutput(relation);
        }
    }

    public void testTbucketTimestampPresentButDroppedNullify() {
        analyzer().addSampleData()
            .statementError(
                setUnmappedNullify("FROM sample_data | DROP @timestamp | STATS c = COUNT(*) BY tbucket(1 hour)"),
                allOf(containsString(UnresolvedTimestamp.UNRESOLVED_SUFFIX), not(containsString(Verifier.UNMAPPED_TIMESTAMP_SUFFIX)))
            );
    }

    public void testTbucketTimestampPresentButRenamedNullify() {
        analyzer().addSampleData()
            .statementError(
                setUnmappedNullify("FROM sample_data | RENAME @timestamp AS ts | STATS c = COUNT(*) BY tbucket(1 hour)"),
                allOf(containsString(UnresolvedTimestamp.UNRESOLVED_SUFFIX), not(containsString(Verifier.UNMAPPED_TIMESTAMP_SUFFIX)))
            );
    }

    private static void assertTimestampInOutput(EsRelation relation) {
        assertTrue(
            "@timestamp field should be present in the EsRelation output",
            relation.output().stream().anyMatch(a -> MetadataAttribute.TIMESTAMP_FIELD.equals(a.name()))
        );
    }

    private void unmappedTimestampFailure(String query, String... failures) {
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            test().statementError(
                statement,
                allOf(() -> Iterators.map(Iterators.forArray(failures), s -> containsString(s + UNMAPPED_TIMESTAMP_SUFFIX)))
            );
        }
    }

    /**
     * Verify that referencing a sub-field of a flattened field (e.g. "foo.bar" when "foo" is flattened) is rejected
     */
    public void testFlattenedSubFieldRejectionWithSimpleCases() {
        assertUnmappedLoadError(index1(), "FROM test | KEEP field.a", unmappedLoadAndFlattenedSubfieldHelper("field.a", "field"));
        assertUnmappedLoadError(
            index1(),
            "FROM test | STATS x = SAMPLE(field.a, 1)",
            unmappedLoadAndFlattenedSubfieldHelper("field.a", "field")
        );
        assertUnmappedLoadError(
            index1(),
            "FROM test | EVAL x = TO_STRING(field.a)",
            unmappedLoadAndFlattenedSubfieldHelper("field.a", "field")
        );
        assertUnmappedLoadError(index1(), "FROM test | KEEP field.a.b", unmappedLoadAndFlattenedSubfieldHelper("field.a.b", "field"));
        assertUnmappedLoadError(index1(), "FROM test | KEEP field.a.b.c", unmappedLoadAndFlattenedSubfieldHelper("field.a.b.c", "field"));
        assertUnmappedLoadError(index1(), "FROM test | SORT field.x, field.z", unmappedLoadAndFlattenedSubfieldHelper("field.x", "field"));
        assertUnmappedLoadError(
            index1(),
            "FROM test | SORT field.x | KEEP field.z",
            unmappedLoadAndFlattenedSubfieldHelper("field.x", "field", "field.z", "field")
        );
        assertUnmappedLoadError(index1(), "FROM test | KEEP field | KEEP field.a", containsString("Unknown column [field.a]"));
        assertUnmappedLoadError(
            index1(),
            "FROM test | KEEP field | WHERE field.sub.subfield == \"x\"",
            containsString("Unknown column [field.sub.subfield]")
        );
        assertUnmappedLoadError(
            index1(),
            "FROM test | KEEP field | WHERE field.a.b == \"x\" | KEEP field.a",
            containsString("Unknown column [field.a.b], did you mean [field]?")
        );
    }

    /**
     * Verify that referencing a sub-field of a flattened field is rejected in a FORK
     */
    public void testFlattenedSubFieldRejectionWithFork() {
        assertUnmappedLoadError(
            index1(),
            """
                FROM test
                | eval aaa = field.aaa
                | FORK (eval x = resource.attributes.host.name) (eval y = attributes.xxx) (eval z = field.bbb)
                """,
            allOf(
                containsString("Found 5 problems"),
                containsString("line 3:3: FORK is not supported with unmapped_fields=\"load\""),
                // this error appears 3 times thanks to the FORK branching out
                containsString(
                    "line 2:14: Loading subfield [field.aaa] when parent [field] is of flattened field type is not supported with "
                        + "unmapped_fields=\"load\""
                ),
                containsString(
                    "line 3:85: Loading subfield [field.bbb] when parent [field] is of flattened field type is not supported with "
                        + "unmapped_fields=\"load\""
                )
            )
        );
    }

    /**
     * Verify that referencing a sub-field of a flattened field is rejected in a LOOKUP JOIN
     */
    public void testFlattenedSubFieldRejectionWithLookupJoin() {
        assertUnmappedLoadError(
            index1().addLanguagesLookup(),
            """
                FROM test
                | EVAL language_code = 1
                | LOOKUP JOIN languages_lookup ON language_code
                | EVAL x = field.languages
                """,
            allOf(
                containsString("Found 2 problems"),
                containsString("line 3:15: LOOKUP JOIN is not supported with unmapped_fields=\"load\""),
                containsString(
                    "line 4:12: Loading subfield [field.languages] when parent [field] is of flattened field type is not supported with "
                        + "unmapped_fields=\"load\""
                )
            )
        );
    }

    /**
     * Verify that PromQL queries are rejected when unmapped_fields=load
     */
    public void testUnmappedFieldLoadRejectionWithPromQl() {
        TestAnalyzer analyzer = test().addIndex("test", "tsdb-mapping.json");

        assertUnmappedLoadError(
            analyzer,
            "PROMQL index=test step=5m avg(network.bytes_in)",
            allOf(containsString("Found 1 problem"), containsString("line 1:29: PROMQL is not supported with unmapped_fields=\"load\""))
        );

        assertUnmappedLoadError(
            analyzer,
            "PROMQL index=test step=5m rate(network.bytes_in[5m])",
            allOf(containsString("Found 1 problem"), containsString("line 1:29: PROMQL is not supported with unmapped_fields=\"load\""))
        );

        assertUnmappedLoadError(
            analyzer,
            "PROMQL index=test step=5m avg(network.bytes_in) + avg(network.bytes_out)",
            allOf(containsString("Found 1 problem"), containsString("line 1:29: PROMQL is not supported with unmapped_fields=\"load\""))
        );

        assertUnmappedLoadError(
            analyzer,
            "PROMQL index=test start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T01:00:00Z\" buckets=10 avg(network.bytes_in)",
            allOf(containsString("Found 1 problem"), containsString("line 1:29: PROMQL is not supported with unmapped_fields=\"load\""))
        );
    }

    /**
     * When unmapped_fields=load and an index has a partially mapped field that is not KEYWORD (e.g. LONG),
     * analysis must fail once that field is used outside {@link EsRelation}.
     * Covers one offending field; see {@link #testDisallowLoadWithPartiallyMappedNonKeywordReportsAllFields} for multiple.
     */
    public void testDisallowLoadWithPartiallyMappedNonKeyword() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        assertUnmappedLoadError(
            analyzer().addIndex(esIndex),
            "FROM idx* | WHERE partial_long > 0",
            partiallyUnmappedNonKeywordError("partial_long")
        );
    }

    public void testDisallowLoadWithPartiallyMappedNonKeywordReportsAllFields() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(
            Map.of("partial_long", longField("partial_long"), "partial_double", doubleField("partial_double")),
            Set.of("partial_long", "partial_double")
        );
        assertUnmappedLoadError(
            analyzer().addIndex(esIndex),
            "FROM idx* | SORT partial_long, partial_double",
            allOf(
                containsString("Found 2 problems"),
                partiallyUnmappedNonKeywordError("partial_long"),
                partiallyUnmappedNonKeywordError("partial_double")
            )
        );
    }

    /**
     * An EVAL referencing both a partially unmapped non-keyword field and a field with a genuine type conflict
     * should report errors for both fields.
     */
    public void testDisallowLoadWithPartialNonKeywordAndTypeConflictInSameEval() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var conflicted = new InvalidMappedField(
            "conflicted",
            Map.of(DataType.LONG.typeName(), Set.of("idx_a"), DataType.DOUBLE.typeName(), Set.of("idx_b"))
        );
        var partialLong = InvalidMappedField.potentiallyUnmapped(
            "partial_long",
            Map.of(DataType.LONG.typeName(), Set.of("idx_a", "idx_b"))
        );
        var merged = new EsIndex(
            "idx*",
            Map.of("partial_long", partialLong, "conflicted", conflicted),
            Map.of("idx_a", IndexMode.STANDARD, "idx_b", IndexMode.STANDARD, "idx_unmapped", IndexMode.STANDARD),
            Map.of(),
            Map.of()
        );
        assertUnmappedLoadError(
            analyzer().addIndex("idx*", IndexResolution.valid(merged)),
            "FROM idx* | EVAL x = partial_long + 1, y = conflicted + 1",
            allOf(
                containsString("Found 2 problems"),
                partiallyUnmappedNonKeywordError("partial_long"),
                containsString("Cannot use field [conflicted]")
            )
        );
    }

    public void testAllowLoadWithPartialNonKeywordWhenFieldNotReferenced() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(
            Map.of("partial_long", longField("partial_long"), "common", keywordField("common")),
            Set.of("partial_long")
        );
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx* | KEEP common"));
        var limit = as(plan, Limit.class);
        // partial_long must not appear in the output — only the non-PUNK field that was explicitly kept
        assertThat(Expressions.names(limit.output()), is(List.of("common")));
        assertThat(limit.output().getFirst().dataType(), is(DataType.KEYWORD));
    }

    /**
     * Comma-separated {@code FROM} resolves to one merged {@link EsIndex} named {@code idx_a,idx_b}; partial-field checks must use that
     * resolution (see {@link IndexResolution#matches}).
     */
    public void testAllowLoadCommaSeparatedIndicesWhenPartialNonKeywordUnused() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var pattern = "idx_a,idx_b";
        var partialLong = InvalidMappedField.potentiallyUnmapped("partial_long", Map.of(DataType.LONG.typeName(), Set.of("idx_a")));
        var merged = new EsIndex(
            pattern,
            Map.of("partial_long", partialLong, "common", keywordField("common")),
            Map.of("idx_a", IndexMode.STANDARD, "idx_b", IndexMode.STANDARD),
            Map.of(),
            Map.of()
        );
        var plan = analyzer().addIndex(pattern, IndexResolution.valid(merged))
            .statement(setUnmappedLoad("FROM idx_a, idx_b | KEEP common"));
        assertThat(plan, not(nullValue()));
    }

    public void testDisallowLoadCommaSeparatedIndicesWhenPartialNonKeywordUsed() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var pattern = "idx_a,idx_b";
        var partialLong = InvalidMappedField.potentiallyUnmapped("partial_long", Map.of(DataType.LONG.typeName(), Set.of("idx_a")));
        var merged = new EsIndex(
            pattern,
            Map.of("partial_long", partialLong, "common", keywordField("common")),
            Map.of("idx_a", IndexMode.STANDARD, "idx_b", IndexMode.STANDARD),
            Map.of(),
            Map.of()
        );
        assertUnmappedLoadError(
            analyzer().addIndex(pattern, IndexResolution.valid(merged)),
            "FROM idx_a, idx_b | WHERE partial_long > 0",
            partiallyUnmappedNonKeywordError("partial_long")
        );
    }

    public void testAllowLoadFromOnlyWhenPartialNonKeywordUnused() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx*"));
        assertThat(plan, not(nullValue()));
    }

    public void testDisallowLoadWithPartiallyMappedNonKeywordInRename() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(
            Map.of("partial_long", longField("partial_long"), "common", keywordField("common")),
            Set.of("partial_long")
        );
        var analyzer = analyzer().addIndex(esIndex);

        assertUnmappedLoadError(analyzer, "FROM idx* | RENAME partial_long AS pl", partiallyUnmappedNonKeywordError("partial_long"));

        assertUnmappedLoadError(
            analyzer,
            "FROM idx* | RENAME common as c, partial_long AS pl",
            partiallyUnmappedNonKeywordError("partial_long")
        );
    }

    public void testDisallowLoadWithPartiallyMappedNonKeywordInSort() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        assertUnmappedLoadError(
            analyzer().addIndex(esIndex),
            "FROM idx* | SORT partial_long",
            partiallyUnmappedNonKeywordError("partial_long")
        );
    }

    /**
     * Same rule as {@link #testDisallowLoadWithPartiallyMappedNonKeyword} exercised through additional commands
     * ({@code CHANGE_POINT} and {@code MV_EXPAND}) to ensure the check is not accidentally tied to a specific command.
     * A regression that bypasses the verifier for one of these commands would cause its test to fail.
     */
    public void testDisallowLoadWithPartiallyMappedNonKeywordInChangePoint() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
        assumeTrue("Requires CHANGE_POINT", EsqlCapabilities.Cap.CHANGE_POINT.isEnabled());

        var esIndex = partialIndex(
            Map.of(
                "partial_long",
                longField("partial_long"),
                "@timestamp",
                new EsField("@timestamp", DataType.DATETIME, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            Set.of("partial_long")
        );
        assertUnmappedLoadError(
            analyzer().addIndex(esIndex),
            "FROM idx* | CHANGE_POINT partial_long ON @timestamp AS type, pvalue",
            partiallyUnmappedNonKeywordError("partial_long")
        );
    }

    /** See {@link #testDisallowLoadWithPartiallyMappedNonKeywordInChangePoint}. */
    public void testDisallowLoadWithPartiallyMappedNonKeywordInMvExpand() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        assertUnmappedLoadError(
            analyzer().addIndex(esIndex),
            "FROM idx* | MV_EXPAND partial_long",
            partiallyUnmappedNonKeywordError("partial_long")
        );
    }

    public void testDisallowLoadWithPartiallyMappedNonKeywordDottedPath() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var sub = InvalidMappedField.potentiallyUnmapped("sub", Map.of(DataType.LONG.typeName(), Set.of("idx_mapped")));
        var obj = new EsField("obj", DataType.OBJECT, Map.of("sub", sub), true, EsField.TimeSeriesFieldType.NONE);
        var esIndex = new EsIndex("idx*", Map.of("obj", obj), Map.of("idx_mapped", IndexMode.STANDARD), Map.of(), Map.of());
        assertUnmappedLoadError(analyzer().addIndex(esIndex), "FROM idx* | SORT `obj.sub`", partiallyUnmappedNonKeywordError("obj.sub"));
    }

    /**
     * {@code @timestamp} resolved as date/date_nanos union across two indices, with a third index where it is outright unmapped. Under
     * {@code unmapped_fields=load}, this still fails because {@code @timestamp} is partially unmapped and used in {@code WHERE}.
     */
    public void testDisallowLoadWithPartialUnionTimestampInWhere() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var pattern = "sample_data,sample_data_ts_nanos,no_mapping_sample_data";
        var tsField = InvalidMappedField.potentiallyUnmapped(
            "@timestamp",
            Map.of(DataType.DATETIME.typeName(), Set.of("sample_data"), DataType.DATE_NANOS.typeName(), Set.of("sample_data_ts_nanos"))
        );
        var merged = new EsIndex(
            pattern,
            Map.of("@timestamp", tsField),
            Map.of(
                "sample_data",
                IndexMode.STANDARD,
                "sample_data_ts_nanos",
                IndexMode.STANDARD,
                "no_mapping_sample_data",
                IndexMode.STANDARD
            ),
            Map.of(),
            Map.of()
        );
        assertUnmappedLoadError(
            analyzer().addIndex(pattern, IndexResolution.valid(merged)),
            "FROM sample_data, sample_data_ts_nanos, no_mapping_sample_data METADATA _index "
                + "| WHERE @timestamp == \"2021-01-01\"::date_nanos",
            allOf(
                containsString("Found 1 problem"),
                containsString("line 1:116: Cannot use field [@timestamp] due to ambiguities being mapped as [3] incompatible types: "),
                containsString("[keyword] due to loading from _source"),
                containsString("[date_nanos] in [sample_data_ts_nanos]"),
                containsString("[datetime] in [sample_data]")
            )
        );
    }

    public void testAllowLoadWithKeepDrop() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(
            Map.of("partial_long", longField("partial_long"), "common", keywordField("common")),
            Set.of("partial_long")
        );
        var analyzer = analyzer().addIndex(esIndex);

        String[] queries = new String[] {
            "FROM idx* | KEEP common",
            "FROM idx* | KEEP partial_long",
            "FROM idx* | KEEP partial_long, common",
            "FROM idx* | KEEP c*, p*",
            "FROM idx* | DROP partial_long",
            "FROM idx* | DROP common",
            "FROM idx* | DROP c*",
            "FROM idx* | DROP p*",
            "FROM idx* | DROP partial_long | KEEP common", };
        String suffix = randomFrom("", "| EVAL foo = 1", "| STATS count(*)", "| LIMIT 10");
        LogicalPlan plan;
        for (String query : queries) {
            plan = analyzer.statement(setUnmappedLoad(query + suffix));
            assertThat(plan, not(nullValue()));
        }
    }

    public void testAllowLoadWithPartiallyMappedKeyword() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_type_keyword", keywordField("partial_type_keyword")), Set.of("partial_type_keyword"));
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx* | KEEP partial_type_keyword"));
        assertThat(plan, not(nullValue()));
    }

    public void testNullifyWithPartiallyMappedNonKeywordDoesNotFail() {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedNullify("FROM idx* | WHERE partial_long IS NOT NULL"));
        assertThat(plan, not(nullValue()));
    }

    /**
     * With {@code unmapped_fields=load}, referencing a partially unmapped non-KEYWORD field only in {@code FROM} (not downstream)
     * must succeed — the check fires only when the field is used outside the source relation.
     */
    public void testPartiallyUnmappedNonKeywordIsAllowedWithLoad_WhenNotReferenced() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        // partial_long is in the index but not referenced in any downstream expression — no PUNK violation
        assertNotNull(analyzer().addIndex(esIndex).statement("SET unmapped_fields=\"load\"; FROM idx*"));
    }

    private Matcher<String> unmappedLoadAndFlattenedSubfieldHelper(String... pairs) {
        assert pairs.length % 2 == 0;
        String errorMessage =
            "Loading subfield [%s] when parent [%s] is of flattened field type is not supported with unmapped_fields=\"load\"";
        List<Matcher<? super String>> errors = new ArrayList<>();

        for (int i = 0; i < pairs.length; i += 2) {
            errors.add(containsString(String.format(Locale.ROOT, errorMessage, pairs[i], pairs[i + 1])));
        }

        return allOf(errors);
    }

    private void assertUnmappedFailure(TestAnalyzer analyzer, String query, String... failures) {
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            analyzer.statementError(statement, allOf(() -> Iterators.map(Iterators.forArray(failures), Matchers::containsString)));
        }
    }

    private static TestAnalyzer test() {
        return analyzer().addEmployees("test");
    }

    private static TestAnalyzer index1() {
        Map<String, EsField> mapping = Map.of("field", new UnsupportedEsField("field", List.of("flattened")));
        return analyzer().addIndex(new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD), Map.of(), Map.of()));
    }

    private static void assertUnmappedLoadError(TestAnalyzer analyzer, String query, Matcher<String> matcher) {
        analyzer.statementError(setUnmappedLoad(query), matcher);
    }

    private void typeConflictVerificationFailure(String statement, Map<IndexPattern, IndexResolution> indexResolutions) {
        TestAnalyzer ta = analyzer();
        for (var entry : indexResolutions.entrySet()) {
            ta.addIndex(entry.getKey().indexPattern(), entry.getValue());
        }
        var e = expectThrows(VerificationException.class, () -> ta.statement(statement));
        // Single-type partially unmapped fields are caught explicitly by the Verifier; multi-type conflicts are caught by
        // being marked with UnsupportedAttributes, whose error message mentions the type conflicts.
        assertThat(
            e.getMessage(),
            Matchers.anyOf(partiallyUnmappedNonKeywordError("message"), containsString("Cannot use field [message]"))
        );
    }

    private static EsIndex partialIndex(Map<String, EsField> mapping, Set<String> partialFieldNames) {
        Set<String> mappedIndices = Set.of("idx_mapped");
        Map<String, EsField> wrappedMapping = new HashMap<>(mapping);
        for (String fieldName : partialFieldNames) {
            EsField field = wrappedMapping.get(fieldName);
            wrappedMapping.put(fieldName, IndexResolver.wrapPartiallyUnmappedField(field, fieldName, fieldName, mappedIndices));
        }
        return new EsIndex("idx*", wrappedMapping, Map.of("idx_mapped", IndexMode.STANDARD), Map.of(), Map.of());
    }

    private static EsField longField(String name) {
        return new EsField(name, DataType.LONG, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
    }

    private static EsField doubleField(String name) {
        return new EsField(name, DataType.DOUBLE, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
    }

    private static EsField keywordField(String name) {
        return new EsField(name, DataType.KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
    }

    private static String setUnmappedNullify(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
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
        var analyzer = test();
        analyzer.statementError(
            setUnmappedLoad("FROM test | WHERE first_name:\"foo\" | KEEP first_name"),
            allOf(
                containsString("Found 1 problem"),
                containsString(
                    "line 1:47: unmapped_fields=\"load\" does not support full-text search function [:]; use \"default\" or \"nullify\""
                )
            )
        );
        analyzer.statementError(
            setUnmappedLoad("FROM test | WHERE match(first_name, \"foo\") | KEEP first_name"),
            allOf(
                containsString("Found 1 problem"),
                containsString(
                    "line 1:47: unmapped_fields=\"load\" does not support full-text search function [MATCH]; "
                        + "use \"default\" or \"nullify\""
                )
            )
        );
        analyzer.statementError(
            setUnmappedLoad("FROM test | WHERE match_phrase(first_name, \"foo bar\") | KEEP first_name"),
            allOf(
                containsString("Found 1 problem"),
                containsString(
                    "line 1:47: unmapped_fields=\"load\" does not support full-text search function [MatchPhrase]; "
                        + "use \"default\" or \"nullify\""
                )
            )
        );
        if (EsqlCapabilities.Cap.QSTR_FUNCTION.isEnabled()) {
            analyzer.statementError(
                setUnmappedLoad("FROM test | WHERE qstr(\"first_name: foo\") | KEEP first_name"),
                allOf(
                    containsString("Found 1 problem"),
                    containsString(
                        "line 1:47: unmapped_fields=\"load\" does not support full-text search function [QSTR]; "
                            + "use \"default\" or \"nullify\""
                    )
                )
            );
        }
        if (EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled()) {
            analyzer.statementError(
                setUnmappedLoad("FROM test | WHERE kql(\"first_name: foo\") | KEEP first_name"),
                allOf(
                    containsString("Found 1 problem"),
                    containsString(
                        "line 1:47: unmapped_fields=\"load\" does not support full-text search function [KQL]; "
                            + "use \"default\" or \"nullify\""
                    )
                )
            );
        }
        analyzer().addIndex("test", "mapping-full_text_search.json")
            .statementError(
                setUnmappedLoad("FROM test | WHERE knn(vector, [1, 2, 3]) | KEEP vector"),
                allOf(
                    containsString("Found 1 problem"),
                    containsString(
                        "line 1:47: unmapped_fields=\"load\" does not support full-text search function [KNN]; "
                            + "use \"default\" or \"nullify\""
                    )
                )

            );
    }

    private static Matcher<String> partiallyUnmappedNonKeywordError(String fieldName) {
        return containsString("Using partially unmapped non-KEYWORD field [" + fieldName + "]");
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
