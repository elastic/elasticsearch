/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
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
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.nonLoadablePunkWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.fieldCapabilitiesIndexResponse;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.fieldResponseMap;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.mergedResolution;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class AnalyzerUnmappedTests extends AnalyzerUnmappedTestBase {

    /**
     * Query suffixes that use the unsupported type-conflict field [message] in different commands.
     * Each type conflict test iterates over these to verify the error is raised regardless of how the field is used.
     */
    private static final String[] TYPE_CONFLICT_QUERY_SUFFIXES = new String[] {
        "| SORT message",
        "| EVAL x = message",
        "| WHERE message IS NOT NULL" };

    private static final Set<DataType> NO_IMPLICIT_KEYWORD_CONVERTER_PUNK_TYPES = Set.of(
        DataType.AGGREGATE_METRIC_DOUBLE,
        DataType.COUNTER_DOUBLE,
        DataType.COUNTER_INTEGER,
        DataType.COUNTER_LONG,
        DataType.DENSE_VECTOR,
        DataType.EXPONENTIAL_HISTOGRAM,
        DataType.FLATTENED,
        DataType.HISTOGRAM,
        DataType.PARTIAL_AGG,
        DataType.TDIGEST,
        DataType.TEXT
    );

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

    public void testFailEvalAfterDrop() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field
            | EVAL x = does_not_exist_field + 1
            """, "3:12: Unknown column [does_not_exist_field]");
    }

    // A DROP wildcard matching an existing but unsupported-typed field (which reports resolved()==false) must still drop it under
    // nullify/load (so not be mistaken for a non-matching pattern and skipped).
    public void testDropWildcardMatchingUnsupportedField() {
        TestAnalyzer analyzer = analyzer().addIndex("test", "mapping-multi-field-variation.json");
        for (Function<String, String> setUnmapped : List.<Function<String, String>>of(
            AnalyzerUnmappedTestBase::setUnmappedNullify,
            AnalyzerUnmappedTestBase::setUnmappedLoad
        )) {
            assertThat(
                Expressions.names(analyzer.statement(setUnmapped.apply("FROM test | DROP unsupp*")).output()),
                equalTo(Expressions.names(analyzer.statement(setUnmapped.apply("FROM test | DROP unsupported")).output()))
            );
        }
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

    // load now supports subqueries (#142033): outer-only does_not_exist1/2 load in all branches (null-filled where dropped, e.g. the
    // STATS branch) and the statement analyzes successfully (LOOKUP JOIN inside a branch is fine).
    public void testSubquerysMixAndLookupJoinLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        test().addLanguages().addSampleData().addLanguagesLookup().statement(setUnmappedLoad("""
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
            """));
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

    public void testLoadModeAllowsFork() {
        test().statement(setUnmappedLoad("FROM test | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)"));
    }

    public void testLoadModeAllowsForkWithStats() {
        test().statement(setUnmappedLoad("FROM test | FORK (STATS c = COUNT(*)) (STATS d = AVG(salary))"));
    }

    public void testLoadModeAllowsForkWithMultipleBranches() {
        test().statement(setUnmappedLoad("FROM test | FORK (WHERE emp_no > 1) (WHERE emp_no < 100) (WHERE salary > 50000)"));
    }

    public void testLoadModeAllowsForkAfterLinearPipeline() {
        test().statement(setUnmappedLoad("FROM test | WHERE emp_no > 1 | FORK (WHERE salary > 50000) (WHERE salary < 30000)"));
    }

    public void testLoadModeAllowsForkWithUnmappedFieldInBranch() {
        test().statement(setUnmappedLoad("FROM test | FORK (KEEP emp_no, does_not_exist) (WHERE salary > 50000)"));
    }

    // A DROP of an unmapped field materializes it in the sibling branch (#152843); on a multi-FORK plan that new alignment runs before
    // FORK verification, so this guards that it degrades to the clean single-FORK rejection rather than throwing from resolveFork.
    public void testLoadModeRejectsMultipleForksWithDroppedUnmappedField() {
        partialMappingTest().statementError(setUnmappedLoad("""
            FROM partial_mapping_sample_data
            | FORK (DROP unmapped_message) (WHERE true)
            | FORK (WHERE true) (WHERE true)
            """), containsString("Only a single FORK command is supported, but found multiple"));
    }

    // Same guard as above for a FORK nested inside a FORK branch: the outer branch drops the unmapped field ahead of the inner FORK,
    // so both the outer sibling materialization and the nesting are seen before the single-FORK rejection fires.
    public void testLoadModeRejectsNestedForkWithDroppedUnmappedField() {
        partialMappingTest().statementError(setUnmappedLoad("""
            FROM partial_mapping_sample_data
            | FORK (DROP unmapped_message | FORK (WHERE true) (WHERE true)) (WHERE true)
            """), containsString("Only a single FORK command is supported, but found multiple"));
    }

    public void testLoadModeRejectsSubqueryUnionForkWithDroppedUnmappedField() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        partialMappingTest().statementError(setUnmappedLoad("""
            FROM (FROM partial_mapping_sample_data),(FROM partial_mapping_sample_data)
            | FORK (DROP unmapped_message) (WHERE true)
            """), containsString("FORK after subquery is not supported"));
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

    // Regression for #142026.
    public void testNullifyUnmappedFieldOutsideLookupJoinDoesNotPanic() {
        test().addLanguagesLookup()
            .statement(
                setUnmappedNullify(
                    "FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code | EVAL x = does_not_exist"
                )
            );
    }

    // Regression for #142026.
    public void testTwoLookupJoinsWhereFirstKeyUnknownDoesNotPanic() {
        test().addLanguagesLookup()
            .statementError(
                "FROM test | LOOKUP JOIN languages_lookup ON unknown_field | EVAL language_code = languages"
                    + " | LOOKUP JOIN languages_lookup ON language_code",
                containsString("Unknown column [unknown_field] in left side of join")
            );
    }

    // #142033 / PR #151750: an IN-subquery lowers to a semi-join; unmapped_fields="load" now materializes the field used as the
    // IN's left key on the join's left input, so it resolves like any other loaded field across the shapes below.
    public void testLoadModeLoadsUnmappedFieldAsInSubqueryLeftKey() {
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM partial_mapping_sample_data
            | WHERE unmapped_message IN (FROM partial_mapping_sample_data | WHERE message == "42" | KEEP unmapped_message)
            | KEEP message, unmapped_message
            """);
    }

    public void testLoadModeLoadsNonexistentFieldAsInSubqueryLeftKey() {
        expectInSubqueryLeftKeyResolved("nonexistent_field", """
            FROM partial_mapping_sample_data
            | WHERE nonexistent_field IN (FROM partial_mapping_sample_data | WHERE message == "42" | KEEP nonexistent_field)
            | KEEP message, nonexistent_field
            """);
    }

    public void testLoadModeLoadsUnmappedFieldAsNestedInSubqueryLeftKey() {
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM partial_mapping_sample_data
            | WHERE unmapped_message IN
                (FROM partial_mapping_sample_data
                 | WHERE unmapped_message IN (FROM partial_mapping_sample_data | WHERE message == "42" | KEEP unmapped_message)
                 | KEEP unmapped_message)
            | KEEP message, unmapped_message
            """);
    }

    public void testLoadModeLoadsUnmappedInSubqueryLeftKeyInsideSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM (FROM partial_mapping_sample_data
                  | WHERE unmapped_message IN (FROM partial_mapping_sample_data | WHERE message == "42" | KEEP unmapped_message)
                  | KEEP message, unmapped_message),
                 (FROM partial_mapping_sample_data | WHERE message == "Connected to 10.1.0.3!" | KEEP message, unmapped_message)
            """);
    }

    public void testLoadModeLoadsUnmappedInSubqueryLeftKeyWithSubqueryInFromOnRhs() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM partial_mapping_sample_data
            | WHERE unmapped_message IN
                (FROM (FROM partial_mapping_sample_data | WHERE message == "42" | KEEP unmapped_message),
                      (FROM partial_mapping_sample_data | WHERE message == "Connected to 10.1.0.3!" | KEEP unmapped_message)
                 | KEEP unmapped_message)
            | KEEP message, unmapped_message
            """);
    }

    public void testLoadModeLoadsUnmappedInSubqueryLeftKeyAfterFork() {
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM partial_mapping_sample_data
            | FORK (WHERE message == "42")
                   (WHERE message == "Connected to 10.1.0.3!")
            | WHERE unmapped_message IN (FROM partial_mapping_sample_data
                                         | WHERE message == "42" OR message == "Connected to 10.1.0.3!"
                                         | KEEP unmapped_message)
            | KEEP message, unmapped_message
            """);
    }

    public void testLoadModeLoadsUnmappedInSubqueryLeftKeyInsideFork() {
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM partial_mapping_sample_data
            | FORK (WHERE unmapped_message IN (FROM partial_mapping_sample_data
                                               | WHERE message == "42"
                                               | KEEP unmapped_message)
                    | KEEP message, unmapped_message)
                   (WHERE message == "Connected to 10.1.0.3!" | KEEP message, unmapped_message)
            | KEEP message, unmapped_message
            """);
    }

    // FORK inside the IN subquery is rejected later (post-optimization); this asserts the left-key load itself resolves at analysis.
    public void testLoadModeLoadsUnmappedInSubqueryLeftKeyWithForkOnRhs() {
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM partial_mapping_sample_data
            | WHERE unmapped_message IN
                (FROM partial_mapping_sample_data
                 | FORK (WHERE message == "42")
                        (WHERE message == "Connected to 10.1.0.3!")
                 | KEEP unmapped_message)
            | KEEP message, unmapped_message
            """);
    }

    // #142033 / PR #151750 (ivancea): an outer subquery-in-FROM union combined with an IN-subquery whose RHS is itself a union.
    // The outer reference (unmapped_message via EVAL) must broadcast-load into the LHS union even when the RHS union's branches
    // transiently surface the same name (3a keeps then drops it; 3b never mentions it) — both must resolve.
    public void testLoadModeBroadcastsOuterRefAcrossSiblingUnionsWhenRhsSurfacesName() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM (FROM partial_mapping_sample_data | WHERE message == "42"),
                 (FROM partial_mapping_sample_data | WHERE message == "Connected to 10.1.0.1!")
            | WHERE message IN
                (FROM (FROM partial_mapping_sample_data | KEEP message, unmapped_message),
                      (FROM partial_mapping_sample_data | KEEP message, unmapped_message)
                 | KEEP message)
            | EVAL y = unmapped_message
            | KEEP message, y, unmapped_message
            """);
    }

    public void testLoadModeBroadcastsOuterRefAcrossSiblingUnionsWhenRhsHidesName() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        expectInSubqueryLeftKeyResolved("unmapped_message", """
            FROM (FROM partial_mapping_sample_data | WHERE message == "42"),
                 (FROM partial_mapping_sample_data | WHERE message == "Connected to 10.1.0.1!")
            | WHERE message IN
                (FROM (FROM partial_mapping_sample_data | KEEP message),
                      (FROM partial_mapping_sample_data | KEEP message)
                 | KEEP message)
            | EVAL y = unmapped_message
            | KEEP message, y, unmapped_message
            """);
    }

    /**
     * Asserts {@code column} fully resolves under load: the plan resolves and {@code column} surfaces in the output. Where
     * {@code column} is an IN-subquery left key, it must resolve (not stay an {@code UnresolvedAttribute} masked by the RHS's
     * same-named column) — a loaded {@link FieldAttribute}, or a {@code ReferenceAttribute} to it once above a FORK/union. #142033.
     */
    private void expectInSubqueryLeftKeyResolved(String column, String query) {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        LogicalPlan plan = partialMappingTest().statement(setUnmappedLoad(query));
        assertThat("plan should be fully resolved once the IN left key loads from _source", plan.resolved(), is(true));
        assertThat("column [" + column + "] should be present in the resolved output", Expressions.names(plan.output()), hasItem(column));
        plan.forEachDown(AbstractSubqueryJoin.class, join -> {
            for (Attribute leftKey : join.config().leftFields()) {
                if (leftKey.name().equals(column)) {
                    assertThat("IN left key [" + column + "] should be resolved", leftKey.resolved(), is(true));
                }
            }
        });
    }

    // Regression: multi-key LOOKUP JOIN where one key resolves and another doesn't in iteration 1.
    // Iteration 2 entered resolveUsingColumns with [resolved, unresolved] and crashed on the cast.
    public void testMultiKeyLookupJoinWithMixedResolution_doesNotPanic() {
        test().addLanguagesLookup()
            .statement(
                setUnmappedNullify(
                    "FROM test | EVAL language_code = languages "
                        + "| LOOKUP JOIN languages_lookup ON language_code, language_name "
                        + "| EVAL x = does_not_exist"
                )
            );
    }

    public void testLoadLookupJoinAfterFilter_Works() {
        test().addLanguagesLookup().statement(setUnmappedLoad("""
            FROM test
            | WHERE emp_no > 1
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | KEEP emp_no, language_name
            """));
    }

    public void testLoadForkWithLookupJoin_Works() {
        test().addLanguagesLookup().statement(setUnmappedLoad("""
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)
            """));
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

    // unmapped_fields="load" now supports subqueries (#142033): a main index plus a subquery analyzes successfully.
    public void testLoadModeAllowsMainIndexPlusSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguages().statement(setUnmappedLoad("FROM test, (FROM languages | WHERE language_code > 1)"));
    }

    // unmapped_fields="load" now supports subqueries (#142033): two subqueries without a main index analyze successfully.
    public void testLoadModeAllowsTwoSubqueriesWithoutMainIndex() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(setUnmappedLoad("FROM (FROM test),(FROM test)"));
    }

    // unmapped_fields="load" now supports subqueries (#142033): three subqueries analyze successfully.
    public void testLoadModeAllowsThreeSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(setUnmappedLoad("FROM (FROM test),(FROM test),(FROM test)"));
    }

    // Nested subqueries are rejected by checkNestedUnionAlls, which runs at post-optimization (not during analysis), so the
    // analyzer no longer fails this statement once the subquery+load restriction is lifted (#142033).
    public void testLoadModeAllowsNestedSubqueriesAtAnalysis() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguages()
            .addSampleData()
            .statement(setUnmappedLoad("FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)"));
    }

    // unmapped_fields="load" now supports subqueries (#142033): a subquery containing a LOOKUP JOIN analyzes successfully.
    public void testLoadModeAllowsSubqueryWithLookupJoin() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguagesLookup().statement(setUnmappedLoad("""
            FROM test,
                (FROM test
                | EVAL language_code = languages
                | LOOKUP JOIN languages_lookup ON language_code)
            """));
    }

    public void testLoadModeAllowsSingleSubqueryPlusFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        // A single subquery without a main index is merged into the main query during analysis, so there is no
        // Subquery node and the only previous blocker (FORK + load) is now allowed.
        test().statement(setUnmappedLoad("FROM (FROM test) | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)"));
    }

    // The subquery+load restriction is lifted (#142033), but FORK after a subquery is still rejected (checkFork, post-analysis).
    public void testLoadModeDisallowsMultipleSubqueriesPlusFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statementError(
            setUnmappedLoad("FROM (FROM test),(FROM test) | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)"),
            allOf(
                containsString("Found 2 problems"),
                // error below appears twice
                containsString("line 1:34: FORK after subquery is not supported")
            )
        );
    }

    // The subquery+load restriction is lifted (#142033), but FORK after a subquery is still rejected (checkFork, post-analysis).
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
                    containsString("Found 2 problems"),
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

    // unmapped_fields="load" now supports branching views/subqueries (#142033): the branching-view equivalent analyzes successfully.
    public void testLoadModeAllowsBranchingViewEquivalent() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(setUnmappedLoad("FROM (FROM test | WHERE emp_no > 1),(FROM test | WHERE emp_no < 100)"));
    }

    // does_not_exist is referenced only in the outer KEEP and is unmapped in every branch, so it is loaded from _source in all branches
    // (#142033); the branching-view equivalent analyzes successfully.
    public void testLoadModeAllowsBranchingViewEquivalentWithUnmappedField() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().statement(
            setUnmappedLoad("FROM (FROM test | WHERE emp_no > 1),(FROM test | WHERE emp_no < 100) | KEEP emp_no, does_not_exist")
        );
    }

    public void testLoadModeDisallowsCrossBranchTypeConflict() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguages().statementError(setUnmappedLoad("""
            FROM languages,
                (FROM test | KEEP emp_no, language_code)
            | EVAL x = language_code + 1
            | KEEP language_code, x
            """), containsString("Column [language_code] has conflicting data types in subqueries: [integer, keyword]"));
    }

    public void testLoadModeAllowsCrossBranchTypeConflictWhenOnlyKept() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguages().statement(setUnmappedLoad("""
            FROM languages,
                (FROM test | KEEP emp_no, language_code)
            | KEEP language_code
            """));
    }

    // Regression: RENAME of a cross-branch union-typed column above a multi-source FROM used to crash with a 500
    // (UnresolvedException "Invalid call to dataType on an unresolved object") in ResolveUnionTypesInUnionAll, because the
    // not-yet-resolved RENAME alias yielded an UnresolvedAttribute whose dataType() was queried during the type cascade. It must
    // instead surface the same clean cross-branch conflict verification error as other references to the unsupported union column.
    public void testLoadModeCrossBranchTypeConflictRenameFailsCleanly() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguages().statementError(setUnmappedLoad("""
            FROM languages,
                (FROM test | KEEP emp_no, language_code)
            | RENAME language_code AS lc
            """), containsString("Column [language_code] has conflicting data types in subqueries: [integer, keyword]"));
    }

    // Same regression as above but the renamed conflicting column is also consumed by a downstream SORT: the first cascade pass sees a
    // resolved RENAME alias and retargets the OrderBy, then a later pass leaves the alias unresolved (unsupported child). It must still
    // fail cleanly with the cross-branch conflict error rather than crash in ResolveUnionTypesInUnionAll.
    public void testLoadModeCrossBranchTypeConflictRenameThenSortFailsCleanly() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        test().addLanguages().statementError(setUnmappedLoad("""
            FROM languages,
                (FROM test | KEEP emp_no, language_code)
            | RENAME language_code AS lc
            | SORT lc
            """), containsString("Column [language_code] has conflicting data types in subqueries: [integer, keyword]"));
    }

    // Regression: to_string() (a convert function) over a field that is BOTH partially unmapped within a branch (multi-typed
    // {float, keyword} in the merged languages_mixed_numerics+partial_message_types_lookup relation) AND union-typed across the
    // UnionAll branches under load used to fail analysis with "Unknown column [$$language_code_float$converted_to$keyword]".
    public void testLoadModeToStringOverCrossBranchMultiTypeUnionField() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("languages_mixed_numerics", fieldResponseMap("language_code_float", "float")),
                fieldCapabilitiesIndexResponse("partial_message_types_lookup", fieldResponseMap("message_type", "keyword"))
            ),
            List.of()
        );
        TestAnalyzer a = analyzer();
        a.addIndex(
            "languages_mixed_numerics,partial_message_types_lookup",
            mergedResolution("languages_mixed_numerics,partial_message_types_lookup", caps, true)
        );
        a.addIndex("clientips", "mapping-clientips.json");
        a.statement(setUnmappedLoad("""
            FROM languages_mixed_numerics, partial_message_types_lookup, (FROM clientips)
            | EVAL x = to_string(language_code_float)
            """));
        // The raw language_code_float still flows to the default output as a non-loadable float PUNK (null where unmapped).
        assertWarnings(nonLoadablePunkWarning("language_code_float", "float"));
    }

    public void testLoadModeToStringOverMultiTypeUnionFieldInSubqueryBranch() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("languages_mixed_numerics", fieldResponseMap("language_code_float", "float")),
                fieldCapabilitiesIndexResponse("partial_message_types_lookup", fieldResponseMap("message_type", "keyword"))
            ),
            List.of()
        );
        TestAnalyzer a = analyzer();
        a.addIndex("clientips", "mapping-clientips.json");
        a.addIndex(
            "languages_mixed_numerics,partial_message_types_lookup",
            mergedResolution("languages_mixed_numerics,partial_message_types_lookup", caps, true)
        );
        a.statement(setUnmappedLoad("""
            FROM clientips, (FROM languages_mixed_numerics, partial_message_types_lookup)
            | EVAL x = to_string(language_code_float)
            """));
        // The raw language_code_float still flows to the default output as a non-loadable float PUNK (null where unmapped).
        assertWarnings(nonLoadablePunkWarning("language_code_float", "float"));
    }

    public void testLoadModeCrossBranchTextPunkResolvesToTextNotUnsupported() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        // foo is TEXT (no KEYWORD->TEXT converter) in text_idx and unmapped in unmapped_idx -> two-legged PUNK in the merged branch.
        FieldCapabilitiesResponse mergedCaps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("text_idx", fieldResponseMap(Map.of("foo", "text", "id", "long"))),
                fieldCapabilitiesIndexResponse("unmapped_idx", fieldResponseMap(Map.of("id", "long")))
            ),
            List.of()
        );
        FieldCapabilitiesResponse subCaps = new FieldCapabilitiesResponse(
            List.of(fieldCapabilitiesIndexResponse("kw_idx", fieldResponseMap(Map.of("id", "long")))),
            List.of()
        );
        TestAnalyzer a = analyzer();
        a.addIndex("text_idx,unmapped_idx", mergedResolution("text_idx,unmapped_idx", mergedCaps, true));
        a.addIndex("kw_idx", mergedResolution("kw_idx", subCaps, true));

        var plan = a.statement(setUnmappedLoad("FROM text_idx, unmapped_idx, (FROM kw_idx) | SORT id | LIMIT 3"));

        Attribute foo = plan.output().stream().filter(at -> at.name().equals("foo")).findFirst().orElseThrow();
        assertThat(foo.dataType(), equalTo(DataType.TEXT));
        // No column, at the top output or in any UnionAll, may surface as UNSUPPORTED.
        plan.output()
            .forEach(at -> assertThat(at.name() + " should not be UNSUPPORTED", at.dataType(), not(equalTo(DataType.UNSUPPORTED))));
        plan.forEachDown(
            org.elasticsearch.xpack.esql.plan.logical.UnionAll.class,
            ua -> ua.output()
                .forEach(at -> assertThat(at.name() + " should not be UNSUPPORTED", at.dataType(), not(equalTo(DataType.UNSUPPORTED))))
        );
        assertWarnings(nonLoadablePunkWarning("foo", "text"));
    }

    public void testLoadModeCrossBranchSmallNumericPunkResolvesToWidenedNotUnsupported() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        FieldCapabilitiesResponse mergedCaps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("float_idx", fieldResponseMap(Map.of("foo", "float", "id", "long"))),
                fieldCapabilitiesIndexResponse("unmapped_idx", fieldResponseMap(Map.of("id", "long")))
            ),
            List.of()
        );
        FieldCapabilitiesResponse subCaps = new FieldCapabilitiesResponse(
            List.of(fieldCapabilitiesIndexResponse("kw_idx", fieldResponseMap(Map.of("id", "long")))),
            List.of()
        );
        TestAnalyzer a = analyzer();
        a.addIndex("float_idx,unmapped_idx", mergedResolution("float_idx,unmapped_idx", mergedCaps, true));
        a.addIndex("kw_idx", mergedResolution("kw_idx", subCaps, true));

        var plan = a.statement(setUnmappedLoad("FROM float_idx, unmapped_idx, (FROM kw_idx) | SORT id | LIMIT 3"));

        Attribute foo = plan.output().stream().filter(at -> at.name().equals("foo")).findFirst().orElseThrow();
        assertThat(foo.dataType(), equalTo(DataType.DOUBLE));
        plan.output()
            .forEach(at -> assertThat(at.name() + " should not be UNSUPPORTED", at.dataType(), not(equalTo(DataType.UNSUPPORTED))));
        assertWarnings(nonLoadablePunkWarning("foo", "float"));
    }

    public void testSingleTypeLongUnmappedAutoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("bar", Map.of())
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("foo,bar", caps, true));
        // This test targets the new LOAD auto-cast behavior only; mixed-cluster behavior is covered in #151863.
        // Use a version that supports the compact path for now, then switch to the dedicated gate introduced there.
        TestAnalyzer ta = analyzer().minimumTransportVersion(CompactMultiTypeEsField.CompactMultiTypeEsField);
        for (var entry : resolutions.entrySet()) {
            ta.addIndex(entry.getKey().indexPattern(), entry.getValue());
        }
        for (String suffix : TYPE_CONFLICT_QUERY_SUFFIXES) {
            var plan = ta.statement(setUnmappedLoad("FROM foo, bar " + suffix));
            assertTwoLeggedPunkResolution(plan, "message", DataType.LONG);
        }
    }

    public void testTypeConflictLongKeywordUnmappedNoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("test1", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("test2", fieldResponseMap("message", "keyword")),
                fieldCapabilitiesIndexResponse("test3", Map.of())
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("test1,test2,test3", caps, true));
        for (String suffix : TYPE_CONFLICT_QUERY_SUFFIXES) {
            typeConflictVerificationFailure(setUnmappedLoad("FROM test1, test2, test3 " + suffix), resolutions);
        }
    }

    public void testTypeConflictLongIntUnmappedNoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

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

    public void testSameMappingHashNotPartiallyUnmapped() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

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

    public void testSameMappingHashWithUnmappedIndexAutoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(
            List.of(
                fieldCapabilitiesIndexResponse("foo", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("bar", fieldResponseMap("message", "long")),
                fieldCapabilitiesIndexResponse("baz", Map.of())
            ),
            List.of()
        );
        var resolutions = indexResolutions(mergedResolution("foo,bar,baz", caps, true));
        // See testSingleTypeLongUnmappedAutoCast for why this currently pins the compact-path transport version.
        TestAnalyzer ta = analyzer().minimumTransportVersion(CompactMultiTypeEsField.CompactMultiTypeEsField);
        for (var entry : resolutions.entrySet()) {
            ta.addIndex(entry.getKey().indexPattern(), entry.getValue());
        }
        var plan = ta.statement(setUnmappedLoad("FROM foo, bar, baz | SORT message"));
        assertTwoLeggedPunkResolution(plan, "message", DataType.LONG);
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
            test().statementError(statement, containsString("[tbucket(1 hour)] "));
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

        Set<DataType> noConverterTypes = new HashSet<>();
        for (DataType dataType : DataType.values()) {
            if (excludedTypes.contains(dataType)) {
                continue;
            }
            // Build a minimal mapping: one keyword field (emp_no stand-in for SORT) and one field of the type under test,
            // wrapped as a single-type PUNK (as IndexResolver would do in production).
            Map<String, EsField> mapping = Map.of(
                "sort_field",
                new EsField("sort_field", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
                "test_field",
                new PotentiallyUnmappedSingleTypeEsField(
                    new EsField("test_field", dataType.widenSmallNumeric(), Map.of(), true, EsField.TimeSeriesFieldType.NONE),
                    Set.of("test1")
                )
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
            if (supportsKeywordConversionUnderLoad(dataType.widenSmallNumeric())) {
                assertThat(
                    "Partially-mapped " + dataType + " field with KEYWORD converter should be re-written as UnionTypeEsField",
                    fieldAttr.field(),
                    instanceOf(UnionTypeEsField.class)
                );
            } else {
                noConverterTypes.add(dataType);
                assertThat(
                    "Partially-mapped " + dataType + " field with no KEYWORD converter should remain a regular EsField",
                    fieldAttr.field().getClass(),
                    is(EsField.class)
                );
            }
        }
        assertThat(noConverterTypes, equalTo(NO_IMPLICIT_KEYWORD_CONVERTER_PUNK_TYPES));
        // Every surviving single-type PUNK falls back to null where unmapped, so all of them warn.
        assertWarnings(
            noConverterTypes.stream()
                .map(dt -> nonLoadablePunkWarning("test_field", dt.widenSmallNumeric().typeName()))
                .toArray(String[]::new)
        );
    }

    private static boolean supportsKeywordConversionUnderLoad(DataType mappedType) {
        if (mappedType == DataType.DENSE_VECTOR) {
            // #152184: implicit KEYWORD->DENSE_VECTOR is unsafe because source-backed unmapped vectors load as numeric arrays.
            return false;
        }
        var converterFactory = EsqlDataTypeConverter.converterFunctionFactory(mappedType);
        if (converterFactory == null) {
            return false;
        }
        var keywordField = new FieldAttribute(
            Source.EMPTY,
            "dummy",
            new EsField("dummy", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        AbstractConvertFunction converter = converterFactory.apply(Source.EMPTY, keywordField, EsqlTestUtils.TEST_CFG);
        return converter.supportedTypes().contains(DataType.KEYWORD);
    }

    /**
     * Regression test for #151525: {@link IndexResolver#wrapPartiallyUnmappedField} must preserve
     * the original type name for small numeric fields (short, byte, float, half_float, scaled_float).
     * The physical layer looks up conversion expressions by the shard-reported type (e.g. "short"),
     * so the type stored in the {@link PotentiallyUnmappedSingleTypeEsField} must match, not the widened type.
     */
    public void testWrapPartiallyUnmappedFieldPreservesSmallNumericTypes() {
        Set<String> mappedIndices = Set.of("idx_mapped");
        for (DataType smallNumeric : List.of(DataType.SHORT, DataType.BYTE, DataType.FLOAT, DataType.HALF_FLOAT, DataType.SCALED_FLOAT)) {
            EsField field = new EsField("f", smallNumeric, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
            var wrapped = (PotentiallyUnmappedSingleTypeEsField) IndexResolver.wrapPartiallyUnmappedField(field, "f", "f", mappedIndices);
            assertThat(
                "Partially-unmapped " + smallNumeric + " field should be stored under its original (non-widened) type name",
                wrapped.getTypesToIndices(),
                equalTo(Map.of(smallNumeric.typeName(), mappedIndices))
            );
            assertThat("The original mapped field should be preserved verbatim for null-fallback", wrapped.mappedField(), equalTo(field));
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
                        )
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
                containsString("Found 6 problems"),
                // field.aaa (before the FORK) and field.bbb (in one branch, loaded into all under load) each fail flattened-subfield
                // loading once per branch - 3 times each.
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
                containsString("Found 1 problem"),
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
            allOf(containsString("Found 1 problem"), containsString("line 1:55: PROMQL is not supported with unmapped_fields=\"load\""))
        );

        assertUnmappedLoadError(
            analyzer,
            "PROMQL index=test step=5m rate(network.bytes_in[5m])",
            allOf(containsString("Found 1 problem"), containsString("line 1:55: PROMQL is not supported with unmapped_fields=\"load\""))
        );

        assertUnmappedLoadError(
            analyzer,
            "PROMQL index=test step=5m avg(network.bytes_in) + avg(network.bytes_out)",
            allOf(containsString("Found 1 problem"), containsString("line 1:55: PROMQL is not supported with unmapped_fields=\"load\""))
        );

        assertUnmappedLoadError(
            analyzer,
            "PROMQL index=test start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T01:00:00Z\" buckets=10 avg(network.bytes_in)",
            allOf(containsString("Found 1 problem"), containsString("line 1:114: PROMQL is not supported with unmapped_fields=\"load\""))
        );
    }

    // nullify is allowed with PromQL (unlike load), but a field after the collapsing aggregate still fails.
    public void testUnmappedFieldNullifyWithPromQl() {
        TestAnalyzer analyzer = test().addIndex("test", "tsdb-mapping.json");

        assertTrue(analyzer.statement(setUnmappedNullify("PROMQL index=test step=5m sum(network.bytes_in)")).resolved());

        analyzer.statementError(
            setUnmappedNullify("PROMQL index=test step=5m sum(network.bytes_in) | EVAL x = does_not_exist"),
            containsString("Unknown column [does_not_exist]")
        );
    }

    /**
     * When unmapped_fields=load and an index has a partially mapped field that is not KEYWORD (e.g. LONG),
     * analysis must autocast to the mapped type if conversion was possible.
     */
    public void testLoadWithPartiallyMappedNonKeywordAutoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx* | WHERE partial_long > 0"));

        assertThat(plan, not(nullValue()));
        assertTwoLeggedPunkResolution(plan, "partial_long", DataType.LONG);
    }

    public void testLoadWithPartiallyMappedNonKeywordReportsAllFieldsAutoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(
            Map.of("partial_long", longField("partial_long"), "partial_double", doubleField("partial_double")),
            Set.of("partial_long", "partial_double")
        );
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx* | SORT partial_long, partial_double"));
        assertThat(plan, not(nullValue()));

        // assert PUNKs are resolved
        assertTwoLeggedPunkResolution(plan, "partial_long", DataType.LONG);
        assertTwoLeggedPunkResolution(plan, "partial_double", DataType.DOUBLE);
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
        var partialLong = new PotentiallyUnmappedSingleTypeEsField(longField("partial_long"), Set.of("idx_a", "idx_b"));
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
                containsString("Found 1 problem"),
                containsString("line 1:72: Cannot use field [conflicted] due to ambiguities being mapped as [2] incompatible types:")
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
        var partialLong = new PotentiallyUnmappedSingleTypeEsField(longField("partial_long"), Set.of("idx_a"));
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
        var partialLong = new PotentiallyUnmappedSingleTypeEsField(longField("partial_long"), Set.of("idx_a"));
        var merged = new EsIndex(
            pattern,
            Map.of("partial_long", partialLong, "common", keywordField("common")),
            Map.of("idx_a", IndexMode.STANDARD, "idx_b", IndexMode.STANDARD),
            Map.of(),
            Map.of()
        );

        var plan = analyzer().addIndex(merged).statement(setUnmappedLoad("FROM idx_a, idx_b | WHERE partial_long > 0"));
        assertThat(plan, not(nullValue()));
        assertTwoLeggedPunkResolution(plan, "partial_long", DataType.LONG);
    }

    public void testAllowLoadFromOnlyWhenPartialNonKeywordUnused() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx*"));
        assertThat(plan, not(nullValue()));
    }

    public void testLoadWithPartiallyMappedNonKeywordInRenameAutoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(
            Map.of("partial_long", longField("partial_long"), "common", keywordField("common")),
            Set.of("partial_long")
        );
        var analyzer = analyzer().addIndex(esIndex);

        var plan = analyzer.statement(setUnmappedLoad("FROM idx* | RENAME partial_long AS pl"));
        assertThat(plan, not(nullValue()));
        assertTwoLeggedPunkResolution(plan, "partial_long", DataType.LONG);

        plan = analyzer.statement(setUnmappedLoad("FROM idx* | RENAME common as c, partial_long AS pl"));
        assertThat(plan, not(nullValue()));
        assertTwoLeggedPunkResolution(plan, "partial_long", DataType.LONG);
    }

    public void testNonLoadablePunkWarnsWhenInOutputNotWhenExcluded() {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK.isEnabled()
        );

        var kept = new EsField("kept_amd", DataType.AGGREGATE_METRIC_DOUBLE, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
        var excluded = new EsField("excluded_amd", DataType.AGGREGATE_METRIC_DOUBLE, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
        var esIndex = partialIndex(
            Map.of("kept_amd", kept, "excluded_amd", excluded, "common", keywordField("common")),
            Set.of("kept_amd", "excluded_amd")
        );
        var analyzer = analyzer().addIndex(esIndex);

        var plan = analyzer.statement(setUnmappedLoad("FROM idx* | KEEP kept_amd, common"));
        var keptAttr = EsqlTestUtils.singleValue(plan.output().stream().filter(a -> a.name().equals("kept_amd")).toList());
        assertThat(keptAttr.dataType(), equalTo(DataType.AGGREGATE_METRIC_DOUBLE));
        assertWarnings(nonLoadablePunkWarning("kept_amd", "aggregate_metric_double"));
    }

    public void testNonLoadablePunkWarnsUnderBareFrom() {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK.isEnabled()
        );

        var plan = analyzer().addIndex(partialAmdAndCommonIndex()).statement(setUnmappedLoad("FROM idx*"));
        var attr = EsqlTestUtils.singleValue(plan.output().stream().filter(a -> a.name().equals("partial_amd")).toList());
        assertThat(attr.dataType(), equalTo(DataType.AGGREGATE_METRIC_DOUBLE));
        assertWarnings(nonLoadablePunkWarning("partial_amd", "aggregate_metric_double"));
    }

    public void testNonLoadablePunkWarnsUnderKeepWildcard() {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK.isEnabled()
        );

        var plan = analyzer().addIndex(partialAmdAndCommonIndex()).statement(setUnmappedLoad("FROM idx* | KEEP *"));
        var attr = EsqlTestUtils.singleValue(plan.output().stream().filter(a -> a.name().equals("partial_amd")).toList());
        assertThat(attr.dataType(), equalTo(DataType.AGGREGATE_METRIC_DOUBLE));
        assertWarnings(nonLoadablePunkWarning("partial_amd", "aggregate_metric_double"));
    }

    public void testNonLoadablePunkNoWarnWhenExcludedByKeepWildcard() {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK.isEnabled()
        );

        // partial_amd is excluded by the wildcard, so it never reaches the output and must not warn.
        var plan = analyzer().addIndex(partialAmdAndCommonIndex()).statement(setUnmappedLoad("FROM idx* | KEEP comm*"));
        var attr = EsqlTestUtils.singleValue(plan.output());
        assertThat(attr.name(), equalTo("common"));
    }

    public void testNonLoadablePunkNoWarnWhenDropped() {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK.isEnabled()
        );

        // A dropped PUNK, whether named explicitly or matched by a wildcard, leaves the output and must not warn.
        for (String query : List.of("FROM idx* | DROP partial_amd", "FROM idx* | DROP partial*")) {
            var plan = analyzer().addIndex(partialAmdAndCommonIndex()).statement(setUnmappedLoad(query));
            var attr = EsqlTestUtils.singleValue(plan.output());
            assertThat("query [" + query + "]", attr.name(), equalTo("common"));
        }
    }

    public void testNonLoadablePunkDirectCastLeavesKeptRawFieldAsFallback() {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK.isEnabled()
        );

        var field = "partial_amd";
        var esIndex = partialIndex(Map.of(field, aggregateMetricDoubleField(field)), Set.of(field));
        var plan = analyzer().addIndex(esIndex)
            .statement(setUnmappedLoad("FROM idx* | EVAL x = " + field + "::keyword | KEEP x, " + field));
        // The cast loads into a separate KEYWORD attribute (x); the kept raw field keeps its own identity, stays an
        // AGGREGATE_METRIC_DOUBLE null fallback, and therefore still warns.
        var x = EsqlTestUtils.singleValue(plan.output().stream().filter(a -> a.name().equals("x")).toList());
        assertThat(x.dataType(), equalTo(DataType.KEYWORD));
        var raw = EsqlTestUtils.singleValue(plan.output().stream().filter(a -> a.name().equals(field)).toList());
        assertThat(raw.dataType(), equalTo(DataType.AGGREGATE_METRIC_DOUBLE));
        assertTrue(
            "Expected x to load unmapped rows from _source via a KEYWORD union",
            unionFields(plan).stream().anyMatch(u -> u.getDataType() == DataType.KEYWORD && u.getUnmappedConversionExpression() != null)
        );
        assertWarnings(nonLoadablePunkWarning(field, "aggregate_metric_double"));
    }

    public void testNonLoadablePunkEvalSameNameOverrideDoesNotWarn() {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK.isEnabled()
        );

        var plan = analyzer().addIndex(partialAmdAndCommonIndex())
            .statement(setUnmappedLoad("FROM idx* | EVAL partial_amd = partial_amd :: keyword"));
        var attr = EsqlTestUtils.singleValue(plan.output().stream().filter(a -> a.name().equals("partial_amd")).toList());
        assertThat(attr.dataType(), equalTo(DataType.KEYWORD));
    }

    public void testNonLoadablePunkNoWarningWhenVerifierFails() {
        assumeTrue(
            "Requires OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK.isEnabled()
        );

        var analyzer = analyzer().addIndex(partialAmdAndCommonIndex());
        var e = expectThrows(VerificationException.class, () -> analyzer.statement(setUnmappedLoad("FROM idx* | WHERE common | LIMIT 10")));
        assertThat(e.getMessage(), containsString("Condition expression needs to be boolean, found [KEYWORD]"));
        assertWarnings();
    }

    public void testLoadWithPartiallyMappedNonKeywordInSortAutoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx* | SORT partial_long"));
        assertThat(plan, not(nullValue()));
        assertTwoLeggedPunkResolution(plan, "partial_long", DataType.LONG);
    }

    /**
     * Same rule as {@link #testLoadWithPartiallyMappedNonKeywordAutoCast} exercised through additional commands
     * ({@code CHANGE_POINT} and {@code MV_EXPAND}) to ensure the check is not accidentally tied to a specific command.
     * A regression that bypasses the verifier for one of these commands would cause its test to fail.
     */
    public void testLoadWithPartiallyMappedNonKeywordInChangePointAutoCast() {
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

        var plan = analyzer().addIndex(esIndex)
            .statement(setUnmappedLoad("FROM idx* | CHANGE_POINT partial_long ON @timestamp AS type, pvalue"));
        assertThat(plan, not(nullValue()));
        assertTwoLeggedPunkResolution(plan, "partial_long", DataType.LONG);
    }

    /** See {@link #testLoadWithPartiallyMappedNonKeywordInChangePointAutoCast}. */
    public void testAllowLoadWithPartiallyMappedNonKeywordInMvExpand() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var esIndex = partialIndex(Map.of("partial_long", longField("partial_long")), Set.of("partial_long"));
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx* | MV_EXPAND partial_long"));

        assertThat(plan, not(nullValue()));
        assertTwoLeggedPunkResolution(plan, "partial_long", DataType.LONG);
    }

    public void testLoadWithPartiallyMappedNonKeywordDottedPathAutoCast() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        var sub = new PotentiallyUnmappedSingleTypeEsField(longField("sub"), Set.of("idx_mapped"));
        var obj = new EsField("obj", DataType.OBJECT, Map.of("sub", sub), true, EsField.TimeSeriesFieldType.NONE);
        var esIndex = new EsIndex("idx*", Map.of("obj", obj), Map.of("idx_mapped", IndexMode.STANDARD), Map.of(), Map.of());

        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx* | SORT `obj.sub`"));
        assertThat(plan, not(nullValue()));
        assertTwoLeggedPunkResolution(plan, "obj.sub", DataType.LONG);
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
     * With {@code unmapped_fields=load}, a partially unmapped non-KEYWORD field present in the index but not referenced downstream of
     * {@code FROM} imposes no constraints and must analyze cleanly.
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

    /**
     * Assert that the plan contains exactly one {@link UnionTypeEsField} of the passed type. This is a proxy to the idea that
     * the two-legged PUNK was correctly resolved by the analyzer rule.
     */
    private void assertTwoLeggedPunkResolution(LogicalPlan plan, String name, DataType type) {
        Set<UnionTypeEsField> fields = new HashSet<>();

        plan.forEachExpressionDown(FieldAttribute.class, fa -> {
            if (fa.name().equals(name) && fa.field() instanceof UnionTypeEsField field) {
                fields.add(field);
            }
        });

        String msg = String.format(
            Locale.ROOT,
            "Expected exactly one %s field [%s] of type [UnionTypeEsField]. Got %s.",
            name,
            type,
            fields.size()
        );

        assertThat(msg, fields, hasSize(1));

        UnionTypeEsField field = fields.iterator().next();

        assertEquals(field.getDataType(), type);
        assertThat(field.getConversionExpressions().isEmpty(), is(false));
        assertThat(field.getUnmappedConversionExpression(), notNullValue());
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
        assertThat(e.getMessage(), containsString("Cannot use field [message]"));
    }

    private static EsIndex partialIndex(Map<String, EsField> mapping, Set<String> partialFieldNames) {
        Set<String> mappedIndices = Set.of("idx_mapped");
        Map<String, EsField> wrappedMapping = new HashMap<>(mapping);
        for (String fieldName : partialFieldNames) {
            wrappedMapping.compute(
                fieldName,
                (k, field) -> IndexResolver.wrapPartiallyUnmappedField(field, fieldName, fieldName, mappedIndices)
            );
        }
        return new EsIndex("idx*", wrappedMapping, Map.of("idx_mapped", IndexMode.STANDARD), Map.of(), Map.of());
    }

    private static EsIndex partialAmdAndCommonIndex() {
        return partialIndex(
            Map.of("partial_amd", aggregateMetricDoubleField("partial_amd"), "common", keywordField("common")),
            Set.of("partial_amd")
        );
    }

    private static EsField longField(String name) {
        return new EsField(name, DataType.LONG, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
    }

    private static EsField doubleField(String name) {
        return new EsField(name, DataType.DOUBLE, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
    }

    public void testNoConverterPunkDirectCastLoadsUnmapped() {
        var esIndex = partialIndex(Map.of("partial_text", textField("partial_text")), Set.of("partial_text"));
        var plan = analyzer().addIndex(esIndex).statement(setUnmappedLoad("FROM idx* | EVAL x = partial_text::keyword | KEEP x"));
        assertTrue(
            "Expected a KEYWORD union with an unmapped conversion (unmapped rows loaded from _source)",
            unionFields(plan).stream().anyMatch(u -> u.getDataType() == DataType.KEYWORD && u.getUnmappedConversionExpression() != null)
        );
    }

    /**
     * Casting a <em>renamed</em> no-converter PUNK falls back (unmapped rows become {@code null}) instead of loading the unmapped leg:
     * {@code ResolveUnionTypes} only loads it for a cast directly on the field's own {@link FieldAttribute}, like a genuine union type.
     */
    public void testNoConverterPunkRenameThenCastDoesNotLoadUnmapped() {
        var esIndex = partialIndex(Map.of("partial_text", textField("partial_text")), Set.of("partial_text"));
        var plan = analyzer().addIndex(esIndex)
            .statement(setUnmappedLoad("FROM idx* | RENAME partial_text AS pt | EVAL x = pt::keyword | KEEP x"));
        var attr = EsqlTestUtils.singleValue(plan.output());
        assertThat(attr.name(), equalTo("x"));
        assertThat(attr.dataType(), equalTo(DataType.KEYWORD));
        assertThat(unionFields(plan), Matchers.empty());
        // The cast targets the renamed alias, not the field itself, so the unmapped leg is not loaded and partial_text falls back to null.
        assertWarnings(nonLoadablePunkWarning("partial_text", "text"));
    }

    private static final List<DataType> SMALL_NUMERIC_TYPES = List.of(
        DataType.SHORT,
        DataType.BYTE,
        DataType.FLOAT,
        DataType.HALF_FLOAT,
        DataType.SCALED_FLOAT
    );

    /** Regression test for #151525. Verify we key by the unwidened type (e.g., {@code SHORT}), not the wide one (e.g., {@code INTEGER}). */
    public void testTwoLeggedPunkSmallNumericExplicitCastUsesOriginalTypeAsKey() {
        assumeTrue("Requires OPTIONAL_FIELDS_V5", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());

        for (DataType dt : SMALL_NUMERIC_TYPES) {
            DataType widened = dt.widenSmallNumeric();
            String sortField = "sort_field";
            String smallTypeField = "f";
            EsIndex esIndex = partialIndex(
                Map.of(
                    sortField,
                    new EsField(sortField, DataType.INTEGER, emptyMap(), true, EsField.TimeSeriesFieldType.NONE),
                    smallTypeField,
                    new EsField(smallTypeField, dt, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
                ),
                Set.of(smallTypeField)
            );
            String castFn = widened == DataType.INTEGER ? "to_integer" : "to_double";
            LogicalPlan plan = analyzer().minimumTransportVersion(CompactMultiTypeEsField.CompactMultiTypeEsField)
                .addIndex(esIndex)
                .statement(setUnmappedLoad(Strings.format("FROM idx* | EVAL x = %s(%s) | SORT sort_field", castFn, smallTypeField)));

            Holder<CompactMultiTypeEsField> field = new Holder<>();
            plan.forEachExpressionDown(FieldAttribute.class, fa -> {
                if (fa.field().getName().equals(smallTypeField) && fa.field() instanceof CompactMultiTypeEsField c) {
                    field.set(c);
                }
            });
            CompactMultiTypeEsField compact = field.get();
            assertThat("Widened data type for " + dt + " (explicit cast)", compact.getDataType(), is(widened));
            assertThat(
                "typeToConversionExpressions for explicitly-cast partially-unmapped " + dt + " must use the original type as key",
                compact.getTypeToConversionExpressions().keySet(),
                equalTo(Set.of(dt))
            );
            assertThat(
                "Inner field of the convert function for " + dt + " (explicit cast) must keep the original type",
                ((AbstractConvertFunction) compact.getTypeToConversionExpressions().get(dt)).field().dataType(),
                is(dt)
            );
            assertThat(
                "unmappedConversionExpression (explicit cast) inner field must be KEYWORD",
                ((AbstractConvertFunction) compact.getUnmappedConversionExpression()).field().dataType(),
                is(DataType.KEYWORD)
            );
            // The raw (uncast) field reaches the default output and falls back to null where unmapped.
            assertWarnings(nonLoadablePunkWarning(smallTypeField, dt.typeName()));
        }
    }

    public void testUnmappedFieldsDefaultWithQueryStringFullTextFunctionsDoesNotLoadUnmappedFields() {
        var analyzer = test();
        for (var function : List.of(
            Map.entry(EsqlCapabilities.Cap.QSTR_FUNCTION, "qstr(\"first_name: foo\")"),
            Map.entry(EsqlCapabilities.Cap.KQL_FUNCTION, "kql(\"first_name: foo\")")
        )) {
            if (function.getKey().isEnabled()) {
                analyzer.statementError(
                    "FROM test | WHERE " + function.getValue() + " | EVAL x = LENGTH(does_not_exist_field) | KEEP x",
                    containsString("Unknown column [does_not_exist_field]")
                );
            }
        }
    }

    private static EsField textField(String name) {
        return new EsField(name, DataType.TEXT, emptyMap(), false, EsField.TimeSeriesFieldType.NONE);
    }

    private static EsField aggregateMetricDoubleField(String name) {
        return new EsField(name, DataType.AGGREGATE_METRIC_DOUBLE, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
    }

    private static List<UnionTypeEsField> unionFields(LogicalPlan plan) {
        List<UnionTypeEsField> unions = new ArrayList<>();
        plan.forEachExpressionDown(FieldAttribute.class, fa -> {
            if (fa.field() instanceof UnionTypeEsField u) {
                unions.add(u);
            }
        });
        return unions;
    }
}
