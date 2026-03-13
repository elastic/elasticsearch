/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzeStatement;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

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

    public void testFailStatsThenEvalExistingField() {
        var query = """
            FROM test
            | STATS cnt = COUNT(*)
            | EVAL x = emp_no
            """;
        var failure = "line 3:12: Unknown column [emp_no]";
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

    // same tree as above, except for the source nodes

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

    // Known issue: https://github.com/elastic/elasticsearch/issues/141827.
    public void testFailLookupJoinWithoutCast() {
        String query = """
            FROM test
            | EVAL language_code = does_not_exist
            | LOOKUP JOIN languages_lookup ON language_code
            """;
        var failure = "is incompatible with right field [language_code] of type [INTEGER]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailSemanticTextLoad() {
        String query = """
            FROM test
            | WHERE KNN(does_not_exist, [0, 1, 2])
            """;
        var failure = "first argument of [KNN(does_not_exist, [0, 1, 2])] must be [dense_vector, null, text], "
            + "found value [does_not_exist] type [keyword]";
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailRateLoad() {
        String query = """
            TS k8s
            | STATS max(rate(does_not_exist))
            """;
        var failure = "first argument of [rate(does_not_exist)] must be [counter_long, counter_integer or counter_double], "
            + "found value [does_not_exist] type [keyword]";
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
            | STATS max(rate(network.total_cost))  BY tbucket = bucket(newTs, 1hour)
            """), "3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX);

        verificationFailure(setUnmappedNullify("""
            TS k8s
            | DROP @timestamp
            | STATS max(rate(network.total_cost))
            """), "3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX);
    }

    // This test verifies that we do not allow an unmapped @timestamp fields in tbucket.
    public void testFailNoUnmappedTimestamp() throws Exception {
        String query = ("""
            FROM employees
            | STATS c = COUNT(*) BY tbucket(1 hour)
            """);
        var failure = "[tbucket(1 hour)] requires the [@timestamp] field";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
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

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
