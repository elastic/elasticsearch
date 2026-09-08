/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DetermineUnmappedFieldsToKeepOrderingTests extends AnalyzerUnmappedTestBase {

    /** An explicit term still beats a wildcard and keeps its written position, because the real KEEP resolver decides. */
    public void testKeepOrderingHonouredForDiscoveredFields() {
        assertThat(orderFor("FROM test | KEEP unmapped.*, emp_no", "unmapped.a"), equalTo(List.of("unmapped.a", "emp_no")));
        assertThat(orderFor("FROM test | KEEP emp_no, unmapped.*", "unmapped.a"), equalTo(List.of("emp_no", "unmapped.a")));
    }

    /** A DROP after the KEEP is replayed too, so it removes real columns and discovered fields alike. */
    public void testDropAfterKeepAppliesToDiscoveredFields() {
        assertThat(
            orderFor("FROM test | KEEP emp_no, first_name, unmapped.* | DROP first_name", "unmapped.a"),
            equalTo(List.of("emp_no", "unmapped.a"))
        );
        assertThat(
            orderFor("FROM test | KEEP emp_no, unmapped.* | DROP unmapped.b*", "unmapped.a", "unmapped.b1"),
            equalTo(List.of("emp_no", "unmapped.a"))
        );
    }

    public void testRenameAfterKeepAppliesToDiscoveredFields() {
        assertThat(
            orderFor("FROM test | KEEP emp_no, unmapped.* | RENAME emp_no AS id", "unmapped.a"),
            equalTo(List.of("id", "unmapped.a"))
        );
    }

    public void testTransparentCommandsAfterKeepDoNotDisturbOrder() {
        assertThat(
            orderFor("FROM test | KEEP emp_no, unmapped.* | WHERE emp_no > 0 | LIMIT 5", "unmapped.a"),
            equalTo(List.of("emp_no", "unmapped.a"))
        );
    }

    /** Without a governing KEEP the discovered fields still sit with the relation columns, after them and before anything later. */
    public void testWithoutGoverningKeepDiscoveredFieldsFollowTheRelationColumns() {
        List<String> order = orderFor("FROM test | DROP salary", "unmapped.a");
        assertThat(order, hasItem("unmapped.a"));
        assertThat(order, not(hasItem("salary")));
        assertThat(order.get(order.size() - 1), equalTo("unmapped.a"));
    }

    public void testDiscoveredFieldsTakeTheRelationSlotSoKeepPositionsThem() {
        assertThat(
            orderFor("FROM test | KEEP emp_no, unmapped.*", "unmapped.a", "unmapped.b"),
            equalTo(List.of("emp_no", "unmapped.a", "unmapped.b"))
        );
        assertThat(orderFor("FROM test | KEEP unmapped.*, emp_no", "unmapped.a"), equalTo(List.of("unmapped.a", "emp_no")));
    }

    /** An EVAL column is added on top of the relation, so it trails the discovered fields rather than being interleaved with them. */
    public void testEvalColumnTrailsDiscoveredFields() {
        assertThat(
            orderFor("FROM test | KEEP emp_no, unmapped.* | EVAL x = 1", "unmapped.a"),
            equalTo(List.of("emp_no", "unmapped.a", "x"))
        );
    }

    public void testRenameSwapAboveKeep() {
        assertThat(
            orderFor(
                "FROM test | KEEP emp_no, first_name, unmapped.* | RENAME emp_no AS t, first_name AS emp_no, t AS first_name",
                "unmapped.a"
            ),
            equalTo(List.of("first_name", "emp_no", "unmapped.a"))
        );
    }

    /** An EVAL re-creating a renamed-away name used to alias two live columns onto one original, dropping one of them. */
    public void testEvalRecreatingRenamedAwayNameAboveKeep() {
        assertThat(
            orderFor(
                "FROM test | KEEP emp_no, unmapped.* | RENAME emp_no AS a | RENAME a AS b | EVAL emp_no = 1 | RENAME emp_no AS c",
                "unmapped.a"
            ),
            equalTo(List.of("b", "unmapped.a", "c"))
        );
    }

    public void testReplayWithNoDiscoveredFieldsReproducesTheRealColumns() {
        assertThat(orderFor("FROM test | KEEP emp_no, unmapped.* | RENAME emp_no AS id"), equalTo(List.of("id")));
    }

    public void testReusedAnalyzerDoesNotReportThePreviousQuerysOrdering() {
        TestAnalyzer test = test();
        test.statement(setUnmappedLoadAll("FROM test | KEEP emp_no, unmapped.*"));
        Analyzer analyzer = test.lastAnalyzer();
        assertThat(analyzer.unmappedFieldsOrdering(), notNullValue());

        analyzer.analyze(EsqlTestUtils.TEST_PARSER.createStatement(setUnmappedLoadAll("FROM test | KEEP emp_no")).plan());
        // here the only field we are KEEPing is a known one and also this is not a pattern (that could match unmapped fields)
        // so, there is no need to do anything from LOAD_ALL point of view, thus the null for UnmappedFieldsOrdering
        assertThat(analyzer.unmappedFieldsOrdering(), nullValue());
    }

    public void testNoOrderingRecordedWithoutLoadAll() {
        TestAnalyzer withLoadAll = test();
        withLoadAll.statement(setUnmappedLoadAll("FROM test | KEEP emp_no, first*"));
        assertThat(withLoadAll.lastAnalyzer().unmappedFieldsOrdering(), notNullValue());

        TestAnalyzer withoutLoadAll = test();
        withoutLoadAll.statement("FROM test | KEEP emp_no, first*");
        // no SET unmapped_fields = "load_all", nothing to do and UnmappedFieldsOrdering stays unset
        assertThat(withoutLoadAll.lastAnalyzer().unmappedFieldsOrdering(), nullValue());
    }

    public void testForkPlacesDiscoveredFieldsAfterForkColumn() {
        assertThat(
            orderFor("FROM test | FORK (WHERE true) (WHERE true)", "unmapped.a"),
            equalTo(
                List.of(
                    "_meta_field",
                    "emp_no",
                    "first_name",
                    "gender",
                    "hire_date",
                    "job",
                    "job.raw",
                    "languages",
                    "last_name",
                    "long_noidx",
                    "salary",
                    "_fork",
                    "unmapped.a"
                )
            )
        );
    }

    public void testForkKeepAfterForkOrdersUnmappedWildcardBeforeLaterWildcard() {
        assertThat(
            orderFor("FROM test | FORK (WHERE true) (WHERE true) | KEEP _fork, emp_no, unmapped*, zz*", "unmapped.a", "zz_code"),
            equalTo(List.of("_fork", "emp_no", "unmapped.a", "zz_code"))
        );
    }

    public void testForkPatternLessKeepInEveryBranchHasNoOrdering() {
        TestAnalyzer analyzer = test();
        analyzer.statement(setUnmappedLoadAll("FROM test | FORK (KEEP emp_no) (KEEP first_name)"));
        assertThat(analyzer.lastAnalyzer().unmappedFieldsOrdering(), nullValue());
    }

    // Parked while LOAD_ALL subquery semantics are rebuilt case by case.
    /*
    public void testSubqueryStatsInOneBranchOrderingMatchesAnalyzedOutput() {
        assertReplayMatchesAnalyzedOutput(test(), """
            FROM (FROM test), (FROM test | STATS c = COUNT(*))
            | SORT c NULLS LAST
            """, "unmapped_extra");
    }

    public void testSubqueryKeepWildcardAfterSortOrderingMatchesAnalyzedOutput() {
        assertReplayMatchesAnalyzedOutput(test(), """
            FROM (FROM test | WHERE emp_no == 10001), (FROM test | WHERE emp_no == 10002)
            | SORT emp_no DESC
            | KEEP emp*, unmapped*
            """, "unmapped_foo");
    }

    public void testPartialMappingSubqueryStatsOrderingMatchesOptimizedOutput() {
        assertReplayMatchesAnalyzedOutput(partialMappingTest(), """
            FROM (FROM partial_mapping_sample_data | STATS c = COUNT(*)),
                 (FROM partial_mapping_sample_data | WHERE message == "42")
            | SORT c NULLS LAST
            """, "unmapped_message");
    }

    public void testPartialMappingSubqueryKeepWildcardInOneBranchOrderingMatchesOptimizedOutput() {
        assertReplayMatchesAnalyzedOutput(partialMappingTest(), """
            FROM (FROM partial_mapping_sample_data | WHERE message == "Connected to 10.1.0.1!" | KEEP messag*),
                 (FROM partial_mapping_sample_data | WHERE message == "42")
            | SORT message
            """, "language_code", "unmapped.nested", "unmapped_event_duration", "unmapped_message");
    }

    public void testPartialMappingSubqueryKeepWildcardAfterSortOrderingMatchesOptimizedOutput() {
        assertReplayMatchesAnalyzedOutput(partialMappingTest(), """
            FROM (FROM partial_mapping_sample_data | WHERE message == "42"),
                 (FROM partial_mapping_sample_data | WHERE message == "Connected to 10.1.0.1!"),
                 (FROM partial_mapping_sample_data | WHERE message == "Connected to 10.1.0.2!")
            | SORT unmapped_message DESC
            | KEEP messag*, unmapped_mess*
            """, "unmapped_message_extra");
    }
    */

    /**
     * Same size check {@code ExpandUnmappedFieldsPostProcessor} uses: replayed output must be the executed
     * columns (minus {@code $$unmapped_fields}) plus the discovered leaves.
     */
    private static void assertReplayMatchesAnalyzedOutput(TestAnalyzer analyzer, String query, String... discovered) {
        LogicalPlan analyzed = analyzer.statement(setUnmappedLoadAll(query));
        LogicalPlan optimized = new LogicalPlanOptimizer(unboundLogicalOptimizerContext()).optimize(analyzed);
        UnmappedFieldsOrdering ordering = analyzer.lastAnalyzer().unmappedFieldsOrdering();
        assertThat("no ordering captured for [" + query + "]", ordering, notNullValue());
        List<Attribute> leaves = Arrays.stream(discovered)
            .map(name -> (Attribute) new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD))
            .toList();
        List<String> replayed = Expressions.names(ordering.order(leaves));
        assertThat("replay dropped discovered fields: " + replayed, replayed, hasItems(discovered));
        assertThat(replayed, not(hasItem(UnmappedFieldsAttribute.ATTRIBUTE_NAME)));
        for (LogicalPlan plan : List.of(analyzed, optimized)) {
            List<String> executed = new ArrayList<>();
            for (String name : Expressions.names(plan.output())) {
                if (name.equals(UnmappedFieldsAttribute.ATTRIBUTE_NAME) == false) {
                    executed.add(name);
                }
            }
            assertThat(
                "stage="
                    + (plan == analyzed ? "analyzed" : "optimized")
                    + " replay="
                    + replayed
                    + " executedWithoutUfa="
                    + executed
                    + " plan="
                    + plan,
                replayed.size(),
                equalTo(executed.size() + discovered.length)
            );
        }
    }

    private static List<String> orderFor(String query, String... discovered) {
        TestAnalyzer analyzer = test();
        analyzer.statement(setUnmappedLoadAll(query));
        UnmappedFieldsOrdering ordering = analyzer.lastAnalyzer().unmappedFieldsOrdering();
        assertThat("no ordering captured for [" + query + "]", ordering, notNullValue());
        List<Attribute> leaves = Arrays.stream(discovered)
            .map(name -> (Attribute) new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD))
            .toList();
        return Expressions.names(ordering.order(leaves));
    }

}
