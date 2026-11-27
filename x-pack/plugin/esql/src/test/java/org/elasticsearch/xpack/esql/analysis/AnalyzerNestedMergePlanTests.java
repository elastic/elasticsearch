/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * {@code NESTED_EXPAND} and {@code NESTED_ANY} after a command that merges branches, {@code FORK} or a subquery, read each
 * branch's own nested documents, so they apply inside every branch. The indices are the ones {@link AnalyzerNestedExpandTests}
 * describes.
 */
public class AnalyzerNestedMergePlanTests extends AnalyzerTestCase {
    public AnalyzerNestedMergePlanTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireCapability() {
        assumeTrue("NESTED_EXPAND is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND.isEnabled());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testExpandAfterFork() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne | FORK (WHERE id == \"1\") (WHERE id == \"2\") | NESTED_EXPAND items | KEEP id, items.sku, _fork"
        );
        assertThat(expands(plan), hasSize(2));
        assertThat(names(plan.output()), contains("id", "items.sku", "_fork"));
    }

    public void testExpandAfterSubqueries() {
        for (String source : List.of("(FROM ne), (FROM ne)", "ne, (FROM ne)", "(FROM ne | WHERE id == \"1\"), (FROM ne)")) {
            LogicalPlan plan = nestedAnalyzer().query("FROM " + source + " | NESTED_EXPAND items | KEEP id, items.sku");
            assertThat(source, expands(plan), hasSize(2));
            assertThat(source, names(plan.output()), contains("id", "items.sku"));
        }
    }

    public void testExpandAfterAForkReadsThroughAnAlias() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne | EVAL o = items | FORK (WHERE id == \"1\") (WHERE id == \"2\") | NESTED_EXPAND o | KEEP id, o.sku"
        );
        assertThat(expands(plan), hasSize(2));
        assertThat(names(plan.output()), contains("id", "o.sku"));
    }

    public void testAnyAfterFork() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne | FORK (WHERE id == \"1\") (WHERE id == \"2\") | WHERE NESTED_ANY(items.qty > 1) | KEEP id, _fork"
        );
        assertThat(predicates(plan), hasSize(2));
        assertThat(names(plan.output()), contains("id", "_fork"));
    }

    public void testAnyAfterSubqueries() {
        LogicalPlan plan = nestedAnalyzer().query("FROM (FROM ne), (FROM ne) | WHERE NESTED_ANY(items, items.qty > 1) | KEEP id");
        assertThat(predicates(plan), hasSize(2));
    }

    public void testAnyInEvalAfterFork() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM ne | FORK (WHERE id == \"1\") (WHERE id == \"2\") | EVAL hit = NESTED_ANY(items.qty > 1) | KEEP id, hit"
        );
        assertThat(predicates(plan), hasSize(2));
        assertThat(names(plan.output()), hasItems("id", "hit"));
    }

    public void testExpandAfterAViewAndASubquery() {
        LogicalPlan plan = nestedAnalyzer().addView("v", "FROM ne | WHERE id == \"1\"")
            .query("FROM v, (FROM ne) | NESTED_EXPAND items | KEEP id, items.sku");
        assertThat(expands(plan), hasSize(2));
    }

    public void testExpandInsideEveryBranchStillWorks() {
        LogicalPlan plan = nestedAnalyzer().query(
            "FROM (FROM ne | NESTED_EXPAND items), (FROM ne | NESTED_EXPAND items) | KEEP id, items.sku"
        );
        assertThat(expands(plan), hasSize(2));
    }

    public void testNoMergePlanIsUntouched() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | NESTED_EXPAND items | KEEP id, items.sku");
        assertThat(expands(plan), hasSize(1));
        assertThat(plan.anyMatch(MergePlan.class::isInstance), is(false));
    }

    private TestAnalyzer nestedAnalyzer() {
        return AnalyzerNestedExpandTests.nestedAnalyzer(minimumVersionAtLeast(NestedPredicate.VERSION));
    }

    private static List<NestedExpand> expands(LogicalPlan plan) {
        List<NestedExpand> found = new ArrayList<>();
        plan.forEachDown(NestedExpand.class, found::add);
        return found;
    }

    private static List<NestedPredicate> predicates(LogicalPlan plan) {
        List<NestedPredicate> found = new ArrayList<>();
        plan.forEachExpressionDown(NestedPredicate.class, found::add);
        return found;
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }
}
