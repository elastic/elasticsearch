/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.NestedFlatten;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerNestedExpandTests.nestedAnalyzer;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Binding of {@code NESTED_FLATTEN}. The mappings are the ones {@link AnalyzerNestedExpandTests} builds, so the private
 * nested schema is the same one {@code NESTED_EXPAND} and {@code NESTED_ANY} read.
 */
public class AnalyzerNestedFlattenTests extends AnalyzerTestCase {
    public AnalyzerNestedFlattenTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireCapability() {
        assumeTrue("NESTED_FLATTEN is snapshot-only", EsqlCapabilities.Cap.NESTED_FLATTEN.isEnabled());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testBindsChildColumns() {
        NestedFlatten flatten = flatten(nested().query("FROM ne | NESTED_FLATTEN items"));
        assertThat(flatten.path(), equalTo("items"));
        assertThat(names(flatten.expanded()), contains("items.loc", "items.qty", "items.sku"));
        assertThat(flatten.paths(), contains("loc", "qty", "sku"));
        assertThat(types(flatten.expanded()), contains(DataType.GEO_POINT, DataType.LONG, DataType.KEYWORD));
    }

    public void testOutputHasNoNestedScope() {
        LogicalPlan plan = nested().query("FROM ne | NESTED_FLATTEN items");
        assertThat(names(plan.output()), hasItem("items.qty"));
        assertThat(names(plan.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))));
        plan.forEachDown(
            EsRelation.class,
            relation -> assertThat(names(relation.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))))
        );
    }

    /** {@code NESTED_EXPAND} still owns the names it publishes. Flattening a different collection is fine. */
    public void testExpandAndFlattenDifferentCollections() {
        LogicalPlan plan = nested().query("FROM ne | NESTED_FLATTEN items | NESTED_EXPAND events | KEEP id, items.qty, events.start");
        assertThat(flatten(plan).path(), equalTo("items"));
        List<NestedExpand> expands = new ArrayList<>();
        plan.forEachDown(NestedExpand.class, expands::add);
        assertThat(expands.getFirst().path(), equalTo("events"));
    }

    public void testExpandOfTheSameCollectionWinsTheName() {
        nested().error(
            "FROM ne | NESTED_EXPAND items | NESTED_FLATTEN items",
            containsString("NESTED_FLATTEN requires a mapped nested field, not [items]")
        );
    }

    public void testAnyThenFlatten() {
        LogicalPlan plan = nested().query("FROM ne | WHERE NESTED_ANY(items.qty > 1) | NESTED_FLATTEN items | KEEP id, items.qty");
        assertThat(flatten(plan).path(), equalTo("items"));
        assertThat(names(plan.output()), contains("id", "items.qty"));
    }

    public void testCastOfConflictingChildIsReadByTheFlatten() {
        LogicalPlan plan = nested().query("FROM ne_types | NESTED_FLATTEN items | EVAL x = TO_LONG(items.v) | KEEP x");
        NestedFlatten flatten = flatten(plan);
        List<String> names = names(flatten.expanded());
        int generated = names.indexOf("$$items.v$converted_to$long");
        assertThat(names, hasItem("$$items.v$converted_to$long"));
        assertThat(flatten.paths().get(generated), equalTo("v"));
        assertThat(((FieldAttribute) flatten.expanded().get(generated)).field(), instanceOf(UnionTypeEsField.class));
    }

    public void testMatchOnFlattenedChildIsRejected() {
        nested().error(
            "FROM ne | NESTED_FLATTEN items | WHERE MATCH(items.sku, \"a\") | KEEP id",
            containsString("Full-text matching on expanded nested fields is not supported yet")
        );
    }

    public void testLookupIndex() {
        nested().error(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | NESTED_FLATTEN items",
            containsString("NESTED_FLATTEN cannot read [items] from a LOOKUP JOIN index")
        );
    }

    public void testOneNestingLevel() {
        nested().error("FROM ne | NESTED_FLATTEN items.parts", containsString("NESTED_FLATTEN supports one nesting level"));
    }

    public void testAfterStats() {
        nested().error("FROM ne | STATS c = COUNT(*) BY id | NESTED_FLATTEN items", equalTo("1:53: Unknown column [items]"));
    }

    public void testAfterInlineStats() {
        nested().error(
            "FROM ne | INLINE STATS c = COUNT(*) BY id | NESTED_FLATTEN items",
            equalTo("1:60: NESTED_FLATTEN reads each row's nested documents, so it cannot follow [INLINE STATS c = COUNT(*) BY id]")
        );
    }

    public void testOlderNode() {
        nestedAnalyzer(TransportVersionUtils.randomVersionNotSupporting(NestedFlatten.VERSION)).error(
            "FROM ne | NESTED_FLATTEN items",
            equalTo("1:26: NESTED_FLATTEN requires all nodes to support nested flattening")
        );
    }

    public void testBeforeTimeSeriesStats() {
        nested().error(
            "TS ne_ts | NESTED_FLATTEN items | STATS max(cpu)",
            equalTo(
                "1:12: nested_flatten [NESTED_FLATTEN items] in the time-series before the first aggregation [STATS max(cpu)] is not allowed"
            )
        );
    }

    private TestAnalyzer nested() {
        return nestedAnalyzer(minimumVersionAtLeast(NestedFlatten.VERSION));
    }

    private static NestedFlatten flatten(LogicalPlan plan) {
        List<NestedFlatten> found = new ArrayList<>();
        plan.forEachDown(NestedFlatten.class, found::add);
        assertThat(found, not(empty()));
        return found.getFirst();
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }

    private static List<DataType> types(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::dataType).toList();
    }
}
