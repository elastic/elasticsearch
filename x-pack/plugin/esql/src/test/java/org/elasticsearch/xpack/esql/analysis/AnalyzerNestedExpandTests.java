/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.mergedResolution;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Binding and rejection of {@code NESTED_EXPAND} during analysis. The mappings go through
 * {@link org.elasticsearch.xpack.esql.session.IndexResolver} from field caps, so the nested scope fields the command reads
 * are built as they are for a real query.
 * <ul>
 *     <li>{@code ne}: {@code id}, nested {@code items} with {@code sku}, {@code qty}, {@code loc} and nested {@code items.parts},
 *     and nested {@code events} with {@code start}.</li>
 *     <li>{@code ne_orders}: only {@code id}.</li>
 *     <li>{@code ne_lookup}: a lookup index with {@code id} and nested {@code items}.</li>
 *     <li>{@code ne_conflict}: two indices, one maps {@code items.sub} as an object and the other as nested.</li>
 *     <li>{@code ne_ts}: a time-series index with nested {@code items}.</li>
 * </ul>
 */
public class AnalyzerNestedExpandTests extends AnalyzerTestCase {
    public AnalyzerNestedExpandTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireCapability() {
        assumeTrue("NESTED_EXPAND is snapshot-only", Build.current().isSnapshot());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testBindsChildColumns() {
        NestedExpand expand = expand(nestedAnalyzer().query("FROM ne | NESTED_EXPAND items"));
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("items.loc", "items.qty", "items.sku"));
        assertThat(expand.paths(), contains("loc", "qty", "sku"));
        assertThat(types(expand.expanded()), contains(DataType.GEO_POINT, DataType.LONG, DataType.KEYWORD));
    }

    /** The children leave the private schema as ordinary columns. The rest of the private schema stays out of the output. */
    public void testOutputHasNoNestedScope() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | NESTED_EXPAND items");
        assertThat(names(plan.output()), hasItems("id", "items.qty", "items.sku"));
        assertThat(names(plan.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))));
        plan.forEachDown(
            EsRelation.class,
            relation -> assertThat(names(relation.output()), everyItem(not(startsWith(NestedScopeField.PREFIX))))
        );
    }

    public void testWithoutNestedExpandChildrenAreUnknown() {
        nestedAnalyzer().error("FROM ne | KEEP items.sku", equalTo("1:16: Unknown column [items.sku]"));
    }

    public void testMappedChildBeforeExpansionIsUnknown() {
        nestedAnalyzer().error("FROM ne | WHERE items.sku == \"a\" | NESTED_EXPAND items", equalTo("1:17: Unknown column [items.sku]"));
    }

    public void testUndeclaredNameBeforeExpansionLoadsFromParent() {
        LogicalPlan plan = nestedAnalyzer().statement(
            "SET unmapped_fields=\"load\"; FROM ne | EVAL u = items.undeclared | NESTED_EXPAND items | KEEP id, u"
        );
        assertLoadedFromParent(plan);
    }

    public void testUndeclaredNameAfterExpansionLoadsFromParent() {
        LogicalPlan plan = nestedAnalyzer().statement(
            "SET unmapped_fields=\"load\"; FROM ne | NESTED_EXPAND items | EVAL u = items.undeclared | KEEP id, u"
        );
        assertLoadedFromParent(plan);
    }

    public void testUnknownField() {
        nestedAnalyzer().error("FROM ne | NESTED_EXPAND missing", equalTo("1:25: Unknown column [missing]"));
    }

    public void testNotNestedField() {
        nestedAnalyzer().error("FROM ne | NESTED_EXPAND id", equalTo("1:25: NESTED_EXPAND requires a mapped nested field, not [id]"));
    }

    public void testChildOfNestedField() {
        nestedAnalyzer().error(
            "FROM ne | NESTED_EXPAND items.sku",
            equalTo("1:25: NESTED_EXPAND requires a mapped nested field, not [items.sku]")
        );
    }

    public void testOneNestingLevel() {
        nestedAnalyzer().error("FROM ne | NESTED_EXPAND items.parts", equalTo("1:25: NESTED_EXPAND supports one nesting level"));
    }

    /** A second collection keeps its private schema after the first expands, so it can be expanded in turn. */
    public void testSiblingCollection() {
        LogicalPlan plan = nestedAnalyzer().query("FROM ne | NESTED_EXPAND items | NESTED_EXPAND events");
        NestedExpand events = expand(plan);
        assertThat(events.path(), equalTo("events"));
        assertThat(names(events.expanded()), contains("events.start"));
        NestedExpand items = expand(events.child());
        assertThat(items.path(), equalTo("items"));
        assertThat(names(plan.output()), hasItems("items.sku", "events.start"));
    }

    public void testSameCollectionTwice() {
        nestedAnalyzer().error(
            "FROM ne | NESTED_EXPAND items | NESTED_EXPAND items",
            equalTo("1:47: NESTED_EXPAND requires a mapped nested field, not [items]")
        );
    }

    /** The lookup index's documents are not the rows being read, so the collection it brings has no children for them. */
    public void testCollectionOfALookupIndex() {
        nestedAnalyzer().error(
            "FROM ne_orders | LOOKUP JOIN ne_lookup ON id | NESTED_EXPAND items",
            equalTo("1:62: NESTED_EXPAND cannot read [items] from a LOOKUP JOIN index; that index's documents are not the rows being read")
        );
    }

    public void testCollectionOfTheDrivingIndexAfterALookupJoin() {
        NestedExpand expand = expand(nestedAnalyzer().query("FROM ne | LOOKUP JOIN ne_lookup_note ON id | NESTED_EXPAND items"));
        assertThat(expand.path(), equalTo("items"));
        assertThat(names(expand.expanded()), contains("items.loc", "items.qty", "items.sku"));
    }

    /** A STATS replaces the rows with groups, which have no child documents, and its output has no collection to expand. */
    public void testAfterStats() {
        nestedAnalyzer().error("FROM ne | STATS c = COUNT(*) BY id | NESTED_EXPAND items", equalTo("1:52: Unknown column [items]"));
    }

    /** An INLINE STATS keeps the parent columns, collection included, but its rows are computed from the groups. */
    public void testAfterInlineStats() {
        nestedAnalyzer().error(
            "FROM ne | INLINE STATS c = COUNT(*) BY id | NESTED_EXPAND items",
            equalTo("1:59: NESTED_EXPAND reads each row's nested documents, so it cannot follow [INLINE STATS c = COUNT(*) BY id]")
        );
    }

    /** A column named like the collection replaces it, and reading its children would contradict that column. */
    public void testColumnNamedLikeTheCollection() {
        nestedAnalyzer().error(
            "FROM ne | EVAL items = 1 | NESTED_EXPAND items",
            equalTo("1:42: NESTED_EXPAND cannot use [items]: a column of that name replaces it here")
        );
    }

    /** Each child's path comes from the first index, so another index nesting it elsewhere would read nulls there. */
    public void testIndicesDisagreeOnTheNestedPath() {
        nestedAnalyzer().error(
            "FROM ne_conflict | NESTED_EXPAND items",
            equalTo(
                "1:34: NESTED_EXPAND cannot read [items.sub]: its nested path is [items.sub] in [ne_conflict_nested] "
                    + "but [items] in [ne_conflict_object]"
            )
        );
    }

    public void testBeforeTimeSeriesStats() {
        nestedAnalyzer().error(
            "TS ne_ts | NESTED_EXPAND items | STATS max(cpu)",
            equalTo(
                "1:12: nested_expand [NESTED_EXPAND items] in the time-series before the first aggregation [STATS max(cpu)] is not allowed"
            )
        );
    }

    private TestAnalyzer nestedAnalyzer() {
        return nestedAnalyzer(minimumVersion);
    }

    /** An analyzer over the indices this class describes. */
    public static TestAnalyzer nestedAnalyzer(TransportVersion minimumVersion) {
        return EsqlTestUtils.analyzer()
            .addIndex("ne", resolution("ne", nestedIndex()))
            .addIndex("ne_orders", resolution("ne_orders", index("ne_orders", IndexMode.STANDARD, field("id", "keyword"))))
            .addIndex(
                "ne_conflict",
                resolution(
                    "ne_conflict",
                    index(
                        "ne_conflict_object",
                        IndexMode.STANDARD,
                        field("items", "nested"),
                        field("items.sub", "object"),
                        field("items.sub.v", "keyword")
                    ),
                    index(
                        "ne_conflict_nested",
                        IndexMode.STANDARD,
                        field("items", "nested"),
                        field("items.sub", "nested"),
                        field("items.sub.v", "keyword")
                    )
                )
            )
            .addIndex("ne_ts", resolution("ne_ts", timeSeriesIndex()))
            .addLookupIndex(
                "ne_lookup",
                resolution(
                    "ne_lookup",
                    index("ne_lookup", IndexMode.LOOKUP, field("id", "keyword"), field("items", "nested"), field("items.qty", "long"))
                )
            )
            .addLookupIndex(
                "ne_lookup_note",
                resolution("ne_lookup_note", index("ne_lookup_note", IndexMode.LOOKUP, field("id", "keyword"), field("note", "keyword")))
            )
            .stripErrorPrefix(true)
            .minimumTransportVersion(minimumVersion);
    }

    private static FieldCapabilitiesIndexResponse nestedIndex() {
        return index(
            "ne",
            IndexMode.STANDARD,
            field("id", "keyword"),
            field("items", "nested"),
            field("items.sku", "keyword"),
            field("items.qty", "long"),
            field("items.loc", "geo_point"),
            field("items.parts", "nested"),
            field("items.parts.pn", "keyword"),
            field("events", "nested"),
            field("events.start", "long")
        );
    }

    private static FieldCapabilitiesIndexResponse timeSeriesIndex() {
        return index(
            "ne_ts",
            IndexMode.TIME_SERIES,
            field("@timestamp", "date"),
            new IndexFieldCapabilitiesBuilder("host", "keyword").isDimension(true).build(),
            new IndexFieldCapabilitiesBuilder("cpu", "long").metricType(TimeSeriesParams.MetricType.GAUGE).build(),
            field("items", "nested"),
            field("items.qty", "long")
        );
    }

    private static IndexResolution resolution(String pattern, FieldCapabilitiesIndexResponse... indices) {
        return mergedResolution(pattern, FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build());
    }

    private static FieldCapabilitiesIndexResponse index(String name, IndexMode mode, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>();
        for (IndexFieldCapabilities field : fields) {
            byName.put(field.name(), field);
        }
        return new FieldCapabilitiesIndexResponse(name, name, byName, true, mode, 1, 0, 1L, 1L);
    }

    private static IndexFieldCapabilities field(String name, String type) {
        return new IndexFieldCapabilitiesBuilder(name, type).build();
    }

    private static void assertLoadedFromParent(LogicalPlan plan) {
        assertThat(names(expand(plan).expanded()), not(hasItem("items.undeclared")));
        List<EsRelation> relations = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relations::add);
        assertThat(relations, hasSize(1));
        assertThat(names(relations.getFirst().output()), hasItem("items.undeclared"));
    }

    private static NestedExpand expand(LogicalPlan plan) {
        List<NestedExpand> found = new ArrayList<>();
        plan.forEachDown(NestedExpand.class, found::add);
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
