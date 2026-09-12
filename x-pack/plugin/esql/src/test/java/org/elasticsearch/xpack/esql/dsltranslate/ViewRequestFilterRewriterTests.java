/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link ViewRequestFilterRewriter}: the request-filter policy that installs a {@link Filter} above each
 * view branch in a {@link ViewUnionAll} (identified via {@link ViewUnionAll#isViewBranch(String)}), bound against the
 * view's output schema.
 */
public class ViewRequestFilterRewriterTests extends ESTestCase {

    private static final long NOW = 1_600_000_000_000L;
    private static final Configuration CONFIG = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).now(Instant.ofEpochMilli(NOW)).build();
    private static final TransportVersion CURRENT = RequestFilterRewriter.ESQL_REQUEST_FILTER_ON_DATASET;
    private static final TransportVersion TOO_OLD = TransportVersion.minimumCompatible();

    // --- helpers ---

    private static ReferenceAttribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    /**
     * Creates a lightweight plan that outputs the given attributes. Used to represent a view subplan's top node (e.g.
     * the aggregate or filter at the top of the view's pipeline) without spinning up a full logical plan.
     */
    private static ExternalRelation viewSubplan(String viewName, Attribute... attrs) {
        List<Attribute> output = List.of(attrs);
        String path = "file:///view/" + viewName;
        SourceMetadata metadata = new SimpleSourceMetadata(output, "test", path);
        return new ExternalRelation(Source.EMPTY, path, metadata, output, FileList.UNRESOLVED, Map.of(), viewName);
    }

    /** A bare EsRelation standing in for a plain index relation (null key in ViewUnionAll). */
    private static EsRelation bareIndex() {
        return EsqlTestUtils.relation();
    }

    /**
     * Builds a {@link ViewUnionAll} with one null-key (bare index) child and one named (view) child that outputs
     * the given attribute. The view child is registered in {@code viewBranchKeys} so that
     * {@link ViewUnionAll#isViewBranch(String)} returns {@code true} for it.
     */
    private static ViewUnionAll unionWithView(String viewName, Attribute viewOutputAttr) {
        LogicalPlan viewPlan = viewSubplan(viewName, viewOutputAttr);
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put(null, bareIndex());
        map.put(viewName, viewPlan);
        return new ViewUnionAll(Source.EMPTY, map, Set.of(viewName), List.of(viewOutputAttr));
    }

    /** Retrieves the named subplan for {@code viewName} from a (possibly rewritten) {@link ViewUnionAll}. */
    private static LogicalPlan viewChild(LogicalPlan plan, String viewName) {
        assertThat(plan, instanceOf(ViewUnionAll.class));
        LogicalPlan child = ((ViewUnionAll) plan).namedSubqueries().get(viewName);
        assertNotNull("expected a child for view '" + viewName + "'", child);
        return child;
    }

    /**
     * Retrieves the null-key (bare index) child from a (possibly rewritten) {@link ViewUnionAll}.
     * In tests, bare-index branches are stored under the {@code null} key to match the
     * {@link org.elasticsearch.xpack.esql.view.ViewCompaction} convention.
     */
    private static LogicalPlan bareIndexChild(LogicalPlan plan) {
        assertThat(plan, instanceOf(ViewUnionAll.class));
        LogicalPlan child = ((ViewUnionAll) plan).namedSubqueries().get(null);
        assertNotNull("expected a bare-index child (null key)", child);
        return child;
    }

    // --- core behaviour ---

    /** A null request filter is a no-op: the plan is returned unchanged. */
    public void testNullFilterLeavesPlanUnchanged() {
        ViewUnionAll vua = unionWithView("myView", attr("y", DataType.INTEGER));
        assertSame(vua, ViewRequestFilterRewriter.rewrite(vua, null, true, CONFIG, CURRENT));
    }

    /**
     * A supported term filter is translated and installed as an ordinary {@link Filter} above the view subplan, bound
     * against the view's output schema (field {@code y} present → binds to its attribute).
     */
    public void testSupportedFilterIsInstalledAboveViewSubplan() {
        Attribute y = attr("y", DataType.INTEGER);
        ViewUnionAll vua = unionWithView("myView", y);
        LogicalPlan original = vua.namedSubqueries().get("myView");

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 42), true, CONFIG, CURRENT);

        LogicalPlan viewChild = viewChild(result, "myView");
        assertThat("filter is installed above the view subplan", viewChild, instanceOf(Filter.class));
        assertThat(((Filter) viewChild).child(), sameInstance(original));
    }

    /**
     * The bare-index child (null key) is left untouched: it uses the existing Lucene-scan request-filter path and
     * must not receive a duplicate logical {@link Filter}.
     */
    public void testBareIndexChildIsNotWrapped() {
        LogicalPlan bare = bareIndex();
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put(null, bare);
        map.put("myView", viewSubplan("myView", attr("y", DataType.INTEGER)));
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, map, Set.of("myView"), List.of(attr("y", DataType.INTEGER)));

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 42), true, CONFIG, CURRENT);
        assertThat(bareIndexChild(result), sameInstance(bare));
        assertThat(bareIndexChild(result), not(instanceOf(Filter.class)));
    }

    /**
     * A {@link ViewUnionAll} that contains only a bare-index child (no view subplans) is returned unchanged: there is
     * nothing to filter.
     */
    public void testPlanWithNoViewSubplansIsUnchanged() {
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put(null, bareIndex());
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, map, Set.of(), List.of());

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 42), true, CONFIG, CURRENT);
        assertSame(vua, result);
    }

    /**
     * A plan containing no {@link ViewUnionAll} nodes at all (e.g. a plain index-only query) is returned unchanged.
     */
    public void testPlanWithNoViewUnionAllIsUnchanged() {
        LogicalPlan indexOnly = bareIndex();
        assertSame(indexOnly, ViewRequestFilterRewriter.rewrite(indexOnly, QueryBuilders.termQuery("y", 42), true, CONFIG, CURRENT));
    }

    /**
     * A {@code match_all} filter translates to {@link org.elasticsearch.xpack.esql.core.expression.Literal#TRUE TRUE},
     * which is a supported no-op: the view is left unfiltered rather than wrapped in a trivially-true {@link Filter}.
     */
    public void testMatchAllFilterLeavesViewUnfilteredNoWrap() {
        ViewUnionAll vua = unionWithView("myView", attr("y", DataType.INTEGER));
        LogicalPlan original = vua.namedSubqueries().get("myView");

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.matchAllQuery(), true, CONFIG, CURRENT);
        // match_all → TRUE → view subplan not wrapped
        assertThat(viewChild(result, "myView"), sameInstance(original));
    }

    // --- multi-view ---

    /**
     * All view subplans in a single {@link ViewUnionAll} are wrapped independently, each bound against that subplan's
     * own output schema. A field present in one view but absent in another binds to {@code NULL} in the second.
     */
    public void testMultipleViewSubplansAreEachWrapped() {
        Attribute y = attr("y", DataType.INTEGER);
        Attribute z = attr("z", DataType.INTEGER);
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put("viewA", viewSubplan("viewA", y));
        map.put("viewB", viewSubplan("viewB", z));
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, map, Set.of("viewA", "viewB"), List.of(y, z));

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 1), true, CONFIG, CURRENT);

        // viewA has field y → bound to the attribute → real filter
        assertThat(viewChild(result, "viewA"), instanceOf(Filter.class));
        // viewB does not have field y → bound to NULL → FALSE under term → still a Filter (non-trivial condition)
        assertThat(viewChild(result, "viewB"), instanceOf(Filter.class));
    }

    // --- fail-closed ---

    /** Fail-closed: an unsupported DSL construct fails the whole query with a 400 (IllegalArgumentException). */
    public void testUnsupportedDslConstructFailsTheQuery() {
        ViewUnionAll vua = unionWithView("myView", attr("y", DataType.KEYWORD));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.wildcardQuery("y", "x*"), true, CONFIG, CURRENT)
        );
        assertThat(e.getMessage(), containsString("[wildcard]"));
        assertThat(e.getMessage(), containsString("views"));
    }

    /**
     * Fail-closed: a filter that mixes a supported term with an unsupported wildcard fails the whole query — the
     * supported clause does not rescue it.
     */
    public void testMixedDslWithUnsupportedClauseFailsTheQuery() {
        ViewUnionAll vua = unionWithView("myView", attr("y", DataType.KEYWORD));
        expectThrows(
            IllegalArgumentException.class,
            () -> ViewRequestFilterRewriter.rewrite(
                vua,
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("y", "a")).must(QueryBuilders.wildcardQuery("y", "x*")),
                true,
                CONFIG,
                CURRENT
            )
        );
    }

    // --- feature flag gate ---

    /**
     * When the feature is disabled (release build without the flag) the view is read unfiltered and a warning names
     * every distinct view, rather than the filter being silently dropped.
     */
    public void testDisabledLeavesThePlanUnchangedAndWarns() {
        ViewUnionAll vua = unionWithView("myView", attr("y", DataType.INTEGER));

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 1), false, CONFIG, CURRENT);
        assertSame(vua, result);
        assertWarnings(
            "The request filter was not applied to view(s) [myView] because applying the request filter to "
                + "views is not enabled in this build; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from views instead"
        );
    }

    /** Disabled short-circuits before translation, so even an unsupported construct does not fail the query. */
    public void testDisabledDoesNotFailOnUnsupportedConstruct() {
        ViewUnionAll vua = unionWithView("myView", attr("y", DataType.KEYWORD));
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.wildcardQuery("y", "x*"), false, CONFIG, CURRENT);
        assertSame(vua, result);
        assertWarnings(
            "The request filter was not applied to view(s) [myView] because applying the request filter to "
                + "views is not enabled in this build; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from views instead"
        );
    }

    /**
     * When the flag is disabled and there are no view subplans in the plan, no warning is emitted: there is nothing to
     * tell the user about.
     */
    public void testDisabledWithNoViewsEmitsNoWarning() {
        LogicalPlan indexOnly = bareIndex();
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(indexOnly, QueryBuilders.termQuery("y", 1), false, CONFIG, CURRENT);
        assertSame(indexOnly, result);
        // no warning expected; ESTestCase.assertWarnings checks all headers are clear if not called
    }

    // --- version gate ---

    /**
     * The critical version gate: below {@link RequestFilterRewriter#ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is
     * skipped entirely so that no plan an old node cannot deserialize is shipped.
     */
    public void testOldMinimumVersionSkipsRewriteAndWarns() {
        ViewUnionAll vua = unionWithView("myView", attr("y", DataType.INTEGER));

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 1), true, CONFIG, TOO_OLD);
        assertSame(vua, result);
        assertWarnings(
            "The request filter was not applied to view(s) [myView] because the cluster contains a node "
                + "too old to evaluate the translated filter; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from views instead"
        );
    }

    /** A version-gate skip with no views in the plan is silent. */
    public void testOldVersionWithNoViewsIsQuiet() {
        LogicalPlan indexOnly = bareIndex();
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(indexOnly, QueryBuilders.termQuery("y", 1), true, CONFIG, TOO_OLD);
        assertSame(indexOnly, result);
    }

    /** The version-gate warning names all distinct views in the plan once. */
    public void testVersionGateWarningNamesAllViewsOnce() {
        Attribute y = attr("y", DataType.INTEGER);
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put("viewA", viewSubplan("viewA", y));
        map.put("viewB", viewSubplan("viewB", y));
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, map, Set.of("viewA", "viewB"), List.of(y));

        ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 1), true, CONFIG, TOO_OLD);
        assertWarnings(
            "The request filter was not applied to view(s) [viewA, viewB] because the cluster contains a node "
                + "too old to evaluate the translated filter; they were read unfiltered. "
                + "Use a WHERE clause to filter rows from views instead"
        );
    }

    /**
     * Kibana sends an empty filter rather than omitting the field when no filtering is wanted. Such a filter would translate to a
     * no-op and install nothing, so it must not be treated as needing view boundaries — otherwise it suppresses view compaction for
     * nothing. Covers the shapes the translator folds to TRUE: match_all, an empty bool, and bools nesting only empty ones.
     */
    public void testMatchEverythingFiltersDoNotNeedViewBoundaries() {
        assertFalse("no filter at all", ViewRequestFilterRewriter.appliesToViewOutputs(null));
        assertFalse("match_all", ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.matchAllQuery()));
        assertFalse("empty bool", ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.boolQuery()));
        assertFalse(
            "bool filtering on match_all",
            ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.boolQuery().filter(QueryBuilders.matchAllQuery()))
        );
        assertFalse(
            "bool nesting an empty bool under must",
            ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()))
        );
        assertFalse(
            "bool nesting an empty bool under filter",
            ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery()))
        );
    }

    /** Anything that can exclude a document needs the boundaries kept. */
    public void testRealFiltersNeedViewBoundaries() {
        assertTrue("term", ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.termQuery("region", "eu")));
        assertTrue("range", ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.rangeQuery("cnt").gt(0)));
        assertTrue(
            "bool with a real must",
            ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("region", "eu")))
        );
        // should / must_not count as filtering even in the shapes the translator would discard: narrower is the safe direction.
        assertTrue(
            "bool with only a should",
            ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("region", "eu")))
        );
        assertTrue(
            "bool with only a must_not",
            ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("region", "eu")))
        );
        // An unsupported construct must not be mistaken for a no-op — it has to reach the fail-closed translation.
        assertTrue("unsupported wildcard", ViewRequestFilterRewriter.appliesToViewOutputs(QueryBuilders.wildcardQuery("region", "e*")));
    }

    /**
     * The exact payload Kibana sends when no filtering is wanted — a bool with all four clause lists present but empty. Parsed from
     * JSON rather than hand-built, so this pins the real wire format: if a future change made the emptiness test depend on the lists
     * being absent rather than empty, this catches it.
     */
    public void testKibanaEmptyFilterPayloadDoesNotNeedViewBoundaries() throws IOException {
        QueryBuilder kibanaEmpty = parseFilter("""
            {
              "bool": {
                "must": [],
                "filter": [],
                "should": [],
                "must_not": []
              }
            }
            """);
        assertThat(kibanaEmpty, instanceOf(BoolQueryBuilder.class));
        assertFalse("Kibana's empty filter must not preserve view boundaries", ViewRequestFilterRewriter.appliesToViewOutputs(kibanaEmpty));
        // Sanity: the same bool with one real clause does need them, so the assertion above is not vacuous.
        QueryBuilder withClause = parseFilter("""
            { "bool": { "must": [ { "term": { "region": "eu" } } ], "filter": [], "should": [], "must_not": [] } }
            """);
        assertTrue(ViewRequestFilterRewriter.appliesToViewOutputs(withClause));
    }

    private static QueryBuilder parseFilter(String json) throws IOException {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(
            new NamedXContentRegistry(searchModule.getNamedXContents())
        );
        try (XContentParser parser = XContentType.JSON.xContent().createParser(config, json)) {
            return AbstractQueryBuilder.parseTopLevelQuery(parser);
        }
    }
}
