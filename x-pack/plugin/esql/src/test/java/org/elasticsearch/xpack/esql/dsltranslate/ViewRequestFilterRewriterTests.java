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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.plan.logical.BranchIdentity;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link ViewRequestFilterRewriter}: the request-filter policy that installs a {@link Filter} above each
 * view branch in a {@link BranchPlan} (identified via {@link BranchPlan#isViewBranch(String)}), bound against the
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

    /** A bare EsRelation standing in for a plain index relation (null key in BranchPlan). */
    private static EsRelation bareIndex() {
        return EsqlTestUtils.relation();
    }

    /**
     * Builds a {@link BranchPlan} with one null-key (bare index) child and one named (view) child that outputs
     * the given attribute. The view child is registered in {@code viewBranchKeys} so that
     * {@link BranchPlan#isViewBranch(String)} returns {@code true} for it.
     */
    private static BranchPlan unionWithView(String viewName, Attribute viewOutputAttr) {
        LogicalPlan viewPlan = viewSubplan(viewName, viewOutputAttr);
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put(null, bareIndex());
        map.put(viewName, viewPlan);
        return new BranchPlan(Source.EMPTY, map, Map.of(viewName, BranchIdentity.view(viewName)), List.of(viewOutputAttr));
    }

    /** Retrieves the named subplan for {@code viewName} from a (possibly rewritten) {@link BranchPlan}. */
    private static LogicalPlan viewChild(LogicalPlan plan, String viewName) {
        assertThat(plan, instanceOf(BranchPlan.class));
        LogicalPlan child = ((BranchPlan) plan).namedSubqueries().get(viewName);
        assertNotNull("expected a child for view '" + viewName + "'", child);
        return child;
    }

    /**
     * Retrieves the null-key (bare index) child from a (possibly rewritten) {@link BranchPlan}.
     * In tests, bare-index branches are stored under the {@code null} key to match the
     * {@link org.elasticsearch.xpack.esql.view.ViewCompaction} convention.
     */
    private static LogicalPlan bareIndexChild(LogicalPlan plan) {
        assertThat(plan, instanceOf(BranchPlan.class));
        LogicalPlan child = ((BranchPlan) plan).namedSubqueries().get(null);
        assertNotNull("expected a bare-index child (null key)", child);
        return child;
    }

    // --- interaction with the dataset rewriter (elastic/elasticsearch#161957) ---

    /**
     * A dataset read inside a view's body must not be wrapped by {@link RequestFilterRewriter}: the filter belongs on
     * the view's output, where this rewriter installs it. Both run in session order here. Were the dataset leaf also
     * wrapped, a filter on the field the view computes would bind to NULL against the dataset schema and drop every row
     * before the view's EVAL ran.
     */
    public void testDatasetInsideViewIsNotFilteredOnViewComputedField() {
        ReferenceAttribute empNo = attr("emp_no", DataType.INTEGER);
        ReferenceAttribute doubled = attr("doubled", DataType.INTEGER);
        ExternalRelation dataset = viewSubplan("employees", empNo);
        LogicalPlan viewBody = new Eval(Source.EMPTY, dataset, List.of(new Alias(Source.EMPTY, "doubled", empNo, doubled.id())));
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put("emp_eval_view", viewBody);
        BranchPlan vua = new BranchPlan(
            Source.EMPTY,
            map,
            Map.of("emp_eval_view", BranchIdentity.view("emp_eval_view")),
            List.of(empNo, doubled)
        );

        QueryBuilder filter = QueryBuilders.termQuery("doubled", 2);
        LogicalPlan afterDataset = RequestFilterRewriter.rewrite(vua, filter, CONFIG, CURRENT, true);
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(afterDataset, filter, CONFIG, CURRENT);

        // The only Filter is the one on the view output; the dataset leaf is still the Eval's direct child.
        LogicalPlan child = viewChild(result, "emp_eval_view");
        assertThat(child, instanceOf(Filter.class));
        assertThat(((Filter) child).child(), instanceOf(Eval.class));
        assertThat(((Eval) ((Filter) child).child()).child(), sameInstance(dataset));
        assertThat(result.collect(Filter.class::isInstance), hasSize(1));
    }

    /** A dataset outside any view branch remains a target of the dataset rewriter, alongside the view-output filter. */
    public void testDatasetOutsideViewIsStillFilteredByDatasetRewriter() {
        ReferenceAttribute y = attr("y", DataType.INTEGER);
        ExternalRelation bareDataset = viewSubplan("bare_ds", y);
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put(null, bareDataset);
        map.put("myView", viewSubplan("myView", y));
        BranchPlan vua = new BranchPlan(Source.EMPTY, map, Map.of("myView", BranchIdentity.view("myView")), List.of(y));

        QueryBuilder filter = QueryBuilders.termQuery("y", 1);
        LogicalPlan afterDataset = RequestFilterRewriter.rewrite(vua, filter, CONFIG, CURRENT, true);
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(afterDataset, filter, CONFIG, CURRENT);

        LogicalPlan bare = bareIndexChild(result);
        assertThat(bare, instanceOf(Filter.class));
        assertThat(((Filter) bare).child(), sameInstance(bareDataset));
        assertThat(viewChild(result, "myView"), instanceOf(Filter.class));
        assertThat(result.collect(Filter.class::isInstance), hasSize(2));
    }

    /**
     * The exclusion is by node identity, not by dataset name. The same dataset read both inside a view body and in a
     * bare branch of the same query yields two distinct relation nodes that are {@code equals()} to each other; only
     * the one inside the view must be left to the view-output filter, while the bare one is still filtered at the leaf.
     */
    public void testSameDatasetInsideAndOutsideViewIsFilteredOnlyOutside() {
        ReferenceAttribute empNo = attr("emp_no", DataType.INTEGER);
        ReferenceAttribute doubled = attr("doubled", DataType.INTEGER);
        ExternalRelation insideView = viewSubplan("employees", empNo);
        ExternalRelation outsideView = viewSubplan("employees", empNo);
        assertThat(insideView, equalTo(outsideView));
        assertThat(insideView, not(sameInstance(outsideView)));
        LogicalPlan viewBody = new Eval(Source.EMPTY, insideView, List.of(new Alias(Source.EMPTY, "doubled", empNo, doubled.id())));
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put(null, outsideView);
        map.put("emp_eval_view", viewBody);
        BranchPlan vua = new BranchPlan(
            Source.EMPTY,
            map,
            Map.of("emp_eval_view", BranchIdentity.view("emp_eval_view")),
            List.of(empNo, doubled)
        );

        QueryBuilder filter = QueryBuilders.termQuery("emp_no", 1);
        LogicalPlan afterDataset = RequestFilterRewriter.rewrite(vua, filter, CONFIG, CURRENT, true);
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(afterDataset, filter, CONFIG, CURRENT);

        LogicalPlan bare = bareIndexChild(result);
        assertThat(bare, instanceOf(Filter.class));
        assertThat(((Filter) bare).child(), sameInstance(outsideView));
        LogicalPlan view = viewChild(result, "emp_eval_view");
        assertThat(view, instanceOf(Filter.class));
        assertThat(((Filter) view).child(), instanceOf(Eval.class));
        assertThat(((Eval) ((Filter) view).child()).child(), sameInstance(insideView));
        assertThat(result.collect(Filter.class::isInstance), hasSize(2));
    }

    /**
     * A view nested inside a non-view branch of an outer union (a user-written subquery) is a view the query names, so
     * its output is filtered too. The dataset read inside it is left to that filter by the dataset rewriter.
     */
    public void testViewNestedInLiteralSubqueryBranchIsFilteredAtItsOutput() {
        ReferenceAttribute y = attr("y", DataType.INTEGER);
        ExternalRelation innerDataset = viewSubplan("innerView", y);
        LinkedHashMap<String, LogicalPlan> innerMap = new LinkedHashMap<>();
        innerMap.put("innerView", innerDataset);
        BranchPlan inner = new BranchPlan(Source.EMPTY, innerMap, Map.of("innerView", BranchIdentity.view("innerView")), List.of(y));
        LogicalPlan subquery = new Filter(Source.EMPTY, inner, Literal.TRUE);
        LinkedHashMap<String, LogicalPlan> outerMap = new LinkedHashMap<>();
        outerMap.put("outerView", viewSubplan("outerView", y));
        outerMap.put("unnamed_view_1", subquery);
        BranchPlan outer = new BranchPlan(Source.EMPTY, outerMap, Map.of("outerView", BranchIdentity.view("outerView")), List.of(y));

        QueryBuilder filter = QueryBuilders.termQuery("y", 1);
        LogicalPlan afterDataset = RequestFilterRewriter.rewrite(outer, filter, CONFIG, CURRENT, true);
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(afterDataset, filter, CONFIG, CURRENT);

        assertThat(viewChild(result, "outerView"), instanceOf(Filter.class));
        LogicalPlan subqueryBranch = viewChild(result, "unnamed_view_1");
        // The subquery branch itself is not a view branch, so it is not wrapped; its nested view is.
        assertThat(subqueryBranch, instanceOf(Filter.class));
        assertThat(((Filter) subqueryBranch).condition(), sameInstance(Literal.TRUE));
        LogicalPlan innerChild = viewChild(((Filter) subqueryBranch).child(), "innerView");
        assertThat(innerChild, instanceOf(Filter.class));
        assertThat(((Filter) innerChild).child(), sameInstance(innerDataset));
        assertThat(result.collect(Filter.class::isInstance), hasSize(3));
    }

    /** A view nested inside another view's body is an implementation detail of the outer view and is not filtered again. */
    public void testViewNestedInViewBranchIsNotFilteredAgain() {
        ReferenceAttribute y = attr("y", DataType.INTEGER);
        ExternalRelation innerDataset = viewSubplan("innerView", y);
        LinkedHashMap<String, LogicalPlan> innerMap = new LinkedHashMap<>();
        innerMap.put("innerView", innerDataset);
        BranchPlan inner = new BranchPlan(Source.EMPTY, innerMap, Map.of("innerView", BranchIdentity.view("innerView")), List.of(y));
        LinkedHashMap<String, LogicalPlan> outerMap = new LinkedHashMap<>();
        outerMap.put("outerView", inner);
        BranchPlan outer = new BranchPlan(Source.EMPTY, outerMap, Map.of("outerView", BranchIdentity.view("outerView")), List.of(y));

        QueryBuilder filter = QueryBuilders.termQuery("y", 1);
        LogicalPlan afterDataset = RequestFilterRewriter.rewrite(outer, filter, CONFIG, CURRENT, true);
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(afterDataset, filter, CONFIG, CURRENT);

        LogicalPlan outerChild = viewChild(result, "outerView");
        assertThat(outerChild, instanceOf(Filter.class));
        assertThat(((Filter) outerChild).child(), sameInstance(inner));
        assertThat(result.collect(Filter.class::isInstance), hasSize(1));
    }

    // --- core behaviour ---

    /** A null request filter is a no-op: the plan is returned unchanged. */
    public void testNullFilterLeavesPlanUnchanged() {
        BranchPlan vua = unionWithView("myView", attr("y", DataType.INTEGER));
        assertSame(vua, ViewRequestFilterRewriter.rewrite(vua, null, CONFIG, CURRENT));
    }

    /**
     * A supported term filter is translated and installed as an ordinary {@link Filter} above the view subplan, bound
     * against the view's output schema (field {@code y} present → binds to its attribute).
     */
    public void testSupportedFilterIsInstalledAboveViewSubplan() {
        Attribute y = attr("y", DataType.INTEGER);
        BranchPlan vua = unionWithView("myView", y);
        LogicalPlan original = vua.namedSubqueries().get("myView");

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 42), CONFIG, CURRENT);

        LogicalPlan viewChild = viewChild(result, "myView");
        assertThat("filter is installed above the view subplan", viewChild, instanceOf(Filter.class));
        Filter filter = (Filter) viewChild;
        assertThat(filter.child(), sameInstance(original));
        // The term translates to the any-value equality mv_contains(y, 42), with y bound to the view's own output attribute —
        // not a fresh reference by name — so the filter resolves against exactly what the view produces.
        assertThat(filter.condition(), instanceOf(MvContains.class));
        MvContains condition = (MvContains) filter.condition();
        assertThat("field side is the view's output attribute", condition.children().get(0), sameInstance(y));
        assertThat(condition.children().get(1), equalTo(new Literal(Source.EMPTY, 42, DataType.INTEGER)));
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
        BranchPlan vua = new BranchPlan(
            Source.EMPTY,
            map,
            Map.of("myView", BranchIdentity.view("myView")),
            List.of(attr("y", DataType.INTEGER))
        );

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 42), CONFIG, CURRENT);
        assertThat(bareIndexChild(result), sameInstance(bare));
        assertThat(bareIndexChild(result), not(instanceOf(Filter.class)));
    }

    /**
     * A {@link BranchPlan} that contains only a bare-index child (no view subplans) is returned unchanged: there is
     * nothing to filter.
     */
    public void testPlanWithNoViewSubplansIsUnchanged() {
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put(null, bareIndex());
        BranchPlan vua = new BranchPlan(Source.EMPTY, map, Map.of(), List.of());

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 42), CONFIG, CURRENT);
        assertSame(vua, result);
    }

    /**
     * A plan containing no {@link BranchPlan} nodes at all (e.g. a plain index-only query) is returned unchanged.
     */
    public void testPlanWithNoBranchPlanIsUnchanged() {
        LogicalPlan indexOnly = bareIndex();
        assertSame(indexOnly, ViewRequestFilterRewriter.rewrite(indexOnly, QueryBuilders.termQuery("y", 42), CONFIG, CURRENT));
    }

    /**
     * A {@code match_all} filter translates to {@link org.elasticsearch.xpack.esql.core.expression.Literal#TRUE TRUE},
     * which is a supported no-op: the view is left unfiltered rather than wrapped in a trivially-true {@link Filter}.
     */
    public void testMatchAllFilterLeavesViewUnfilteredNoWrap() {
        BranchPlan vua = unionWithView("myView", attr("y", DataType.INTEGER));
        LogicalPlan original = vua.namedSubqueries().get("myView");

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.matchAllQuery(), CONFIG, CURRENT);
        // match_all → TRUE → view subplan not wrapped
        assertThat(viewChild(result, "myView"), sameInstance(original));
    }

    // --- multi-view ---

    /**
     * All view subplans in a single {@link BranchPlan} are wrapped independently, each bound against that subplan's
     * own output schema. A field present in one view but absent in another binds to {@code NULL} in the second.
     */
    public void testMultipleViewSubplansAreEachWrapped() {
        Attribute y = attr("y", DataType.INTEGER);
        Attribute z = attr("z", DataType.INTEGER);
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put("viewA", viewSubplan("viewA", y));
        map.put("viewB", viewSubplan("viewB", z));
        BranchPlan vua = new BranchPlan(
            Source.EMPTY,
            map,
            Map.of("viewA", BranchIdentity.view("viewA"), "viewB", BranchIdentity.view("viewB")),
            List.of(y, z)
        );

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 1), CONFIG, CURRENT);

        // viewA has field y → bound to the attribute → real filter
        assertThat(viewChild(result, "viewA"), instanceOf(Filter.class));
        // viewB does not have field y → bound to NULL → FALSE under term → still a Filter (non-trivial condition)
        assertThat(viewChild(result, "viewB"), instanceOf(Filter.class));
    }

    // --- unsupported constructs are dropped with a warning ---

    // ---- per-function version gating on the view path (elastic/elasticsearch#159672) ----
    //
    // CURRENT here is the rewrite's own pin, which is below esql_mv_compare, so it is the window the per-function
    // gate exists for: the view rewrite runs, and a single-bound range needs a function the targeted nodes lack.

    /**
     * The view path must reach the translator with the CLUSTER's minimum, not {@code current()}. If it passed
     * {@code current()} — as the neighbouring field-name extractor deliberately does — the gate would be silently off
     * here and the branch would ship a function an older node cannot read. Nothing else pins which of the two
     * spellings this path uses.
     */
    public void testViewBranchDropsAGatedFunctionBelowItsPin() {
        Attribute y = attr("y", DataType.KEYWORD);
        BranchPlan vua = unionWithView("myView", y);

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.rangeQuery("y").gt("m"), CONFIG, CURRENT);

        LogicalPlan viewChild = viewChild(result, "myView");
        assertThat("nothing gated is installed below the pin", viewChild, not(instanceOf(Filter.class)));
        assertWarnings(
            "The request filter could not be fully applied to view(s); the following were not applied, "
                + "[range[single lower bound on keyword]] on view [myView] because "
                + QueryDslTranslator.VERSION_REASON
                + ". Use a WHERE clause to filter rows from views instead"
        );
    }

    /** At or above the pin the same filter translates and installs, so the gate is the only thing stopping it. */
    public void testViewBranchInstallsTheSameFilterAtOrAboveThePin() {
        Attribute y = attr("y", DataType.KEYWORD);
        BranchPlan vua = unionWithView("myView", y);

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(
            vua,
            QueryBuilders.rangeQuery("y").gt("m"),
            CONFIG,
            TransportVersion.current()
        );

        assertThat(viewChild(result, "myView"), instanceOf(Filter.class));
    }

    private static final String SKIPPED_WILDCARD_WARNING =
        "The request filter could not be fully applied to view(s); the following Query DSL constructs are not supported and were "
            + "skipped: [wildcard] on view [myView]. Use a WHERE clause to filter rows from views instead";

    /**
     * An unsupported DSL construct does not fail the query, matching the dataset policy: it is dropped and a warning names
     * both the construct and the view. With nothing translatable left, the view is read unfiltered rather than wrapped in a
     * no-op Filter.
     */
    public void testUnsupportedDslConstructIsDroppedWithWarning() {
        BranchPlan vua = unionWithView("myView", attr("y", DataType.KEYWORD));
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.wildcardQuery("y", "x*"), CONFIG, CURRENT);
        assertThat("nothing translatable remains, so no Filter is installed", viewChild(result, "myView"), not(instanceOf(Filter.class)));
        assertWarnings(SKIPPED_WILDCARD_WARNING);
    }

    /**
     * A filter mixing a supported term with an unsupported wildcard keeps the term and drops the wildcard, so the installed
     * condition is exactly the term — looser than the request, never tighter — and the warning names only the dropped clause.
     */
    public void testMixedDslKeepsSupportedClausesAndWarnsAboutTheRest() {
        Attribute y = attr("y", DataType.INTEGER);
        BranchPlan vua = unionWithView("myView", y);
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(
            vua,
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("y", 1)).must(QueryBuilders.wildcardQuery("y", "x*")),
            CONFIG,
            CURRENT
        );
        assertThat(viewChild(result, "myView"), instanceOf(Filter.class));
        Filter filter = (Filter) viewChild(result, "myView");
        assertThat("only the term survives", filter.condition(), instanceOf(MvContains.class));
        MvContains condition = (MvContains) filter.condition();
        assertThat(condition.children().get(0), sameInstance(y));
        assertThat(condition.children().get(1), equalTo(new Literal(Source.EMPTY, 1, DataType.INTEGER)));
        assertWarnings(SKIPPED_WILDCARD_WARNING);
    }

    /** One warning for the whole plan: the same construct failing on two views is reported once per view, in plan order. */
    public void testUnsupportedClauseWarningNamesEveryAffectedViewOnce() {
        Attribute y = attr("y", DataType.KEYWORD);
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put("viewA", viewSubplan("viewA", y));
        map.put("viewB", viewSubplan("viewB", y));
        BranchPlan vua = new BranchPlan(
            Source.EMPTY,
            map,
            Map.of("viewA", BranchIdentity.view("viewA"), "viewB", BranchIdentity.view("viewB")),
            List.of(y)
        );

        QueryBuilder twoWildcards = QueryBuilders.boolQuery()
            .must(QueryBuilders.wildcardQuery("y", "x*"))
            .must(QueryBuilders.wildcardQuery("y", "z*"));
        ViewRequestFilterRewriter.rewrite(vua, twoWildcards, CONFIG, CURRENT);
        assertWarnings(
            "The request filter could not be fully applied to view(s); the following Query DSL constructs are not supported and were "
                + "skipped: [wildcard] on view [viewA]; [wildcard] on view [viewB]. Use a WHERE clause to filter rows from views instead"
        );
    }

    // --- version gate ---

    /**
     * The critical version gate: below {@link RequestFilterRewriter#ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is
     * skipped entirely so that no plan an old node cannot deserialize is shipped. The filter is not lost — the planner then
     * pushes it into the view's source scan instead (see {@code ViewBranchFilterPushdownTests}) — but that path is wrong for
     * computed fields, hence the warning.
     */
    public void testOldMinimumVersionSkipsRewriteAndWarns() {
        BranchPlan vua = unionWithView("myView", attr("y", DataType.INTEGER));

        LogicalPlan result = ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 1), CONFIG, TOO_OLD);
        assertSame(vua, result);
        assertWarnings(
            "The request filter was applied to the source indices of view(s) [myView] rather than to their output because the "
                + "cluster contains a node too old to evaluate the translated filter; a filter on a field a view computes "
                + "or renames may therefore be wrong. Use a WHERE clause to filter rows from views instead"
        );
    }

    /** A version-gate skip with no views in the plan is silent. */
    public void testOldVersionWithNoViewsIsQuiet() {
        LogicalPlan indexOnly = bareIndex();
        LogicalPlan result = ViewRequestFilterRewriter.rewrite(indexOnly, QueryBuilders.termQuery("y", 1), CONFIG, TOO_OLD);
        assertSame(indexOnly, result);
    }

    /** The version-gate warning names all distinct views in the plan once. */
    public void testVersionGateWarningNamesAllViewsOnce() {
        Attribute y = attr("y", DataType.INTEGER);
        LinkedHashMap<String, LogicalPlan> map = new LinkedHashMap<>();
        map.put("viewA", viewSubplan("viewA", y));
        map.put("viewB", viewSubplan("viewB", y));
        BranchPlan vua = new BranchPlan(
            Source.EMPTY,
            map,
            Map.of("viewA", BranchIdentity.view("viewA"), "viewB", BranchIdentity.view("viewB")),
            List.of(y)
        );

        ViewRequestFilterRewriter.rewrite(vua, QueryBuilders.termQuery("y", 1), CONFIG, TOO_OLD);
        assertWarnings(
            "The request filter was applied to the source indices of view(s) [viewA, viewB] rather than to their output because the "
                + "cluster contains a node too old to evaluate the translated filter; a filter on a field a view computes "
                + "or renames may therefore be wrong. Use a WHERE clause to filter rows from views instead"
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
        for (Map.Entry<String, QueryBuilder> filter : realFilters().entrySet()) {
            assertTrue(filter.getKey(), ViewRequestFilterRewriter.appliesToViewOutputs(filter.getValue()));
        }
    }

    /** Filters that can exclude a document, keyed by a description for assertion messages. */
    private static Map<String, QueryBuilder> realFilters() {
        Map<String, QueryBuilder> filters = new LinkedHashMap<>();
        filters.put("term", QueryBuilders.termQuery("region", "eu"));
        filters.put("range", QueryBuilders.rangeQuery("cnt").gt(0));
        filters.put("bool with a real must", QueryBuilders.boolQuery().must(QueryBuilders.termQuery("region", "eu")));
        // should / must_not count as filtering even in the shapes the translator would discard: narrower is the safe direction.
        filters.put("bool with only a should", QueryBuilders.boolQuery().should(QueryBuilders.termQuery("region", "eu")));
        filters.put("bool with only a must_not", QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("region", "eu")));
        // An unsupported construct must not be mistaken for a no-op — it has to reach the translation so it can be reported.
        filters.put("unsupported wildcard", QueryBuilders.wildcardQuery("region", "e*"));
        return filters;
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
