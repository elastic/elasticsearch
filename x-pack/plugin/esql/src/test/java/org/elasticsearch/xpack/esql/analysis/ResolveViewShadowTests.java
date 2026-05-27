/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * Tests for the {@code Analyzer.ResolveViewShadows} analyzer rule. Each test builds a small plan
 * tree by hand (since {@link ViewShadowRelation} has no surface syntax) and runs the analyzer
 * with mocked {@link AnalyzerContext#optionalLinkedResolution()} maps to verify the rule's behaviour:
 * <ul>
 *   <li>shadow with a valid lenient resolution → replaced with {@code EsRelation}, and the
 *       shadow's clusters are excluded from the sibling view-body branch;</li>
 *   <li>shadow with no lenient entry (or an invalid resolution) → dropped from the
 *       {@link ViewUnionAll} by {@code ResolveViewShadows};</li>
 *   <li>strict + matched shadow at the same level → both kept as siblings (Strategy A — no
 *       merging into a single combined {@code EsRelation});</li>
 *   <li>two shadows with the same view name but different exclusions resolve independently —
 *       the lookup key is the full {@link ViewShadowRelation#optionalLinkedPattern()}, so the same view
 *       name with one exclusion list can match a remote index while another exclusion list at
 *       the same view name returns nothing.</li>
 * </ul>
 * The actual lenient field-caps integration that populates the resolution map in production is
 * deferred to a follow-up PR; this PR ensures the analyzer-side plumbing is in place and tested
 * against a mocked input.
 * <p>
 * Each test calls {@link #assertWarnings(String...)} to acknowledge the
 * "No limit defined" warning that {@code AddImplicitLimit} adds since the test inputs are bare
 * relations.
 */
public class ResolveViewShadowTests extends ESTestCase {

    private static final Source EMPTY = Source.EMPTY;
    private static final String NO_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";

    /**
     * Shadow with a valid lenient {@link IndexResolution} → replaced with an {@link EsRelation}
     * over the resolved remote index's mapping. Because {@code ResolveViewShadows} operates on
     * {@link ViewUnionAll} nodes (matching production structure from {@link
     * org.elasticsearch.xpack.esql.view.ViewResolver}), the shadow is wrapped in a single-branch
     * {@code ViewUnionAll}; the rule collapses the lone survivor directly to the {@code EsRelation}.
     */
    public void testShadowResolvesToEsRelationWhenLenientMatches() {
        EsIndex remoteV1 = EsIndexGenerator.esIndex("v1", LoadMapping.loadMapping("mapping-one-field.json"));
        var analyzer = analyzer().addLenientShadow(remoteV1).buildAnalyzer();

        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("v1#shadow", new ViewShadowRelation(EMPTY, "v1", List.of()));
        LogicalPlan plan = analyzer.analyze(new ViewUnionAll(EMPTY, branches, List.of()));

        var limit = as(plan, Limit.class);
        var esRelation = as(unwrapProject(limit.child()), EsRelation.class);
        assertEquals("v1", esRelation.indexPattern());
        // The mapping has emp_no — confirms the shadow's EsRelation carries the remote index's fields.
        assertTrue(
            "expected emp_no field in the resolved EsRelation, got: " + esRelation.output(),
            esRelation.output().stream().map(Attribute::name).anyMatch("emp_no"::equals)
        );
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Shadow with no lenient match → dropped by {@code ResolveViewShadows}. With a strict sibling
     * in a {@link ViewUnionAll}, the rule removes the unresolved shadow; the surviving sibling is
     * the strict {@link EsRelation}.
     */
    public void testShadowStrippedWhenNoLenientMatch() {
        EsIndex strictIdx = EsIndexGenerator.esIndex("strict_idx", LoadMapping.loadMapping("mapping-one-field.json"));
        var analyzer = analyzer().addIndex(strictIdx).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(
            viewUnionAllOf("strict_idx", strictUR("strict_idx"), new ViewShadowRelation(EMPTY, "v1", List.of()))
        );

        var limit = as(plan, Limit.class);
        // The ViewUnionAll has collapsed (one shadow stripped, leaving a single strict child).
        var esRelation = as(unwrapProject(limit.child()), EsRelation.class);
        assertEquals("strict_idx", esRelation.indexPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Same setup as {@link #testShadowStrippedWhenNoLenientMatch} but with an explicit
     * invalid lenient entry under the shadow's pattern — should also be stripped (the rule
     * treats any non-valid resolution as "no match").
     */
    public void testShadowStrippedWhenLenientResolutionIsInvalid() {
        ViewShadowRelation shadow = new ViewShadowRelation(EMPTY, "v1", List.of());
        EsIndex strictIdx = EsIndexGenerator.esIndex("strict_idx", LoadMapping.loadMapping("mapping-one-field.json"));
        var analyzer = analyzer().addIndex(strictIdx)
            .addLenientShadow(shadow.optionalLinkedPattern(), IndexResolution.invalid("not found"))
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(viewUnionAllOf("strict_idx", strictUR("strict_idx"), shadow));

        var limit = as(plan, Limit.class);
        var esRelation = as(unwrapProject(limit.child()), EsRelation.class);
        assertEquals("strict_idx", esRelation.indexPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Strategy A: when both strict and shadow resolve, they live as separate {@link EsRelation}
     * siblings inside the {@link ViewUnionAll}. No merging into a single combined
     * {@code EsRelation}.
     */
    public void testShadowResolvesAlongsideStrictResolution() {
        EsIndex strictIdx = EsIndexGenerator.esIndex("strict_idx", LoadMapping.loadMapping("mapping-one-field.json"));
        EsIndex remoteV1 = EsIndexGenerator.esIndex("v1", LoadMapping.loadMapping("mapping-one-field.json"));
        var analyzer = analyzer().addIndex(strictIdx).addLenientShadow(remoteV1).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(
            viewUnionAllOf("strict_idx", strictUR("strict_idx"), new ViewShadowRelation(EMPTY, "v1", List.of()))
        );

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), ViewUnionAll.class);
        assertEquals("expected two children, got: " + unionAll, 2, unionAll.children().size());
        // Each child resolves to an EsRelation (possibly under analyzer-inserted Project wrappers
        // for output alignment across the UnionAll branches).
        var indexNames = unionAll.children().stream().map(c -> as(unwrapProject(c), EsRelation.class).indexPattern()).sorted().toList();
        assertEquals(List.of("strict_idx", "v1"), indexNames);
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Shadow with exclusions: the lookup key is the shadow's full
     * {@link ViewShadowRelation#optionalLinkedPattern()} — view name <em>plus</em> the comma-joined
     * exclusions. Confirms an entry registered under that exact pattern resolves the shadow.
     */
    public void testShadowLookupKeyIncludesExclusions() {
        ViewShadowRelation shadow = new ViewShadowRelation(EMPTY, "v1", List.of("-stale-*"));
        assertEquals("v1,-stale-*", shadow.optionalLinkedPattern().indexPattern());
        EsIndex remoteV1 = EsIndexGenerator.esIndex("v1", LoadMapping.loadMapping("mapping-one-field.json"));
        var analyzer = analyzer().addLenientShadow(shadow.optionalLinkedPattern(), IndexResolution.valid(remoteV1)).buildAnalyzer();

        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("v1#shadow", shadow);
        LogicalPlan plan = analyzer.analyze(new ViewUnionAll(EMPTY, branches, List.of()));

        var limit = as(plan, Limit.class);
        var esRelation = as(unwrapProject(limit.child()), EsRelation.class);
        assertEquals("v1", esRelation.indexPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Two shadows with the <em>same</em> view name but <em>different</em> exclusion lists are
     * looked up independently in {@link AnalyzerContext#optionalLinkedResolution()}: one combination
     * resolves to a remote index (becomes an {@link EsRelation}), the other has no entry
     * (stays unresolved → stripped). Reproduces the motivating scenario from
     * {@code FROM my-data,-my-data,my-data,-unrelated-exclusion}: at the first position the
     * shadow's pattern is {@code my-data,-my-data,-unrelated-exclusion} which the lenient
     * field-caps would empty out, while at the second position the pattern is
     * {@code my-data,-unrelated-exclusion} which can resolve to a remote index. Keying the
     * map by view name alone would conflate the two.
     */
    public void testShadowsWithSameViewNameDifferentExclusionsResolveIndependently() {
        ViewShadowRelation matchedShadow = new ViewShadowRelation(EMPTY, "my-data", List.of("-unrelated-*"));
        ViewShadowRelation emptyShadow = new ViewShadowRelation(EMPTY, "my-data", List.of("-my-data", "-unrelated-*"));
        EsIndex remoteMyData = EsIndexGenerator.esIndex("my-data", LoadMapping.loadMapping("mapping-one-field.json"));
        // Only the matched-shadow's pattern has a lenient entry; the empty-shadow's pattern is absent.
        var analyzer = analyzer().addLenientShadow(matchedShadow.optionalLinkedPattern(), IndexResolution.valid(remoteMyData))
            .addIndex(EsIndexGenerator.esIndex("strict_idx", LoadMapping.loadMapping("mapping-one-field.json")))
            .buildAnalyzer();

        // matchedShadow in a single-branch ViewUnionAll: resolves to EsRelation.
        LinkedHashMap<String, LogicalPlan> matchedBranches = new LinkedHashMap<>();
        matchedBranches.put("my-data#shadow", matchedShadow);
        LogicalPlan matchedPlan = analyzer.analyze(new ViewUnionAll(EMPTY, matchedBranches, List.of()));
        var matchedEs = as(unwrapProject(as(matchedPlan, Limit.class).child()), EsRelation.class);
        assertEquals("my-data", matchedEs.indexPattern());

        // emptyShadow paired with a strict UR: stripped, leaving just the strict EsRelation.
        LogicalPlan emptyPlan = analyzer.analyze(viewUnionAllOf("strict_idx", strictUR("strict_idx"), emptyShadow));
        var strictEs = as(unwrapProject(as(emptyPlan, Limit.class).child()), EsRelation.class);
        assertEquals("strict_idx", strictEs.indexPattern());

        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Regression: a {@link ViewUnionAll} with both a strict UR whose resolution is
     * {@code EMPTY_SUBQUERY} (e.g. CCS subquery that didn't match anything) <em>and</em> a
     * matched shadow used to trip the assertion in {@link ViewUnionAll#asSubqueryMap}. The
     * {@code PruneEmptyUnionAllBranch} analyzer rule called
     * {@code unionAll.replaceChildren(shorterList)} which mismatched the named-subqueries map.
     * The fix routes the prune through {@link UnionAll#pruneEmptyBranches} — overridden in
     * {@link ViewUnionAll} to filter the named map directly, preserving the surviving names.
     * <p>
     * Setup: a ViewUnionAll with strict UR "missing_idx" (resolution {@code EMPTY_SUBQUERY})
     * and a shadow for view "v1" (lenient resolution found a remote index named "v1"). After
     * analysis the EMPTY_SUBQUERY branch is pruned and the shadow resolves to an
     * {@code EsRelation}; the surviving wrapper is a single-child {@link ViewUnionAll} carrying
     * just the resolved {@code v1} branch under its preserved name. (The single-child wrapper
     * is preserved here — {@code ResolveViewShadows} only collapses when <em>all</em> surviving
     * entries are from dropped shadows; in this case EMPTY_SUBQUERY survives {@code ResolveViewShadows}
     * and is later pruned by {@code PruneEmptyUnionAllBranch}, leaving the wrapper intact.)
     */
    public void testPruneEmptySubqueryBranchPreservesShadowResolutionInViewUnionAll() {
        EsIndex remoteV1 = EsIndexGenerator.esIndex("v1", LoadMapping.loadMapping("mapping-one-field.json"));
        var analyzer = analyzer().addIndex("missing_idx", IndexResolution.EMPTY_SUBQUERY).addLenientShadow(remoteV1).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(
            viewUnionAllOf("missing_idx", strictUR("missing_idx"), new ViewShadowRelation(EMPTY, "v1", List.of()))
        );

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), ViewUnionAll.class);
        assertEquals("expected a single surviving child after pruning", 1, unionAll.children().size());
        var esRelation = as(unwrapProject(unionAll.children().getFirst()), EsRelation.class);
        assertEquals("v1", esRelation.indexPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    // ── Cluster exclusion tests (ResolveViewShadows) ─────────────────────────────────────────────

    /**
     * When a shadow is resolved, its cluster aliases must be removed from every {@link EsRelation}
     * inside the sibling view-body branch. This is the core requirement from esql-planning#795:
     * a linked project that has a remote index with the same name as the local view must NOT also
     * run the view body.
     *
     * <p>In {@link org.elasticsearch.xpack.esql.view.ViewResolver} the view-body branch is always
     * keyed by the <em>view name</em> (e.g. {@code "v1"}), and the shadow branch is keyed
     * {@code "v1#shadow"}. The {@code ResolveViewShadows} rule strips the {@code "#shadow"}
     * suffix to find the matching view-body key.
     *
     * <p>Setup: ViewUnionAll with key {@code "v1"} → UR {@code "source_idx"} (the view body),
     * and key {@code "v1#shadow"} → shadow. {@code "source_idx"} resolves to an {@link EsRelation}
     * present on both the local cluster and {@code "remote1"}. The shadow resolves to an
     * {@link EsRelation} for {@code "v1"} present only on {@code "remote1"}.
     *
     * <p>Expected: the surviving ViewUnionAll has two branches, and the view-body's
     * {@code EsRelation("source_idx")} no longer includes {@code "remote1"}.
     */
    public void testShadowExcludesClustersFromViewBody() {
        var mapping = LoadMapping.loadMapping("mapping-one-field.json");
        EsIndex sourceIdx = new EsIndex(
            "source_idx",
            mapping,
            Map.of("source_idx", IndexMode.STANDARD, "remote1:source_idx", IndexMode.STANDARD),
            Map.of("", List.of("source_idx"), "remote1", List.of("source_idx")),
            Map.of("", List.of("source_idx"), "remote1", List.of("source_idx"))
        );
        EsIndex shadowIdx = new EsIndex(
            "v1",
            mapping,
            Map.of("remote1:v1", IndexMode.STANDARD),
            Map.of("remote1", List.of("v1")),
            Map.of("remote1", List.of("v1"))
        );
        var analyzer = analyzer().addIndex(sourceIdx).addLenientShadow(shadowIdx).buildAnalyzer();

        // Key "v1" = view body; "v1#shadow" = shadow — matching the ViewResolver convention.
        LogicalPlan plan = analyzer.analyze(viewUnionAllOf("v1", strictUR("source_idx"), new ViewShadowRelation(EMPTY, "v1", List.of())));

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), ViewUnionAll.class);
        assertEquals("expected two children (view body + shadow), got: " + unionAll, 2, unionAll.children().size());

        EsRelation viewBodyRel = null;
        EsRelation shadowRel = null;
        for (LogicalPlan child : unionAll.children()) {
            EsRelation rel = as(unwrapProject(child), EsRelation.class);
            if (rel.indexPattern().equals("source_idx")) {
                viewBodyRel = rel;
            } else if (rel.indexPattern().equals("v1")) {
                shadowRel = rel;
            }
        }
        assertNotNull("expected a view-body EsRelation for source_idx", viewBodyRel);
        assertNotNull("expected a shadow EsRelation for v1", shadowRel);

        // The shadow covers remote1 — verify it is excluded from the view body.
        assertFalse(
            "remote1 should be excluded from view-body EsRelation because the shadow covers it",
            viewBodyRel.concreteIndices().containsKey("remote1")
        );
        assertFalse("remote1 should be excluded from view-body originalIndices", viewBodyRel.originalIndices().containsKey("remote1"));
        assertFalse(
            "remote1:source_idx should be excluded from view-body indexNameWithModes",
            viewBodyRel.indexNameWithModes().containsKey("remote1:source_idx")
        );
        // Local cluster data is untouched.
        assertTrue("local cluster should remain in view-body EsRelation", viewBodyRel.concreteIndices().containsKey(""));

        // The shadow EsRelation itself is unchanged.
        assertTrue("shadow should still have remote1", shadowRel.concreteIndices().containsKey("remote1"));

        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * When the shadow is <em>not</em> resolved (no lenient match → stripped by
     * {@code ViewCompactionPostIndexResolution}), the view-body {@link EsRelation} must be
     * left unchanged. There is nothing to exclude.
     */
    public void testNoClustersExcludedWhenShadowNotResolved() {
        var mapping = LoadMapping.loadMapping("mapping-one-field.json");
        EsIndex sourceIdx = new EsIndex(
            "source_idx",
            mapping,
            Map.of("source_idx", IndexMode.STANDARD, "remote1:source_idx", IndexMode.STANDARD),
            Map.of("", List.of("source_idx"), "remote1", List.of("source_idx")),
            Map.of("", List.of("source_idx"), "remote1", List.of("source_idx"))
        );
        // No lenient shadow registered → shadow stays unresolved → stripped.
        var analyzer = analyzer().addIndex(sourceIdx).buildAnalyzer();

        // Key "v1" = view body; "v1#shadow" = shadow — matching the ViewResolver convention.
        LogicalPlan plan = analyzer.analyze(viewUnionAllOf("v1", strictUR("source_idx"), new ViewShadowRelation(EMPTY, "v1", List.of())));

        var limit = as(plan, Limit.class);
        // Shadow stripped → single survivor, ViewUnionAll collapses.
        EsRelation viewBodyRel = as(unwrapProject(limit.child()), EsRelation.class);
        assertEquals("source_idx", viewBodyRel.indexPattern());

        // remote1 must still be present — no shadow was resolved so nothing was excluded.
        assertTrue(
            "remote1 should remain in view-body EsRelation when shadow is not resolved",
            viewBodyRel.concreteIndices().containsKey("remote1")
        );
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * A shadow for cluster {@code "remote1"} must not accidentally exclude data from cluster
     * {@code "remote2"}. Only the exact cluster aliases from the resolved shadow's
     * {@link EsRelation#concreteIndices()} are removed.
     */
    public void testShadowExcludesOnlyItsClusters() {
        var mapping = LoadMapping.loadMapping("mapping-one-field.json");
        EsIndex sourceIdx = new EsIndex(
            "source_idx",
            mapping,
            Map.of("source_idx", IndexMode.STANDARD, "remote1:source_idx", IndexMode.STANDARD, "remote2:source_idx", IndexMode.STANDARD),
            Map.of("", List.of("source_idx"), "remote1", List.of("source_idx"), "remote2", List.of("source_idx")),
            Map.of("", List.of("source_idx"), "remote1", List.of("source_idx"), "remote2", List.of("source_idx"))
        );
        // Shadow only covers remote1.
        EsIndex shadowIdx = new EsIndex(
            "v1",
            mapping,
            Map.of("remote1:v1", IndexMode.STANDARD),
            Map.of("remote1", List.of("v1")),
            Map.of("remote1", List.of("v1"))
        );
        var analyzer = analyzer().addIndex(sourceIdx).addLenientShadow(shadowIdx).buildAnalyzer();

        // Key "v1" = view body; "v1#shadow" = shadow — matching the ViewResolver convention.
        LogicalPlan plan = analyzer.analyze(viewUnionAllOf("v1", strictUR("source_idx"), new ViewShadowRelation(EMPTY, "v1", List.of())));

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), ViewUnionAll.class);
        EsRelation viewBodyRel = unionAll.children()
            .stream()
            .map(c -> as(unwrapProject(c), EsRelation.class))
            .filter(r -> r.indexPattern().equals("source_idx"))
            .findFirst()
            .orElseThrow();

        // remote1 excluded (covered by shadow), remote2 and local untouched.
        assertFalse("remote1 should be excluded", viewBodyRel.concreteIndices().containsKey("remote1"));
        assertTrue("remote2 should remain", viewBodyRel.concreteIndices().containsKey("remote2"));
        assertTrue("local cluster should remain", viewBodyRel.concreteIndices().containsKey(""));
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Two view branches (views "a" and "b") share the same ViewUnionAll. Only view "a" has a
     * matching shadow on {@code "clusterx"}. After analysis, branch "a" must have {@code "clusterx"}
     * removed from its {@link EsRelation} leaves, but branch "b" must be completely unchanged —
     * there is no shadow claiming its clusters.
     *
     * <p>This is the canonical multi-view case from esql-planning#795: the {@code ResolveViewShadows}
     * rule must pair each shadow with <em>its own</em> view branch rather than broadcasting the
     * exclusion across all non-shadow branches.
     */
    public void testShadowExcludesOnlyMatchingViewBranch() {
        var mapping = LoadMapping.loadMapping("mapping-one-field.json");
        // Both source indices exist on local + clusterx.
        EsIndex sourceA = new EsIndex(
            "source_a",
            mapping,
            Map.of("source_a", IndexMode.STANDARD, "clusterx:source_a", IndexMode.STANDARD),
            Map.of("", List.of("source_a"), "clusterx", List.of("source_a")),
            Map.of("", List.of("source_a"), "clusterx", List.of("source_a"))
        );
        EsIndex sourceB = new EsIndex(
            "source_b",
            mapping,
            Map.of("source_b", IndexMode.STANDARD, "clusterx:source_b", IndexMode.STANDARD),
            Map.of("", List.of("source_b"), "clusterx", List.of("source_b")),
            Map.of("", List.of("source_b"), "clusterx", List.of("source_b"))
        );
        // Only view "a" has a remote index on clusterx; "b" has no shadow.
        EsIndex shadowA = new EsIndex(
            "a",
            mapping,
            Map.of("clusterx:a", IndexMode.STANDARD),
            Map.of("clusterx", List.of("a")),
            Map.of("clusterx", List.of("a"))
        );

        var analyzer = analyzer().addIndex(sourceA).addIndex(sourceB).addLenientShadow(shadowA).buildAnalyzer();

        // ViewUnionAll: branch "a" → source_a, branch "b" → source_b, shadow "a#shadow".
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("a", strictUR("source_a"));
        branches.put("b", strictUR("source_b"));
        branches.put("a#shadow", new ViewShadowRelation(EMPTY, "a", List.of()));
        ViewUnionAll vua = new ViewUnionAll(EMPTY, branches, List.of());

        LogicalPlan plan = analyzer.analyze(vua);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), ViewUnionAll.class);
        assertEquals("expected three branches after analysis, got: " + unionAll, 3, unionAll.children().size());

        EsRelation relA = null;
        EsRelation relB = null;
        for (LogicalPlan child : unionAll.children()) {
            EsRelation rel = as(unwrapProject(child), EsRelation.class);
            if (rel.indexPattern().equals("source_a")) {
                relA = rel;
            } else if (rel.indexPattern().equals("source_b")) {
                relB = rel;
            }
            // "a" shadow EsRelation is also present but not checked here.
        }
        assertNotNull("expected EsRelation for source_a", relA);
        assertNotNull("expected EsRelation for source_b", relB);

        // Branch "a": clusterx must be excluded (shadow covers it).
        assertFalse("clusterx should be excluded from view-a's EsRelation", relA.concreteIndices().containsKey("clusterx"));
        assertTrue("local cluster should remain in view-a's EsRelation", relA.concreteIndices().containsKey(""));

        // Branch "b": clusterx must NOT be excluded (no shadow for view "b").
        assertTrue("clusterx should remain in view-b's EsRelation — no shadow claimed it", relB.concreteIndices().containsKey("clusterx"));
        assertTrue("local cluster should remain in view-b's EsRelation", relB.concreteIndices().containsKey(""));

        assertWarnings(NO_LIMIT_WARNING);
    }

    private static UnresolvedRelation strictUR(String pattern) {
        return new UnresolvedRelation(EMPTY, new IndexPattern(EMPTY, pattern), false, List.of(), IndexMode.STANDARD, null, "FROM");
    }

    /**
     * Builds a two-branch {@link ViewUnionAll} that mirrors the structure produced by
     * {@link org.elasticsearch.xpack.esql.view.ViewResolver}: the {@code strictName} key is the
     * <em>view name</em> (so it matches {@code shadow.viewName()}), and the shadow is keyed
     * {@code shadow.viewName() + "#shadow"}. Tests that want the {@code ResolveViewShadows}
     * rule to fire must pass the view name as {@code strictName}, not the source-index name.
     */
    private static ViewUnionAll viewUnionAllOf(String strictName, LogicalPlan strict, ViewShadowRelation shadow) {
        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put(strictName, strict);
        children.put(shadow.viewName() + "#shadow", shadow);
        return new ViewUnionAll(EMPTY, children, List.of());
    }

    /** Walks past one analyzer-inserted {@link Project} wrapper to expose the underlying relation. */
    private static LogicalPlan unwrapProject(LogicalPlan plan) {
        return plan instanceof Project p ? p.child() : plan;
    }
}
