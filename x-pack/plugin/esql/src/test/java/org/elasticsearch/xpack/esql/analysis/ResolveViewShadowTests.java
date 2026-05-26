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
import org.elasticsearch.xpack.esql.core.type.EsField;
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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * Tests for the {@code Analyzer.ResolveViewShadow} analyzer rule. Each test builds a small plan
 * tree by hand (since {@link ViewShadowRelation} has no surface syntax) and runs the analyzer
 * with mocked {@link AnalyzerContext#optionalLinkedResolution()} maps to verify the rule's behaviour:
 * <ul>
 *   <li>shadow with a valid lenient resolution → replaced with {@code EsRelation};</li>
 *   <li>shadow with no lenient entry (or an invalid resolution) → left unresolved, then stripped
 *       by {@code ViewCompactionPostIndexResolution};</li>
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
     * over the resolved remote index's mapping.
     */
    public void testShadowResolvesToEsRelationWhenLenientMatches() {
        EsIndex remoteV1 = EsIndexGenerator.esIndex("v1", LoadMapping.loadMapping("mapping-one-field.json"));
        var analyzer = analyzer().addLenientShadow(remoteV1).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(new ViewShadowRelation(EMPTY, "v1", List.of()));

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
     * Shadow with no lenient match → falls through {@code ResolveViewShadow} unchanged. With a
     * strict sibling in a {@link ViewUnionAll}, {@code ViewCompactionPostIndexResolution}'s strip
     * removes the unresolved shadow; the surviving sibling is the strict {@link EsRelation}.
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

        LogicalPlan plan = analyzer.analyze(shadow);

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

        // matchedShadow alone: resolves to EsRelation.
        LogicalPlan matchedPlan = analyzer.analyze(matchedShadow);
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
     * is preserved here — only {@code ViewCompaction.stripViewShadowRelations} collapses.)
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

    /**
     * elastic/esql-planning#795: when a linked project owns an index with the same name as a local
     * view, the view body must not <em>also</em> run on that project — the project's index supplies
     * its data instead (a name cannot be both a view and an index on one project; the index wins).
     * <p>
     * Setup mirrors {@code FROM v1} where {@code v1} is a local view {@code FROM source-idx}: the
     * strict body resolved flat-world across origin ({@code ""}) and linked project {@code "P"}
     * (both have {@code source-idx}), and the shadow shows {@code "P"} also owns an index named
     * {@code v1}. After analysis, {@code "P"} must be gone from the body's per-cluster maps (so the
     * view stops running there) while the shadow's {@code EsRelation} for {@code "P"} survives.
     * <p>
     * Fails before the {@code ExcludeShadowedProjectsFromViewBody} rule: the body retains {@code "P"},
     * double-counting that project (view body on P + P's index).
     */
    public void testViewBodyExcludesProjectOwningSameNamedIndex() {
        // Strict view body "v1": source-idx resolved on origin ("") and linked project "P".
        EsIndex body = new EsIndex(
            "source-idx",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("source-idx", IndexMode.STANDARD, "P:source-idx", IndexMode.STANDARD),
            Map.of("", List.of("source-idx"), "P", List.of("source-idx")),
            Map.of("", List.of("source-idx"), "P", List.of("source-idx"))
        );
        // Shadow: linked project "P" owns an index named "v1" (the view's name).
        EsIndex remoteV1 = new EsIndex(
            "v1",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("P:v1", IndexMode.STANDARD),
            Map.of("P", List.of("v1")),
            Map.of("P", List.of("v1"))
        );

        var analyzer = analyzer().addIndex("source-idx", IndexResolution.valid(body))
            .addLenientShadow(new IndexPattern(EMPTY, "v1"), IndexResolution.valid(remoteV1))
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(viewUnionAllOf("v1", strictUR("source-idx"), new ViewShadowRelation(EMPTY, "v1", List.of())));

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), ViewUnionAll.class);
        assertEquals("expected pruned body + shadow index branches", 2, unionAll.children().size());

        Map<String, EsRelation> byPattern = unionAll.children()
            .stream()
            .map(child -> as(unwrapProject(child), EsRelation.class))
            .collect(Collectors.toMap(EsRelation::indexPattern, r -> r));

        EsRelation bodyRelation = byPattern.get("source-idx");
        EsRelation shadowRelation = byPattern.get("v1");
        assertNotNull("view body branch missing", bodyRelation);
        assertNotNull("shadow remote-index branch missing", shadowRelation);

        // The view body must no longer run on "P" (the project owning a same-named index)...
        assertEquals(
            "view body must not run on the project owning a same-named index",
            Set.of(""),
            bodyRelation.concreteIndices().keySet()
        );
        assertEquals(Set.of("source-idx"), Set.copyOf(bodyRelation.concreteIndices().get("")));
        assertFalse("body's qualified index map still references P", bodyRelation.indexNameWithModes().containsKey("P:source-idx"));
        // ...but "P" still contributes via its own index "v1" through the surviving shadow branch.
        assertEquals(Set.of("P"), shadowRelation.concreteIndices().keySet());

        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * #795 edge: the colliding project is the view body's ONLY project. The view's source data lived
     * only on the project that turns out to own a same-named index, so after excluding that project
     * the body has no clusters left to run on — it contributes nothing, and the project's index (the
     * shadow) supplies all of its rows. The body branch survives with empty per-cluster maps (a
     * zero-row leaf); confirming execution treats that as no rows is left to the integration test.
     */
    public void testViewBodyEmptyWhenOnlyProjectOwnsSameNamedIndex() {
        EsIndex body = new EsIndex(
            "source-idx",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("P:source-idx", IndexMode.STANDARD),
            Map.of("P", List.of("source-idx")),
            Map.of("P", List.of("source-idx"))
        );
        EsIndex remoteV1 = new EsIndex(
            "v1",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("P:v1", IndexMode.STANDARD),
            Map.of("P", List.of("v1")),
            Map.of("P", List.of("v1"))
        );
        var analyzer = analyzer().addIndex("source-idx", IndexResolution.valid(body))
            .addLenientShadow(new IndexPattern(EMPTY, "v1"), IndexResolution.valid(remoteV1))
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(viewUnionAllOf("v1", strictUR("source-idx"), new ViewShadowRelation(EMPTY, "v1", List.of())));

        var unionAll = as(as(plan, Limit.class).child(), ViewUnionAll.class);
        Map<String, EsRelation> byPattern = unionAll.children()
            .stream()
            .map(child -> as(unwrapProject(child), EsRelation.class))
            .collect(Collectors.toMap(EsRelation::indexPattern, r -> r));

        // The body runs nowhere (no clusters left after excluding the only project that has the data)...
        assertEquals("view body should have no clusters left", Set.of(), byPattern.get("source-idx").concreteIndices().keySet());
        // ...and the project's same-named index supplies the rows via the shadow branch.
        assertEquals(Set.of("P"), byPattern.get("v1").concreteIndices().keySet());

        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * #795, nested case: outer view {@code v} is defined as {@code FROM v2}, an inner view over
     * {@code source-idx}. A single linked project {@code P} owns indexes named like <em>both</em>
     * {@code v} and {@code v2}. Because {@code v} resolves to an index on {@code P}, none of {@code v}'s
     * definition runs on {@code P} — not the {@code source-idx} body, and not {@code P}'s {@code v2}
     * index either; {@code P} contributes only its {@code v} index. The rule achieves this bottom-up:
     * the inner shadow is resolved to an {@code EsRelation} before the outer collision prunes the inner
     * subtree, so the inner {@code v2} index on {@code P} is pruned away too.
     */
    public void testNestedViewExcludesProjectOwningOuterViewIndex() {
        Map<String, EsField> mapping = LoadMapping.loadMapping("mapping-one-field.json");
        // v2 body: source-idx on origin ("") and linked project "P".
        EsIndex sourceIdx = new EsIndex(
            "source-idx",
            mapping,
            Map.of("source-idx", IndexMode.STANDARD, "P:source-idx", IndexMode.STANDARD),
            Map.of("", List.of("source-idx"), "P", List.of("source-idx")),
            Map.of("", List.of("source-idx"), "P", List.of("source-idx"))
        );
        EsIndex remoteV2 = new EsIndex(
            "v2",
            mapping,
            Map.of("P:v2", IndexMode.STANDARD),
            Map.of("P", List.of("v2")),
            Map.of("P", List.of("v2"))
        );
        EsIndex remoteV = new EsIndex(
            "v",
            mapping,
            Map.of("P:v", IndexMode.STANDARD),
            Map.of("P", List.of("v")),
            Map.of("P", List.of("v"))
        );

        var analyzer = analyzer().addIndex("source-idx", IndexResolution.valid(sourceIdx))
            .addLenientShadow(new IndexPattern(EMPTY, "v2"), IndexResolution.valid(remoteV2))
            .addLenientShadow(new IndexPattern(EMPTY, "v"), IndexResolution.valid(remoteV))
            .buildAnalyzer();

        // inner ViewUnionAll: v2 = FROM source-idx, plus v2's shadow
        LinkedHashMap<String, LogicalPlan> inner = new LinkedHashMap<>();
        inner.put("v2", strictUR("source-idx"));
        inner.put("v2" + ViewShadowRelation.NAME_SUFFIX, new ViewShadowRelation(EMPTY, "v2", List.of()));
        // outer ViewUnionAll: v = FROM v2, plus v's shadow
        LinkedHashMap<String, LogicalPlan> outer = new LinkedHashMap<>();
        outer.put("v", new ViewUnionAll(EMPTY, inner, List.of()));
        outer.put("v" + ViewShadowRelation.NAME_SUFFIX, new ViewShadowRelation(EMPTY, "v", List.of()));

        LogicalPlan plan = analyzer.analyze(new ViewUnionAll(EMPTY, outer, List.of()));

        Map<String, EsRelation> byPattern = new HashMap<>();
        plan.forEachDown(EsRelation.class, r -> byPattern.put(r.indexPattern(), r));

        // The view body runs on origin only — "P" is excluded since "v" is an index there.
        assertEquals("body must drop P", Set.of(""), byPattern.get("source-idx").concreteIndices().keySet());
        // The inner v2 index on "P" is also excluded — the whole nested definition of v is off on "P".
        assertEquals("inner v2 index on P must be excluded", Set.of(), byPattern.get("v2").concreteIndices().keySet());
        // "P" contributes only via its own "v" index.
        assertEquals(Set.of("P"), byPattern.get("v").concreteIndices().keySet());

        assertWarnings(NO_LIMIT_WARNING);
    }

    private static UnresolvedRelation strictUR(String pattern) {
        return new UnresolvedRelation(EMPTY, new IndexPattern(EMPTY, pattern), false, List.of(), IndexMode.STANDARD, null, "FROM");
    }

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
