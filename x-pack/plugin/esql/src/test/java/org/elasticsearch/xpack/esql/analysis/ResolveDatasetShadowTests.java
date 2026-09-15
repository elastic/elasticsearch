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
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.DatasetShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Tests for the {@code Analyzer.ResolveDatasetShadow} analyzer rule and the
 * {@code Analyzer.StripDatasetShadowRelations} strip step. The dataset analog of
 * {@code ResolveViewShadowTests}.
 * <p>
 * Each test builds a small plan tree by hand (since {@link DatasetShadowRelation} has no surface
 * syntax — {@code DatasetRewriter} emits it under CPS) and runs the analyzer with mocked
 * {@link AnalyzerContext#linkedResolution()} maps to verify:
 * <ul>
 *   <li>a shadow with a valid lenient resolution that matched at least one index → replaced with
 *       {@code EsRelation}; a valid-but-empty resolution counts as no match;</li>
 *   <li>a shadow with no lenient entry → left unresolved, then stripped, leaving the dataset's
 *       {@code ExternalRelation};</li>
 *   <li>a matched shadow living alongside the dataset's external relation → both kept as siblings
 *       (Strategy A — no merging);</li>
 *   <li>the lookup key is the shadow's full {@link DatasetShadowRelation#linkedIndexPattern()}
 *       (dataset name + exclusions), so the same dataset name with different exclusion lists resolves
 *       independently.</li>
 * </ul>
 * The lenient field-caps integration that populates the resolution map in production
 * ({@code EsqlSession.preAnalyzeLinkedIndices}) is shared with view shadows and exercised elsewhere;
 * the "remote dataset/view of the same name FAILS" leg is the detection rail
 * ({@code EsqlResolveFieldsAction} / {@code EsqlCCSUtils.checkForRemoteResourceErrors}) covered by
 * {@code EsqlCCSUtilsTests} — it fires at field-caps time, before this analyzer rule runs, so it is not
 * reachable through the analyzer-only path these tests drive.
 * <p>
 * Each test calls {@link #assertWarnings(String...)} to acknowledge the "No limit defined" warning that
 * {@code AddImplicitLimit} adds since the test inputs are bare relations.
 */
public class ResolveDatasetShadowTests extends ESTestCase {

    private static final Source EMPTY = Source.EMPTY;
    private static final String NO_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";
    private static final String DATASET_PATH = "s3://bucket/ds/*.parquet";

    /**
     * Shadow with a valid lenient {@link IndexResolution} → replaced with an {@link EsRelation} over the
     * resolved linked index's mapping.
     */
    public void testShadowResolvesToEsRelationWhenLenientMatches() {
        EsIndex remoteDs = EsIndexGenerator.esIndex(
            "ds",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("ds", IndexMode.STANDARD)
        );
        var analyzer = analyzer().addLenientResolution(remoteDs).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds"));

        var limit = as(plan, Limit.class);
        var esRelation = as(unwrapProject(limit.child()), EsRelation.class);
        assertEquals("ds", esRelation.indexPattern());
        // The mapping has emp_no — confirms the shadow's EsRelation carries the linked index's fields.
        assertTrue(
            "expected emp_no field in the resolved EsRelation, got: " + esRelation.output(),
            esRelation.output().stream().map(Attribute::name).anyMatch("emp_no"::equals)
        );
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Shadow with no lenient match → falls through {@code ResolveDatasetShadow} unchanged. With the
     * dataset's external relation as a sibling in a plain {@link UnionAll},
     * {@code StripDatasetShadowRelations} removes the unresolved shadow and the single-survivor union
     * collapses, leaving exactly the dataset's {@link ExternalRelation}.
     */
    public void testShadowStrippedWhenNoLenientMatch() {
        var analyzer = datasetExternalAnalyzer().buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(
            unionOf(datasetExternal("ds"), new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds"))
        );

        var limit = as(plan, Limit.class);
        // The UnionAll has collapsed (shadow stripped, leaving the single external child).
        var externalRelation = as(unwrapProject(limit.child()), ExternalRelation.class);
        assertEquals(DATASET_PATH, externalRelation.fileList().originalPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Same setup as {@link #testShadowStrippedWhenNoLenientMatch} but with an explicit invalid lenient
     * entry under the shadow's pattern — should also be stripped (the rule treats any non-valid
     * resolution as "no match").
     */
    public void testShadowStrippedWhenLenientResolutionIsInvalid() {
        DatasetShadowRelation shadow = new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds");
        var analyzer = datasetExternalAnalyzer().addLenientResolution(shadow.linkedIndexPattern(), IndexResolution.invalid("not found"))
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(unionOf(datasetExternal("ds"), shadow));

        var limit = as(plan, Limit.class);
        var externalRelation = as(unwrapProject(limit.child()), ExternalRelation.class);
        assertEquals(DATASET_PATH, externalRelation.fileList().originalPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Shadow whose lenient entry is {@link IndexResolution#empty}, a lenient lookup that matched
     * nothing: treated as no match, so the shadow is stripped and the single-survivor union
     * collapses to the dataset's {@link ExternalRelation}, exactly like a missing or invalid entry.
     */
    public void testShadowStrippedWhenLenientResolutionIsEmpty() {
        DatasetShadowRelation shadow = new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds");
        var analyzer = datasetExternalAnalyzer().addLenientResolution(shadow.linkedIndexPattern(), IndexResolution.empty("ds"))
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(unionOf(datasetExternal("ds"), shadow));

        var limit = as(plan, Limit.class);
        var externalRelation = as(unwrapProject(limit.child()), ExternalRelation.class);
        assertEquals(DATASET_PATH, externalRelation.fileList().originalPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * A matched linked index whose mapping is empty (field-caps returned the index but no usable
     * fields) still resolves the shadow to an {@link EsRelation}: "matched" is defined by the
     * resolution's resolved indices, not by the mapping.
     */
    public void testShadowResolvesWhenMatchedIndexHasEmptyMapping() {
        DatasetShadowRelation shadow = new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds");
        var analyzer = analyzer().addLenientResolution(
            shadow.linkedIndexPattern(),
            IndexResolution.valid(EsIndexGenerator.esIndex("ds"), Set.of("ds"), Map.of())
        ).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(shadow);

        var limit = as(plan, Limit.class);
        var esRelation = as(unwrapProject(limit.child()), EsRelation.class);
        assertEquals("ds", esRelation.indexPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Regression for the CPS view-over-dataset failure: a view whose body applies a pipeline stage to a
     * dataset produces, before analysis,
     * {@code ViewUnionAll(NamedSubquery(Eval(UnionAll(external, datasetShadow))), viewShadow)}. When
     * neither shadow matches a linked index (both lenient entries are valid-but-empty), both shadows must
     * be stripped and both unions must collapse, leaving the {@code Eval} directly over the dataset's
     * {@link ExternalRelation} with no {@code UnionAll} anywhere in the plan. Folding either shadow into
     * an empty {@code EsRelation} instead keeps the inner union alive below the {@code Eval}, where view
     * compaction cannot flatten it, and the post-optimization nested-union verifier rejects the query.
     */
    public void testPipelineViewOverDatasetWithEmptyShadowResolutionsCollapses() {
        DatasetShadowRelation dsShadow = new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds");
        ViewShadowRelation viewShadow = new ViewShadowRelation(EMPTY, "v", LinkedIndexPattern.Kind.OPTIONAL, "v");
        ViewUnionAll viewUnion = pipelineViewOverDataset(dsShadow, viewShadow);

        var analyzer = datasetExternalAnalyzer().addLenientResolution(dsShadow.linkedIndexPattern(), IndexResolution.empty("ds"))
            .addLenientResolution(viewShadow.linkedIndexPattern(), IndexResolution.empty("v"))
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(viewUnion);

        assertFalse("no UnionAll should survive, got: " + plan, containsNode(plan, UnionAll.class));
        assertFalse("no EsRelation should survive, got: " + plan, containsNode(plan, EsRelation.class));
        assertTrue("expected the Eval to survive, got: " + plan, containsNode(plan, Eval.class));
        int[] externalCount = { 0 };
        plan.forEachDown(ExternalRelation.class, n -> externalCount[0]++);
        assertEquals("expected exactly one ExternalRelation, got: " + plan, 1, externalCount[0]);
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Mixed shadow outcomes in one plan: a linked project holds an index named after the view but none
     * named after the dataset, so the view name resolves to both a local view and a linked index while
     * the dataset name resolves only to the dataset. The matched view shadow must be folded into an
     * {@code EsRelation} and kept as a second {@link ViewUnionAll} branch, while the empty dataset shadow
     * is stripped and its union collapses. Guards against the no-match predicate over-stripping: a fix
     * that treated every shadow as unmatched would silently drop the linked index's rows.
     */
    public void testPipelineViewOverDatasetWithMatchedViewShadowAndEmptyDatasetShadow() {
        DatasetShadowRelation dsShadow = new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds");
        ViewShadowRelation viewShadow = new ViewShadowRelation(EMPTY, "v", LinkedIndexPattern.Kind.OPTIONAL, "v");
        ViewUnionAll viewUnion = pipelineViewOverDataset(dsShadow, viewShadow);
        EsIndex linkedView = EsIndexGenerator.esIndex(
            "v",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("v", IndexMode.STANDARD)
        );

        var analyzer = datasetExternalAnalyzer().addLenientResolution(dsShadow.linkedIndexPattern(), IndexResolution.empty("ds"))
            .addLenientResolution(linkedView)
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(viewUnion);

        var survivingViewUnion = onlyNode(plan, ViewUnionAll.class);
        assertEquals(
            "expected the matched view shadow to survive as a second branch, got: " + plan,
            2,
            survivingViewUnion.children().size()
        );
        assertEquals("the dataset union should have collapsed, got: " + plan, 0, plainUnionCount(plan));
        assertEquals("expected exactly one ExternalRelation, got: " + plan, 1, countNodes(plan, ExternalRelation.class));
        assertEquals("expected exactly one EsRelation for the matched view shadow, got: " + plan, 1, countNodes(plan, EsRelation.class));
        assertTrue("expected the view body's Eval to survive, got: " + plan, containsNode(plan, Eval.class));
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * The mirror of {@link #testPipelineViewOverDatasetWithMatchedViewShadowAndEmptyDatasetShadow}: a
     * linked project holds an index named after the dataset but none named after the view, so the dataset
     * name resolves to both a dataset and a linked index. The matched dataset shadow stays as a second
     * branch of the plain {@link UnionAll} beneath the view body's pipeline stage, while the empty view
     * shadow is stripped and the {@link ViewUnionAll} collapses to the body alone. The surviving inner
     * union is no longer nested inside another union, so it is legal.
     */
    public void testPipelineViewOverDatasetWithMatchedDatasetShadowAndEmptyViewShadow() {
        DatasetShadowRelation dsShadow = new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds");
        ViewShadowRelation viewShadow = new ViewShadowRelation(EMPTY, "v", LinkedIndexPattern.Kind.OPTIONAL, "v");
        ViewUnionAll viewUnion = pipelineViewOverDataset(dsShadow, viewShadow);
        EsIndex linkedDs = EsIndexGenerator.esIndex(
            "ds",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("ds", IndexMode.STANDARD)
        );

        var analyzer = datasetExternalAnalyzer().addLenientResolution(viewShadow.linkedIndexPattern(), IndexResolution.empty("v"))
            .addLenientResolution(linkedDs)
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(viewUnion);

        assertFalse("the view union should have collapsed, got: " + plan, containsNode(plan, ViewUnionAll.class));
        assertEquals("expected the dataset union to survive, got: " + plan, 1, plainUnionCount(plan));
        var survivingUnion = onlyNode(plan, UnionAll.class);
        assertEquals(
            "expected the matched dataset shadow to survive as a second branch, got: " + plan,
            2,
            survivingUnion.children().size()
        );
        assertEquals("expected exactly one ExternalRelation, got: " + plan, 1, countNodes(plan, ExternalRelation.class));
        assertEquals("expected exactly one EsRelation for the matched dataset shadow, got: " + plan, 1, countNodes(plan, EsRelation.class));
        assertTrue("expected the view body's Eval to survive, got: " + plan, containsNode(plan, Eval.class));
        assertWarnings(NO_LIMIT_WARNING);
    }

    public void testNestedViewsOverDatasetWithEmptyShadowResolutionsCollapse() {
        DatasetShadowRelation dsShadow = new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds");
        ViewShadowRelation viewShadow1 = new ViewShadowRelation(EMPTY, "v1", LinkedIndexPattern.Kind.OPTIONAL, "v1");
        ViewShadowRelation viewShadow2 = new ViewShadowRelation(EMPTY, "v2", LinkedIndexPattern.Kind.OPTIONAL, "v2");
        ViewShadowRelation viewShadow3 = new ViewShadowRelation(EMPTY, "v3", LinkedIndexPattern.Kind.OPTIONAL, "v3");
        LogicalPlan nestedViews = viewOver(
            "v3",
            viewOver("v2", viewOver("v1", unionOf(datasetExternal("ds"), dsShadow), viewShadow1), viewShadow2),
            viewShadow3
        );

        var analyzer = datasetExternalAnalyzer().addLenientResolution(dsShadow.linkedIndexPattern(), IndexResolution.empty("ds"))
            .addLenientResolution(viewShadow1.linkedIndexPattern(), IndexResolution.empty("v1"))
            .addLenientResolution(viewShadow2.linkedIndexPattern(), IndexResolution.empty("v2"))
            .addLenientResolution(viewShadow3.linkedIndexPattern(), IndexResolution.empty("v3"))
            .buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(nestedViews);

        assertFalse("no union should survive, got: " + plan, containsNode(plan, UnionAll.class));
        assertFalse("no dataset shadow should survive, got: " + plan, containsNode(plan, DatasetShadowRelation.class));
        assertFalse("no view shadow should survive, got: " + plan, containsNode(plan, ViewShadowRelation.class));
        assertEquals("expected exactly one ExternalRelation, got: " + plan, 1, countNodes(plan, ExternalRelation.class));
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Strategy A: when both the dataset external relation and the shadow resolve, they live as separate
     * siblings inside the {@link UnionAll}. No merging.
     */
    public void testShadowResolvesAlongsideDatasetExternalRelation() {
        EsIndex remoteDs = EsIndexGenerator.esIndex(
            "ds",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("ds", IndexMode.STANDARD)
        );
        var analyzer = datasetExternalAnalyzer().addLenientResolution(remoteDs).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(
            unionOf(datasetExternal("ds"), new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds"))
        );

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals("expected two children, got: " + unionAll, 2, unionAll.children().size());
        // One branch carries the dataset's ExternalRelation, the other the shadow's resolved EsRelation —
        // kept separate (Strategy A). Output-alignment may wrap each branch in Project/Eval, so search the
        // whole branch subtree for the leaf rather than assuming a fixed wrapper depth.
        long externalBranches = unionAll.children().stream().filter(c -> containsNode(c, ExternalRelation.class)).count();
        long esRelationBranches = unionAll.children().stream().filter(c -> containsNode(c, EsRelation.class)).count();
        assertEquals("expected one ExternalRelation branch", 1, externalBranches);
        assertEquals("expected one EsRelation branch", 1, esRelationBranches);
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Shadow with exclusions: the lookup key is the shadow's full
     * {@link DatasetShadowRelation#linkedIndexPattern()} — dataset name <em>plus</em> the comma-joined
     * exclusions. Confirms an entry registered under that exact pattern resolves the shadow.
     */
    public void testShadowLookupKeyIncludesExclusions() {
        DatasetShadowRelation shadow = new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds,-stale-*");
        assertEquals("ds,-stale-*", shadow.linkedIndexPattern().pattern().indexPattern());
        EsIndex remoteDs = EsIndexGenerator.esIndex(
            "ds",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("ds", IndexMode.STANDARD)
        );
        var analyzer = analyzer().addLenientResolution(shadow.linkedIndexPattern(), IndexResolution.valid(remoteDs)).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(shadow);

        var limit = as(plan, Limit.class);
        var esRelation = as(unwrapProject(limit.child()), EsRelation.class);
        assertEquals("ds", esRelation.indexPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Two shadows with the <em>same</em> dataset name but <em>different</em> exclusion lists are looked
     * up independently in {@link AnalyzerContext#linkedResolution()}: one combination resolves to a
     * linked index (becomes an {@link EsRelation}), the other has no entry (stays unresolved → stripped).
     * Keying the map by dataset name alone would conflate the two.
     */
    public void testShadowsWithSameDatasetNameDifferentExclusionsResolveIndependently() {
        DatasetShadowRelation matchedShadow = new DatasetShadowRelation(
            EMPTY,
            "my-data",
            LinkedIndexPattern.Kind.OPTIONAL,
            "my-data,-unrelated-*"
        );
        DatasetShadowRelation emptyShadow = new DatasetShadowRelation(
            EMPTY,
            "my-data",
            LinkedIndexPattern.Kind.OPTIONAL,
            "my-data,-my-data,-unrelated-*"
        );
        EsIndex remoteMyData = EsIndexGenerator.esIndex(
            "my-data",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("my-data", IndexMode.STANDARD)
        );
        // Only the matched-shadow's pattern has a lenient entry; the empty-shadow's pattern is absent.
        var analyzer = datasetExternalAnalyzer().addLenientResolution(
            matchedShadow.linkedIndexPattern(),
            IndexResolution.valid(remoteMyData)
        ).buildAnalyzer();

        // matchedShadow alone: resolves to EsRelation.
        LogicalPlan matchedPlan = analyzer.analyze(matchedShadow);
        var matchedEs = as(unwrapProject(as(matchedPlan, Limit.class).child()), EsRelation.class);
        assertEquals("my-data", matchedEs.indexPattern());

        // emptyShadow paired with the dataset external: stripped, leaving just the ExternalRelation.
        LogicalPlan emptyPlan = analyzer.analyze(unionOf(datasetExternal("my-data"), emptyShadow));
        var external = as(unwrapProject(as(emptyPlan, Limit.class).child()), ExternalRelation.class);
        assertEquals(DATASET_PATH, external.fileList().originalPattern());

        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * Regression: a plain {@link UnionAll} with both a strict {@link UnresolvedRelation} whose resolution is
     * {@code EMPTY_SUBQUERY} (e.g. a CCS subquery that matched nothing) <em>and</em> a matched dataset shadow.
     * After analysis the EMPTY_SUBQUERY branch is pruned by {@code PruneEmptyUnionAllBranch} while the shadow
     * resolves to an {@link EsRelation} — the resolved {@code ds} shadow must SURVIVE the prune (the empty branch
     * disappearing must not strip the matched shadow with it). The dataset analog of
     * {@code ResolveViewShadowTests.testPruneEmptySubqueryBranchPreservesShadowResolutionInViewUnionAll}, but over a
     * plain {@link UnionAll} (datasets) rather than a {@code ViewUnionAll}. Since
     * {@link UnionAll#pruneEmptyBranches} preserves single-survivor wrappers, the surviving plan is a single-child
     * {@link UnionAll} carrying just the resolved {@code ds} branch.
     */
    public void testPruneEmptySubqueryBranchPreservesShadowResolutionInDatasetUnionAll() {
        EsIndex remoteDs = EsIndexGenerator.esIndex(
            "ds",
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of("ds", IndexMode.STANDARD)
        );
        var analyzer = analyzer().addIndex("missing_idx", IndexResolution.EMPTY_SUBQUERY).addLenientResolution(remoteDs).buildAnalyzer();

        LogicalPlan plan = analyzer.analyze(
            new UnionAll(
                EMPTY,
                List.of(strictUR("missing_idx"), new DatasetShadowRelation(EMPTY, "ds", LinkedIndexPattern.Kind.OPTIONAL, "ds")),
                List.of()
            )
        );

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals("expected a single surviving child after pruning", 1, unionAll.children().size());
        var esRelation = as(unwrapProject(unionAll.children().getFirst()), EsRelation.class);
        assertEquals("ds", esRelation.indexPattern());
        assertWarnings(NO_LIMIT_WARNING);
    }

    // ---- helpers ----

    /** A strict (non-optional) {@link UnresolvedRelation} for a CCS subquery branch, mirroring a {@code FROM} head. */
    private static UnresolvedRelation strictUR(String pattern) {
        return new UnresolvedRelation(EMPTY, new IndexPattern(EMPTY, pattern), false, List.of(), IndexMode.STANDARD, null, "FROM");
    }

    /** Build a plain {@link UnionAll} of the dataset's external relation and its shadow, mirroring DatasetRewriter. */
    private static UnionAll unionOf(UnresolvedExternalRelation external, DatasetShadowRelation shadow) {
        return new UnionAll(EMPTY, List.of(external, shadow), List.of());
    }

    /** The {@link UnresolvedExternalRelation} the DatasetRewriter produces for a single dataset. */
    private static UnresolvedExternalRelation datasetExternal(String datasetName) {
        return new UnresolvedExternalRelation(EMPTY, Literal.keyword(EMPTY, DATASET_PATH), Map.of(), List.of(), datasetName);
    }

    /** A {@link TestAnalyzer} whose external source resolution resolves {@link #DATASET_PATH}. */
    private static TestAnalyzer datasetExternalAnalyzer() {
        var entries = List.of(new StorageEntry(StoragePath.of("s3://bucket/ds/f1.parquet"), 100, Instant.EPOCH));
        FileList fileList = GlobExpander.fileListOf(entries, DATASET_PATH);
        List<Attribute> schema = List.of(referenceAttribute("id", LONG), referenceAttribute("name", KEYWORD));
        return analyzer().externalSourceResolution(DATASET_PATH, schema, fileList);
    }

    /** Walks past one analyzer-inserted {@link Project} wrapper to expose the underlying relation. */
    private static LogicalPlan unwrapProject(LogicalPlan plan) {
        return plan instanceof Project p ? p.child() : plan;
    }

    /**
     * The pre-analysis shape produced for a view whose body applies a pipeline stage to a dataset:
     * {@code ViewUnionAll(NamedSubquery(Eval(UnionAll(external, datasetShadow))), viewShadow)}. The
     * {@code Eval} between the two unions is what makes the shape interesting: view compaction only
     * flattens a union that is a direct child, so an inner union that survives here cannot be lifted.
     */
    private static ViewUnionAll pipelineViewOverDataset(DatasetShadowRelation dsShadow, ViewShadowRelation viewShadow) {
        Eval eval = new Eval(
            EMPTY,
            unionOf(datasetExternal("ds"), dsShadow),
            List.of(new Alias(EMPTY, "marker", Literal.keyword(EMPTY, "x")))
        );
        return viewOver("v", eval, viewShadow);
    }

    private static ViewUnionAll viewOver(String name, LogicalPlan body, ViewShadowRelation shadow) {
        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put(name, new NamedSubquery(EMPTY, body, name));
        children.put(name + "#shadow", shadow);
        return new ViewUnionAll(EMPTY, children, List.of());
    }

    /** True if the plan subtree contains a node of the given type anywhere below (or at) the given root. */
    private static boolean containsNode(LogicalPlan plan, Class<? extends LogicalPlan> nodeType) {
        boolean[] found = { false };
        plan.forEachDown(nodeType, n -> found[0] = true);
        return found[0];
    }

    /** Number of nodes of the given type anywhere below (or at) the given root. */
    private static int countNodes(LogicalPlan plan, Class<? extends LogicalPlan> nodeType) {
        int[] count = { 0 };
        plan.forEachDown(nodeType, n -> count[0]++);
        return count[0];
    }

    /**
     * Number of plain {@link UnionAll} nodes, excluding {@link ViewUnionAll}. {@code ViewUnionAll}
     * extends {@code UnionAll}, so counting the base type alone cannot tell the dataset union and the
     * view union apart.
     */
    private static int plainUnionCount(LogicalPlan plan) {
        return countNodes(plan, UnionAll.class) - countNodes(plan, ViewUnionAll.class);
    }

    /** Asserts exactly one node of the given type exists in the subtree and returns it. */
    private static <T extends LogicalPlan> T onlyNode(LogicalPlan plan, Class<T> nodeType) {
        List<T> found = new ArrayList<>();
        plan.forEachDown(nodeType, found::add);
        assertEquals("expected exactly one " + nodeType.getSimpleName() + ", got: " + plan, 1, found.size());
        return found.get(0);
    }
}
