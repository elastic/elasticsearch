/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.DatasetShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class DatasetRewriterTests extends ESTestCase {

    private static final IndexNameExpressionResolver RESOLVER = TestIndexNameExpressionResolver.newInstance();

    public void testNoDatasetsLeavesPlanUnchanged() {
        UnresolvedRelation relation = relationOf("my_index");
        ProjectMetadata project = projectWith(Map.of(), Map.of());
        assertSame(relation, rewrite(relation, project));
    }

    public void testUnknownNameLeavesPlanUnchanged() {
        // Even when the cluster has datasets registered, a FROM target whose name matches neither a
        // dataset nor an index is left unchanged for the analyzer to resolve (or fail) as an index.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("not_a_dataset_or_index");
        assertSame(relation, rewrite(relation, project));
    }

    public void testSingleDatasetRewritesToUnresolvedExternalRelation() {
        DataSource parent = dataSource("s3_parent", Map.of("region", new DataSourceSetting("us-east-1", false)));
        Dataset dataset = new Dataset(
            "logs",
            new DataSourceReference("s3_parent"),
            "s3://logs/*.parquet",
            null,
            Map.of("format", "parquet")
        );
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = rewrite(relationOf("logs"), project);

        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation out = (UnresolvedExternalRelation) rewritten;
        assertThat(tablePathString(out), equalTo("s3://logs/*.parquet"));
        assertThat(datasourceParamValue(out, "region"), equalTo("us-east-1"));
        assertThat(paramValue(out, "format"), equalTo("parquet"));
    }

    public void testDatasetSettingsOverrideParentOnKeyCollision() {
        DataSource parent = dataSource("s3_parent", Map.of("region", new DataSourceSetting("us-east-1", false)));
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of("region", "eu-west-2"));
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = rewrite(relationOf("logs"), project);
        assertThat(paramValue((UnresolvedExternalRelation) rewritten, "region"), equalTo("eu-west-2"));
    }

    public void testMultipleDatasetsProduceUnionAll() {
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds1 = new Dataset("ds1", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset ds2 = new Dataset("ds2", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("ds1", ds1, "ds2", ds2));

        LogicalPlan rewritten = rewrite(relationOf("ds1,ds2"), project);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(2));
        assertThat(union.children().get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(union.children().get(1), instanceOf(UnresolvedExternalRelation.class));
    }

    public void testMixedDatasetsAndNonDatasetsProducesUnionAll() {
        // Phase 2: heterogeneous FROM (index + dataset) produces UnionAll instead of rejecting.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("logs", dataset), Set.of("some_idx"));

        LogicalPlan rewritten = rewrite(relationOf("some_idx,logs"), project);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(2));
        // Dataset branches precede the index branch under the unified rail.
        // First child: UnresolvedExternalRelation for the dataset
        assertThat(union.children().get(0), instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation datasetBranch = (UnresolvedExternalRelation) union.children().get(0);
        assertThat(tablePathString(datasetBranch), equalTo("s3://logs/"));
        // Second child: UnresolvedRelation for the non-dataset index
        assertThat(union.children().get(1), instanceOf(UnresolvedRelation.class));
        UnresolvedRelation indexBranch = (UnresolvedRelation) union.children().get(1);
        assertThat(indexBranch.indexPattern().indexPattern(), equalTo("some_idx"));
    }

    public void testMixedMultipleIndicesAndDatasetsProducesUnionAll() {
        // Multiple non-dataset indices are joined into a single UnresolvedRelation branch;
        // each dataset becomes its own UnresolvedExternalRelation branch.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds1 = new Dataset("ds1", new DataSourceReference("s3_parent"), "s3://ds1/", null, Map.of());
        Dataset ds2 = new Dataset("ds2", new DataSourceReference("s3_parent"), "s3://ds2/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("ds1", ds1, "ds2", ds2), Set.of("idx1", "idx2"));

        LogicalPlan rewritten = rewrite(relationOf("idx1,idx2,ds1,ds2"), project);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        // 1 index branch + 2 dataset branches = 3 children
        assertThat(union.children(), hasSize(3));
        assertThat(union.children().get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(union.children().get(1), instanceOf(UnresolvedExternalRelation.class));
        assertThat(union.children().get(2), instanceOf(UnresolvedRelation.class));
    }

    public void testIndexModeNonStandardRejected() {
        // Note on coverage: only TIME_SERIES (via TS) and LOOKUP (via LOOKUP JOIN) are user-reachable
        // through ESQL syntax; LOGSDB has no user-syntax path that constructs an UnresolvedRelation
        // with IndexMode.LOGSDB pointing at a dataset name. The LOGSDB branch is defensive code for
        // any future path that might set it. There is no IT analogue for LOGSDB — this unit case
        // pins the rejection-message contract.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        Map<IndexMode, String> expectedFragments = Map.of(
            IndexMode.TIME_SERIES,
            "TS command is not supported for datasets",
            IndexMode.LOOKUP,
            "LOOKUP JOIN against a dataset is not supported",
            IndexMode.LOGSDB,
            "LOGSDB index mode on FROM <dataset> is not supported"
        );

        for (Map.Entry<IndexMode, String> entry : expectedFragments.entrySet()) {
            VerificationException ex = expectThrows(
                VerificationException.class,
                () -> rewrite(relationOfWithMode("logs", entry.getKey()), project)
            );
            assertThat(ex.getMessage(), containsString(entry.getValue()));
            assertThat(ex.getMessage(), containsString("logs"));
        }
    }

    public void testMetadataFieldsThreadedToUnresolvedExternalRelation() {
        // METADATA fields on FROM <dataset> are no longer rejected; the rewriter carries them
        // verbatim into UnresolvedExternalRelation so the analyzer's ResolveExternalRelations can
        // bind each one to an ExternalMetadataAttribute. The registered dataset name also flows
        // through (drives the per-file _index synthesizer).
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = new UnresolvedRelation(
            Source.EMPTY,
            new IndexPattern(Source.EMPTY, "logs"),
            false,
            List.of(MetadataAttribute.create(Source.EMPTY, MetadataAttribute.INDEX), MetadataAttribute.create(Source.EMPTY, "_id")),
            IndexMode.STANDARD,
            null
        );
        LogicalPlan rewritten = rewrite(relation, project);

        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation out = (UnresolvedExternalRelation) rewritten;
        assertThat("metadata fields preserved verbatim", out.metadataFields(), hasSize(2));
        assertThat(out.metadataFields().get(0).name(), equalTo(MetadataAttribute.INDEX));
        assertThat(out.metadataFields().get(1).name(), equalTo("_id"));
        assertThat("dataset name threaded", out.datasetName(), equalTo("logs"));
    }

    public void testDatasetReferencingUnknownDataSourceFailsWithExplicitMessage() {
        // Production scenario: a broken cluster-state restore leaves a Dataset whose parent
        // DataSource is no longer registered. DataSourceService.deleteDataSources rejects (409)
        // when any dataset still references a data source, so this is normally impossible —
        // but if the invariant breaks, the rewriter must fail with a message that names the
        // missing parent, not NPE inside mergeSettings. Asserts are off in production, so the
        // rewriter throws IllegalStateException explicitly.
        Dataset orphan = new Dataset("orphan_ds", new DataSourceReference("missing_parent"), "s3://orphan/", null, Map.of());
        // Note: dataSources map intentionally does NOT contain "missing_parent" — this is the
        // broken-state we're simulating.
        ProjectMetadata project = projectWith(Map.of(), Map.of("orphan_ds", orphan));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> rewrite(relationOf("orphan_ds"), project));
        assertThat(ex.getMessage(), containsString("dataset [orphan_ds]"));
        assertThat(ex.getMessage(), containsString("unknown data source [missing_parent]"));
    }

    public void testNonSecretSettingsArriveAsTheirOriginalValue() {
        // Non-secret settings are placed in the carrier as their underlying Object (String, Integer,
        // Boolean...). Asserts that mergeSettings does not transform them.
        DataSource parent = dataSource("s3_parent", Map.of("region", new DataSourceSetting("us-east-1", false)));
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = rewrite(relationOf("logs"), project);
        assertThat(datasourceParamValue((UnresolvedExternalRelation) rewritten, "region"), equalTo("us-east-1"));
    }

    // ---- Pattern expansion (parity with FROM <index> patterns via IndexNameExpressionResolver) ----

    public void testWildcardMatchesDatasets() {
        // `FROM logs_*` expands through IndexNameExpressionResolver and picks up every dataset whose
        // name matches the glob — same machinery FROM <index> uses for indices and aliases.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("logs_b", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_b", b));

        LogicalPlan rewritten = rewrite(relationOf("logs_*"), project);
        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(2));
        assertThat(union.children().get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(union.children().get(1), instanceOf(UnresolvedExternalRelation.class));
    }

    public void testWildcardMatchingNoDatasetsLeavesPlanUnchanged() {
        // Pattern matching no datasets and no indices is left for the analyzer to handle (which will
        // error appropriately based on the request's IndicesOptions).
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("metrics_*");
        assertSame(relation, rewrite(relation, project));
    }

    public void testClosedIndexCountsAsNonDatasetInHeterogeneousUnionAll() {
        // The rewriter's IndicesOptions are based on IndexResolver.DEFAULT_OPTIONS so the gatekeeper
        // matches user-side semantics. allowClosedIndices=true means a closed index in the pattern
        // appears in the resolver's result and is bucketed as a non-dataset — preventing the
        // silent-drop where a closed index would disappear and the rewriter would produce a
        // dataset-only relation.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());

        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("s3_parent", parent)))
            .datasets(Map.of("logs", dataset));
        builder.put(
            IndexMetadata.builder("my_closed_index")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .state(IndexMetadata.State.CLOSE)
                .build(),
            false
        );
        ProjectMetadata project = builder.build();

        LogicalPlan rewritten = rewrite(relationOf("my_closed_index,logs"), project);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(2));
        assertThat(union.children().get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(union.children().get(1), instanceOf(UnresolvedRelation.class));
    }

    public void testWildcardSpanningIndicesAndDatasetsProducesUnionAll() {
        // `FROM logs_*` matching both real indices and datasets produces a heterogeneous
        // UnionAll instead of rejecting. The index branch is a single UnresolvedRelation whose
        // pattern holds the concrete resolved index name(s); each dataset becomes its own branch.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds = new Dataset("logs_dataset", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("logs_dataset", ds), Set.of("logs_index"));

        LogicalPlan rewritten = rewrite(relationOf("logs_*"), project);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(2));
        assertThat(union.children().get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(union.children().get(1), instanceOf(UnresolvedRelation.class));
    }

    public void testWildcardMatchingDatasetUnderCpsPreservesRemoteHalf() {
        // `FROM logs_*` matches a local dataset and no local index. With CPS on, the same wildcard may also match
        // indices in linked projects — mirror views: keep the dataset's external relation AND re-emit the original
        // wildcard as an UnresolvedRelation so the remote half resolves at field-caps (instead of dropping it).
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds = new Dataset("logs_dataset", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_dataset", ds));

        LogicalPlan rewritten = rewriteWithAuthorizedCps(relationOf("logs_*"), project, Set.of("logs_dataset"));

        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> children = rewritten.children();
        assertThat(children, hasSize(2));
        assertThat(children.get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(children.get(1), instanceOf(UnresolvedRelation.class));
        assertThat(((UnresolvedRelation) children.get(1)).indexPattern().indexPattern(), equalTo("logs_*"));
    }

    public void testWildcardMatchingDatasetWithoutCpsNotPreserved() {
        // Same query, CPS off (the shipped config today): the dataset replaces the relation outright, no remote pass.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds = new Dataset("logs_dataset", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_dataset", ds));

        LogicalPlan rewritten = rewriteWithAuthorized(relationOf("logs_*"), project, Set.of("logs_dataset"));

        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
    }

    public void testExplicitDatasetNameUnderCpsEmitsShadow() {
        // An exact (non-wildcard) dataset name has no wildcard to re-emit, so its remote half would never reach
        // field-caps. Under CPS the rewriter emits a DatasetShadowRelation sibling next to the dataset's external
        // relation (inside a plain UnionAll) so the lenient linked pass can federate a remote index of the same name —
        // the dataset analog of ViewResolver's OPTIONAL-shadow branch. The unmatched shadow is later stripped by the
        // analyzer, returning to the bare external shape; matched, it survives as a sibling EsRelation.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds = new Dataset("logs_dataset", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_dataset", ds));

        LogicalPlan rewritten = rewriteWithAuthorizedCps(relationOf("logs_dataset"), project, Set.of("logs_dataset"));

        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> children = rewritten.children();
        assertThat(children, hasSize(2));
        assertThat(children.get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(children.get(1), instanceOf(DatasetShadowRelation.class));
        DatasetShadowRelation shadow = (DatasetShadowRelation) children.get(1);
        assertThat(shadow.datasetName(), equalTo("logs_dataset"));
        assertThat(shadow.linkedIndexPattern().pattern().indexPattern(), equalTo("logs_dataset"));
    }

    public void testExplicitDatasetNameWithoutCpsEmitsNoShadow() {
        // Same query, CPS off (the shipped config today): the dataset replaces the relation outright, no shadow.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds = new Dataset("logs_dataset", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_dataset", ds));

        LogicalPlan rewritten = rewriteWithAuthorized(relationOf("logs_dataset"), project, Set.of("logs_dataset"));

        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
    }

    public void testExplicitDatasetShadowCarriesTrailingExclusions() {
        // The shadow's pattern is the exact name plus the relation's trailing exclusions, so the remote half honors the
        // same exclusions the local FROM did — mirroring ViewResolver.collectExclusionsAfterPosition.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("logs_b", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_b", b));

        LogicalPlan rewritten = rewriteWithAuthorizedCps(relationOf("logs_a,-stale-*"), project, Set.of("logs_a"));

        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> children = rewritten.children();
        // external(logs_a) + shadow(logs_a,-stale-*); the exclusion-only relation has no positive wildcard so no
        // UnresolvedRelation is preserved.
        assertThat(children, hasSize(2));
        assertThat(children.get(0), instanceOf(UnresolvedExternalRelation.class));
        DatasetShadowRelation shadow = (DatasetShadowRelation) children.get(1);
        assertThat(shadow.linkedIndexPattern().pattern().indexPattern(), equalTo("logs_a,-stale-*"));
    }

    public void testMultipleExactDatasetNamesUnderCpsEmitShadowEach() {
        // FROM ds1,ds2 (both exact) under CPS: two externals + two shadows in one UnionAll.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds1 = new Dataset("ds1", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset ds2 = new Dataset("ds2", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("ds1", ds1, "ds2", ds2));

        LogicalPlan rewritten = rewriteWithAuthorizedCps(relationOf("ds1,ds2"), project, Set.of("ds1", "ds2"));

        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> children = rewritten.children();
        assertThat(children, hasSize(4));
        long externalCount = children.stream().filter(c -> c instanceof UnresolvedExternalRelation).count();
        long shadowCount = children.stream().filter(c -> c instanceof DatasetShadowRelation).count();
        assertThat(externalCount, equalTo(2L));
        assertThat(shadowCount, equalTo(2L));
    }

    public void testInterleavedExclusionAppliesPositionallyToShadows() {
        // FROM ds1,-x,ds2 — exclusions are positional (ES applies them left-to-right): -x precedes ds2, so only ds1's
        // shadow carries it. (A global sweep would wrongly narrow ds2's shadow with -x too.)
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds1 = new Dataset("ds1", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset ds2 = new Dataset("ds2", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("ds1", ds1, "ds2", ds2));

        LogicalPlan rewritten = rewriteWithAuthorizedCps(relationOf("ds1,-x,ds2"), project, Set.of("ds1", "ds2"));

        assertThat(rewritten, instanceOf(UnionAll.class));
        Set<String> shadowPatterns = rewritten.children()
            .stream()
            .filter(c -> c instanceof DatasetShadowRelation)
            .map(c -> ((DatasetShadowRelation) c).linkedIndexPattern().pattern().indexPattern())
            .collect(java.util.stream.Collectors.toSet());
        assertThat(shadowPatterns, equalTo(Set.of("ds1,-x", "ds2")));
    }

    public void testHeterogeneousFromUnderCpsEmitsShadowForDataset() {
        // A heterogeneous FROM (local index + local dataset) under CPS must run the same
        // non-remotable-abstraction rail as a dataset-only FROM. The dataset's exact name gets a DatasetShadowRelation
        // so a remote index of the same name reads both and a remote dataset/view of the same name fails. Before the
        // unification the heterogeneous path returned before the CPS rail, silently skipping the dataset's remote check.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds = new Dataset("logs_dataset", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("logs_dataset", ds), Set.of("some_idx"));

        LogicalPlan rewritten = rewriteWithAuthorizedCps(relationOf("some_idx,logs_dataset"), project, Set.of("logs_dataset"));

        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> children = rewritten.children();
        // external(logs_dataset) + index branch(some_idx) + shadow(logs_dataset)
        assertThat(children, hasSize(3));
        assertThat(children.get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(children.get(1), instanceOf(UnresolvedRelation.class));
        assertThat(((UnresolvedRelation) children.get(1)).indexPattern().indexPattern(), equalTo("some_idx"));
        assertThat(children.get(2), instanceOf(DatasetShadowRelation.class));
        assertThat(((DatasetShadowRelation) children.get(2)).datasetName(), equalTo("logs_dataset"));
    }

    public void testExactDatasetShadowsDoNotConsumeTheRewriteCap() {
        // The rewrite-time cap counts real reads (datasets + index branch), not the speculative shadows. A shadow
        // strips when its exact name has no remote namesake (the common case), so it must not eat the per-FROM budget.
        // Five exact datasets under CPS = 5 externals + 5 shadows = 10 UnionAll children but only 5 real reads, so it
        // must NOT be rejected at rewrite -- otherwise the shadows would silently halve the dataset budget.
        DataSource parent = dataSource("s3_parent", Map.of());
        Map<String, Dataset> datasets = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            datasets.put("ds" + i, new Dataset("ds" + i, new DataSourceReference("s3_parent"), "s3://" + i + "/", null, Map.of()));
        }
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), datasets);

        LogicalPlan rewritten = rewriteWithAuthorizedCps(relationOf("ds0,ds1,ds2,ds3,ds4"), project, datasets.keySet());

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(10)); // 5 externals + 5 shadows
        assertThat(union.children().stream().filter(c -> c instanceof DatasetShadowRelation).count(), equalTo(5L));
    }

    public void testNonStringSettingsArePreservedThroughCarrier() {
        // Non-secret DataSourceSetting values can be Integer/Long/Boolean — the carrier (Map<String,
        // Object>) should preserve their type. Plugins that read these don't need to parse strings.
        DataSource parent = dataSource(
            "s3_parent",
            Map.of(
                "max_connections",
                new DataSourceSetting(50, false),
                "request_timeout_ms",
                new DataSourceSetting(30000L, false),
                "use_compression",
                new DataSourceSetting(Boolean.TRUE, false)
            )
        );
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of("format", "parquet"));
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = rewrite(relationOf("logs"), project);

        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation out = (UnresolvedExternalRelation) rewritten;
        assertThat(datasourceParamValue(out, "max_connections"), equalTo(50));
        assertThat(datasourceParamValue(out, "request_timeout_ms"), equalTo(30000L));
        assertThat(datasourceParamValue(out, "use_compression"), equalTo(Boolean.TRUE));
        assertThat(out.config().get("format"), equalTo((Object) "parquet"));
    }

    public void testSecretSettingsForwardedAsPlaintextString() {
        // Plaintext-stored secrets (the no-encryption-service producer path) pass through mergeSettings
        // as their original String. The connector receives the String directly — no decrypt needed.
        DataSource parent = dataSource("s3_parent", Map.of("access_key", new DataSourceSetting("AKIAEXAMPLE_SECRET_VALUE", true)));
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = rewrite(relationOf("logs"), project);

        UnresolvedExternalRelation out = (UnresolvedExternalRelation) rewritten;
        Object accessKey = datasourceParamValue(out, "access_key");
        assertThat(accessKey, instanceOf(String.class));
        assertThat(accessKey, equalTo("AKIAEXAMPLE_SECRET_VALUE"));
    }

    public void testSecretSettingsForwardedAsEncryptedDataCarrier() {
        // Encrypted secrets (the master-side encryption step produced an EncryptedData carrier) pass
        // through mergeSettings by reference. The decryption step at the connector boundary
        // (DataSourceCredentials.decryptInPlace) recognizes the carrier by type and materialises
        // plaintext just before the SDK call.
        EncryptedData carrier = new EncryptedData("test-key", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
        DataSource parent = dataSource("s3_parent", Map.of("access_key", new DataSourceSetting(carrier, true)));
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = rewrite(relationOf("logs"), project);

        UnresolvedExternalRelation out = (UnresolvedExternalRelation) rewritten;
        Object accessKey = datasourceParamValue(out, "access_key");
        assertThat("encrypted secret stays an EncryptedData carrier on the live config map", accessKey, instanceOf(EncryptedData.class));
        assertSame("carrier is forwarded by reference", carrier, accessKey);
    }

    public void testFastPathSkipsResolverWhenNoPatternCouldMatchDataset() {
        // A pattern that doesn't match any registered dataset name (literal or wildcard) skips the
        // full resolver expansion. The relation is returned unchanged for the analyzer to handle
        // as today. Verified by `assertSame` — the relation reference is the same instance.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("metrics_unrelated");
        assertSame(relation, rewrite(relation, project));
    }

    public void testWildcardWithExclusion() {
        // `FROM logs_*, -logs_test` picks up matching datasets minus the excluded one.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset test = new Dataset("logs_test", new DataSourceReference("s3_parent"), "s3://test/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_test", test));

        LogicalPlan rewritten = rewrite(relationOf("logs_*,-logs_test"), project);
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation out = (UnresolvedExternalRelation) rewritten;
        assertThat(tablePathString(out), equalTo("s3://a/"));
    }

    public void testWildcardAtUnionAllCapSucceeds() {
        // UnionAll extends Fork which caps at 8 branches — the upper bound the rewriter can hand off.
        // A wildcard expanding to exactly the cap proves the bucketing + UnionAll construction path
        // is bounded-time at the platform's largest supported shape.
        DataSource parent = dataSource("s3_parent", Map.of());
        Map<String, Dataset> datasets = new HashMap<>();
        for (int i = 0; i < 8; i++) {
            datasets.put(
                "logs_" + i,
                new Dataset("logs_" + i, new DataSourceReference("s3_parent"), "s3://logs/" + i + "/", null, Map.of())
            );
        }
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), datasets);

        LogicalPlan rewritten = rewrite(relationOf("logs_*"), project);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(8));
    }

    public void testWildcardOverUnionAllCapRejectsWithUserFacingMessage() {
        // A wildcard matching more than 8 datasets crosses Fork's 8-branch cap. The rewriter
        // intercepts before constructing the UnionAll and throws a VerificationException with
        // user-facing framing — the user typed FROM <pattern>, not FORK, so the error references
        // the pattern + the cap, not Fork's internal name.
        DataSource parent = dataSource("s3_parent", Map.of());
        Map<String, Dataset> datasets = new HashMap<>();
        for (int i = 0; i < 9; i++) {
            datasets.put(
                "logs_" + i,
                new Dataset("logs_" + i, new DataSourceReference("s3_parent"), "s3://logs/" + i + "/", null, Map.of())
            );
        }
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), datasets);

        VerificationException ex = expectThrows(VerificationException.class, () -> rewrite(relationOf("logs_*"), project));
        assertThat(ex.getMessage(), containsString("FROM [logs_*]"));
        assertThat(ex.getMessage(), containsString("resolved to 9 branches"));
        assertThat(ex.getMessage(), containsString("the current limit of 8"));
        assertThat(ex.getMessage(), containsString("Narrow the pattern"));
    }

    public void testDateMathPatternReachesSlowPath() {
        // Fast-path predicate doesn't expand <...> so it returns true conservatively; resolver runs.
        // Pins that no-match date-math doesn't throw.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("<metrics-{now/d}>");
        assertSame(relation, rewrite(relation, project));
    }

    public void testLiteralPatternMatchingDatasetWithDateSuffixRewrites() {
        // A literal pattern that exactly matches a registered dataset whose name happens to contain
        // a date suffix should be rewritten to UnresolvedExternalRelation. This is the literal-match
        // case (no `<...>` expansion); the no-match date-math case is covered by
        // testDateMathPatternReachesSlowPath above. A real date-math match-case would require pinning
        // the resolver's clock, which the rewriter does not expose for tests.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs-2026-05-05", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs-2026-05-05", dataset));

        UnresolvedRelation relation = relationOf("logs-2026-05-05");
        LogicalPlan rewritten = rewrite(relation, project);
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
    }

    public void testNonMatchingExclusionLeavesDatasetsAlone() {
        // `-logs_doesnotexist` is an exclusion that matches nothing; the positive `logs_*` should
        // still expand to the registered datasets unchanged.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("logs_b", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_b", b));

        LogicalPlan rewritten = rewrite(relationOf("logs_*,-logs_doesnotexist"), project);
        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(2));
    }

    public void testMultiExclusion() {
        // `logs_*,-logs_a,-logs_b` excludes two registered datasets; only the rest should remain.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("logs_b", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        Dataset c = new Dataset("logs_c", new DataSourceReference("s3_parent"), "s3://c/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_b", b, "logs_c", c));

        LogicalPlan rewritten = rewrite(relationOf("logs_*,-logs_a,-logs_b"), project);
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation out = (UnresolvedExternalRelation) rewritten;
        assertThat(tablePathString(out), equalTo("s3://c/"));
    }

    public void testRemoteClusterPatternBailsOutOfDatasetRewriting() {
        // Datasets are local-only for the MVP. A cluster-prefixed pattern means CCS — let the
        // original FROM (with all its parts) flow through to regular CCS resolution.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("cluster-1:some_index");
        assertSame(relation, rewrite(relation, project));
    }

    public void testRemoteClusterPatternMatchingLocalDatasetNameBailsOut() {
        // A coincidental local-dataset name match on the local part of a remote-prefixed pattern
        // must not turn the query into a dataset query. The cluster prefix wins; downstream CCS
        // resolution handles `cluster-1:logs` as a remote name (and may fail naturally if absent).
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("cluster-1:logs");
        assertSame(relation, rewrite(relation, project));
    }

    public void testRemoteClusterPatternMixedWithLocalDatasetBailsOut() {
        // Mixing a cluster-prefixed pattern with a local dataset reference also bails — without the
        // bail, the slow path would silently drop the cluster-prefixed part and produce a
        // dataset-only rewrite. Bail out so downstream CCS resolution sees the full original FROM.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("cluster-1:remote_idx,logs");
        assertSame(relation, rewrite(relation, project));
    }

    public void testCommaSeparatedDatasetsAndWildcardCombine() {
        // `FROM logs_a, metrics_*` mixes a literal dataset name with a wildcard that expands to
        // additional datasets — all dataset-side, so a UnionAll of all three children.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset m1 = new Dataset("metrics_1", new DataSourceReference("s3_parent"), "s3://m1/", null, Map.of());
        Dataset m2 = new Dataset("metrics_2", new DataSourceReference("s3_parent"), "s3://m2/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "metrics_1", m1, "metrics_2", m2));

        LogicalPlan rewritten = rewrite(relationOf("logs_a,metrics_*"), project);
        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(3));
        for (LogicalPlan child : union.children()) {
            assertThat(child, instanceOf(UnresolvedExternalRelation.class));
        }
    }

    public void testWildcardSkipsUnauthorizedDataset() {
        // A wildcard expands only to datasets in the authorized set — an unauthorized dataset is
        // invisible, like an unauthorized index under a wildcard.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("logs_b", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_b", b));

        LogicalPlan rewritten = rewriteWithAuthorized(relationOf("logs_*"), project, Set.of("logs_a"));
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        assertThat(tablePathString((UnresolvedExternalRelation) rewritten), equalTo("s3://a/"));
    }

    public void testWildcardAllUnauthorizedLeavesPlanUnchanged() {
        // Nothing authorized under the wildcard: the relation flows through untouched, so the
        // analyzer fails it the same way it fails a pattern that matches nothing.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a));

        UnresolvedRelation relation = relationOf("logs_*");
        assertSame(relation, rewriteWithAuthorized(relation, project, Set.of()));
    }

    public void testExplicitUnauthorizedDatasetIsUnknownIndex() {
        // An explicitly named unauthorized dataset must produce the same error a missing index produces — Unknown index
        // (400) — so an unauthorized dataset is indistinguishable from a nonexistent name (no existence oracle).
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> rewriteWithAuthorized(relationOf("logs"), project, Set.of())
        );
        assertThat(ex.getMessage(), containsString("Unknown index [logs]"));
    }

    public void testExplicitUnauthorizedAmongAuthorizedThrows() {
        // Multi-target FROM with one authorized and one unauthorized dataset errors instead of silently returning
        // partial data for the authorized one — same Unknown index (400) error as above.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("ds1", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("ds2", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("ds1", a, "ds2", b));

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> rewriteWithAuthorized(relationOf("ds1,ds2"), project, Set.of("ds1"))
        );
        assertThat(ex.getMessage(), containsString("Unknown index [ds2]"));
    }

    public void testHeterogeneousFromUnauthorizedDatasetDoesNotBleed() {
        // A readable index co-located in the FROM does not let an unauthorized dataset through: the explicit unauthorized
        // dataset still fails as Unknown index, exactly as it would alone (no existence oracle). Mirrors
        // testExplicitUnauthorizedAmongAuthorizedThrows with a plain index in place of the authorized dataset. The
        // per-target classification this relies on is asserted by testResolveClassifiesIndexAndUnauthorizedDatasetPerTarget
        // (below).
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset secret = new Dataset("secret_ds", new DataSourceReference("s3_parent"), "s3://secret/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("secret_ds", secret), Set.of("some_idx"));

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> rewriteWithAuthorized(relationOf("some_idx,secret_ds"), project, Set.of("some_idx"))
        );
        assertThat(ex.getMessage(), containsString("Unknown index [secret_ds]"));
    }

    // ---- resolve(): the engine-side, per-relation expansion that the action body runs ----

    public void testResolveEmptyWithoutDatasets() {
        // No datasets registered: an explicit name resolves to no authorized datasets and no non-dataset targets.
        DatasetRewriter.DatasetResolution r = resolve("logs", projectWith(Map.of(), Map.of()), Set.of());
        assertThat(r.authorizedDatasets(), equalTo(Set.of()));
        assertTrue(r.nonDatasetNames().isEmpty());
    }

    public void testResolveExpandsToAuthorizedDatasetNames() {
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("logs_b", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_b", b));

        // Explicit single dataset.
        assertThat(resolve("logs_a", project, Set.of("logs_a")).authorizedDatasets(), equalTo(Set.of("logs_a")));
        // Wildcard, all authorized.
        assertThat(resolve("logs_*", project, Set.of("logs_a", "logs_b")).authorizedDatasets(), containsInAnyOrder("logs_a", "logs_b"));
        // Wildcard with exclusion, applied within the relation.
        assertThat(resolve("logs_*,-logs_b", project, Set.of("logs_a", "logs_b")).authorizedDatasets(), equalTo(Set.of("logs_a")));
        // No pattern can match a dataset name → empty.
        assertThat(resolve("metrics", project, Set.of("logs_a", "logs_b")).authorizedDatasets(), equalTo(Set.of()));
    }

    public void testResolveExclusionStaysPerRelation() {
        // The exclusion in one relation must not shadow another relation's expansion — each relation is resolved on its
        // own raw patterns. Here the second relation excludes logs_a; resolving the first still yields it.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("logs_b", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_b", b));

        assertThat(resolve("logs_*", project, Set.of("logs_a", "logs_b")).authorizedDatasets(), containsInAnyOrder("logs_a", "logs_b"));
        assertThat(resolve("logs_*,-logs_a", project, Set.of("logs_a", "logs_b")).authorizedDatasets(), equalTo(Set.of("logs_b")));
    }

    public void testResolveFlagsNonDatasetTargets() {
        // A wildcard spanning a dataset and a real index reports the non-dataset names, driving heterogeneous-FROM UnionAll.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds = new Dataset("logs_dataset", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("logs_dataset", ds), Set.of("logs_index"));

        DatasetRewriter.DatasetResolution r = resolve("logs_*", project, Set.of("logs_dataset"));
        assertThat(r.authorizedDatasets(), equalTo(Set.of("logs_dataset")));
        assertFalse(r.nonDatasetNames().isEmpty());
    }

    public void testResolveClassifiesIndexAndUnauthorizedDatasetPerTarget() {
        // Mixed FROM: the index and the unauthorized dataset are classified independently in one pass — the index is a
        // readable non-dataset target (drives the heterogeneous-FROM UnionAll index branch), the dataset is flagged
        // unauthorized. The enforcement of that flag is asserted by testHeterogeneousFromUnauthorizedDatasetDoesNotBleed
        // (above).
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset secret = new Dataset("secret_ds", new DataSourceReference("s3_parent"), "s3://secret/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("secret_ds", secret), Set.of("some_idx"));

        DatasetRewriter.DatasetResolution r = resolve("some_idx,secret_ds", project, Set.of("some_idx"));
        assertThat(r.nonDatasetNames(), containsInAnyOrder("some_idx"));
        assertThat(r.explicitUnauthorized(), containsInAnyOrder("secret_ds"));
    }

    // --

    /** Rewrite with every registered dataset authorized — the unsecured-cluster behavior. */
    private static LogicalPlan rewrite(LogicalPlan parsed, ProjectMetadata project) {
        return DatasetRewriter.rewriteUnsecured(parsed, project, RESOLVER);
    }

    /**
     * Rewrite one relation with only {@code authorized} datasets readable — the secured-cluster behavior. Models the
     * security filter narrowing the request indices to the authorized subset by passing {@code authorized} as the
     * relation's (filter-narrowed) indices, while the raw FROM pattern stays the un-narrowed mixed-FROM signal.
     */
    private static LogicalPlan rewriteWithAuthorized(UnresolvedRelation relation, ProjectMetadata project, Set<String> authorized) {
        DatasetRewriter.DatasetResolution resolution = resolve(relation.indexPattern().indexPattern(), project, authorized);
        return DatasetRewriter.rewrite(relation, project, Map.of(relation, resolution), false);
    }

    /** As {@link #rewriteWithAuthorized}, but with cross-project search (CPS) enabled. */
    private static LogicalPlan rewriteWithAuthorizedCps(UnresolvedRelation relation, ProjectMetadata project, Set<String> authorized) {
        DatasetRewriter.DatasetResolution resolution = resolve(relation.indexPattern().indexPattern(), project, authorized);
        return DatasetRewriter.rewrite(relation, project, Map.of(relation, resolution), true);
    }

    /** Engine-side resolve of {@code rawPattern} with {@code authorized} as the (filter-narrowed) request indices. */
    private static DatasetRewriter.DatasetResolution resolve(String rawPattern, ProjectMetadata project, Set<String> authorized) {
        String[] raw = Strings.splitStringByCommaToArray(rawPattern);
        return DatasetRewriter.resolve(authorized.toArray(String[]::new), raw, project, RESOLVER);
    }

    private static UnresolvedRelation relationOf(String pattern) {
        return relationOfWithMode(pattern, IndexMode.STANDARD);
    }

    private static UnresolvedRelation relationOfWithMode(String pattern, IndexMode indexMode) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, pattern), false, List.of(), indexMode, null);
    }

    private static DataSource dataSource(String name, Map<String, DataSourceSetting> settings) {
        return new DataSource(name, "test", null, settings);
    }

    private static ProjectMetadata projectWith(Map<String, DataSource> dataSources, Map<String, Dataset> datasets) {
        return projectWithIndices(dataSources, datasets, Set.of());
    }

    private static ProjectMetadata projectWithIndices(
        Map<String, DataSource> dataSources,
        Map<String, Dataset> datasets,
        Set<String> indexNames
    ) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(dataSources))
            .datasets(datasets);
        for (String name : indexNames) {
            builder.put(
                IndexMetadata.builder(name)
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                    .build(),
                false
            );
        }
        return builder.build();
    }

    private static String tablePathString(UnresolvedExternalRelation relation) {
        Object value = ((Literal) relation.tablePath()).value();
        return value instanceof BytesRef br ? BytesRefs.toString(br) : value.toString();
    }

    private static String paramValue(UnresolvedExternalRelation relation, String key) {
        Object value = relation.config().get(key);
        return value instanceof BytesRef br ? BytesRefs.toString(br) : String.valueOf(value);
    }

    @SuppressWarnings("unchecked")
    private static Object datasourceParamValue(UnresolvedExternalRelation relation, String key) {
        Object sub = relation.config().get(ExternalSourceResolver.DATASOURCE_CONFIG_KEY);
        if (sub instanceof Map<?, ?> subMap) {
            return ((Map<String, Object>) subMap).get(key);
        }
        return null;
    }
}
