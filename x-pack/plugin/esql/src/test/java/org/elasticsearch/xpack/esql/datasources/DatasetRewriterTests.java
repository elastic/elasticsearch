/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class DatasetRewriterTests extends ESTestCase {

    private static final IndexNameExpressionResolver RESOLVER = TestIndexNameExpressionResolver.newInstance();

    public void testNoDatasetsLeavesPlanUnchanged() {
        UnresolvedRelation relation = relationOf("my_index");
        ProjectMetadata project = projectWith(Map.of(), Map.of());
        assertSame(relation, DatasetRewriter.rewrite(relation, project, RESOLVER));
    }

    public void testUnknownNameLeavesPlanUnchanged() {
        // Even when the cluster has datasets registered, a FROM target whose name matches neither a
        // dataset nor an index is left unchanged for the analyzer to resolve (or fail) as an index.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("not_a_dataset_or_index");
        assertSame(relation, DatasetRewriter.rewrite(relation, project, RESOLVER));
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

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs"), project, RESOLVER);

        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation out = (UnresolvedExternalRelation) rewritten;
        assertThat(tablePathString(out), equalTo("s3://logs/*.parquet"));
        assertThat(paramValue(out, "region"), equalTo("us-east-1"));
        assertThat(paramValue(out, "format"), equalTo("parquet"));
    }

    public void testDatasetSettingsOverrideParentOnKeyCollision() {
        DataSource parent = dataSource("s3_parent", Map.of("region", new DataSourceSetting("us-east-1", false)));
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of("region", "eu-west-2"));
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs"), project, RESOLVER);
        assertThat(paramValue((UnresolvedExternalRelation) rewritten, "region"), equalTo("eu-west-2"));
    }

    public void testMultipleDatasetsProduceUnionAll() {
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds1 = new Dataset("ds1", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset ds2 = new Dataset("ds2", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("ds1", ds1, "ds2", ds2));

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("ds1,ds2"), project, RESOLVER);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(2));
        assertThat(union.children().get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(union.children().get(1), instanceOf(UnresolvedExternalRelation.class));
    }

    public void testMixedIndicesAndDatasetsRejected() {
        // Register a real index alongside the dataset so the resolver actually sees both abstractions.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("logs", dataset), Set.of("some_idx"));

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> DatasetRewriter.rewrite(relationOf("some_idx,logs"), project, RESOLVER)
        );
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("mixing indices and datasets"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("some_idx"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("logs"));
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
                () -> DatasetRewriter.rewrite(relationOfWithMode("logs", entry.getKey()), project, RESOLVER)
            );
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString(entry.getValue()));
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("logs"));
        }
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

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> DatasetRewriter.rewrite(relationOf("orphan_ds"), project, RESOLVER)
        );
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("dataset [orphan_ds]"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("unknown data source [missing_parent]"));
    }

    public void testSecretSettingUnwrappedToPlaintext() {
        DataSource parent = dataSource(
            "s3_parent",
            Map.of("access_key", new DataSourceSetting("AKIAEXAMPLE", true), "region", new DataSourceSetting("us-east-1", false))
        );
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs"), project, RESOLVER);
        assertThat(paramValue((UnresolvedExternalRelation) rewritten, "access_key"), equalTo("AKIAEXAMPLE"));
    }

    // ---- Pattern expansion (parity with FROM <index> patterns via IndexNameExpressionResolver) ----

    public void testWildcardMatchesDatasets() {
        // `FROM logs_*` expands through IndexNameExpressionResolver and picks up every dataset whose
        // name matches the glob — same machinery FROM <index> uses for indices and aliases.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset b = new Dataset("logs_b", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_b", b));

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs_*"), project, RESOLVER);
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
        assertSame(relation, DatasetRewriter.rewrite(relation, project, RESOLVER));
    }

    public void testWildcardSpanningIndicesAndDatasetsRejected() {
        // `FROM logs_*` matching both real indices and datasets is mixed-FROM territory — same as a
        // literal mix.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds = new Dataset("logs_dataset", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWithIndices(Map.of("s3_parent", parent), Map.of("logs_dataset", ds), Set.of("logs_index"));

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> DatasetRewriter.rewrite(relationOf("logs_*"), project, RESOLVER)
        );
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("mixing indices and datasets"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("logs_index"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("logs_dataset"));
    }

    public void testWildcardWithExclusion() {
        // `FROM logs_*, -logs_test` picks up matching datasets minus the excluded one.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset test = new Dataset("logs_test", new DataSourceReference("s3_parent"), "s3://test/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "logs_test", test));

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs_*,-logs_test"), project, RESOLVER);
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

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs_*"), project, RESOLVER);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(8));
    }

    public void testWildcardOverUnionAllCapRejectsWithUserFacingMessage() {
        // A wildcard matching more than 8 datasets crosses Fork's 8-branch cap. The rewriter
        // intercepts before constructing the UnionAll and throws a VerificationException with
        // user-facing framing — the user typed FROM <pattern>, not FORK, so the error references
        // the pattern + the cap, not Fork's internal name. Tracked as esql-planning#614 (raise the
        // cap or coalesce siblings) for the long-term fix.
        DataSource parent = dataSource("s3_parent", Map.of());
        Map<String, Dataset> datasets = new HashMap<>();
        for (int i = 0; i < 9; i++) {
            datasets.put(
                "logs_" + i,
                new Dataset("logs_" + i, new DataSourceReference("s3_parent"), "s3://logs/" + i + "/", null, Map.of())
            );
        }
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), datasets);

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> DatasetRewriter.rewrite(relationOf("logs_*"), project, RESOLVER)
        );
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("FROM [logs_*]"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("matched 9 datasets"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("current limit is 8"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("Narrow the pattern"));
    }

    public void testCommaSeparatedDatasetsAndWildcardCombine() {
        // `FROM logs_a, metrics_*` mixes a literal dataset name with a wildcard that expands to
        // additional datasets — all dataset-side, so a UnionAll of all three children.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset a = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset m1 = new Dataset("metrics_1", new DataSourceReference("s3_parent"), "s3://m1/", null, Map.of());
        Dataset m2 = new Dataset("metrics_2", new DataSourceReference("s3_parent"), "s3://m2/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", a, "metrics_1", m1, "metrics_2", m2));

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs_a,metrics_*"), project, RESOLVER);
        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(3));
        for (LogicalPlan child : union.children()) {
            assertThat(child, instanceOf(UnresolvedExternalRelation.class));
        }
    }

    // --

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
}
