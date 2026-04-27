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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class DatasetRewriterTests extends ESTestCase {

    public void testNoDatasetsLeavesPlanUnchanged() {
        UnresolvedRelation relation = relationOf("my_index");
        ProjectMetadata project = projectWith(Map.of(), Map.of());
        assertSame(relation, DatasetRewriter.rewrite(relation, project));
    }

    public void testUnknownNameLeavesPlanUnchanged() {
        // Even when the cluster has datasets registered, a FROM target whose name matches neither a
        // dataset nor an index is left unchanged for the analyzer to resolve (or fail) as an index.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        UnresolvedRelation relation = relationOf("not_a_dataset_or_index");
        assertSame(relation, DatasetRewriter.rewrite(relation, project));
    }

    public void testWildcardPatternDoesNotMatchDataset() {
        // Phase 1 looks up dataset names by exact string match — wildcard patterns are not expanded.
        // Even when a dataset literally named like the glob would suggest is registered, the wildcard
        // string falls through to standard index-pattern resolution. Wildcard expansion across
        // datasets is tracked separately in elastic/esql-planning#578.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs_a", new DataSourceReference("s3_parent"), "s3://logs/a/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs_a", dataset));

        UnresolvedRelation relation = relationOf("logs_*");
        assertSame(relation, DatasetRewriter.rewrite(relation, project));
    }

    public void testWildcardMixedWithDatasetIsRejectedAsMixedFrom() {
        // `FROM <wildcard>, <literal_dataset>` falls into mixed-FROM territory because the wildcard
        // string lands in the index bucket (literal-lookup miss) while the dataset name lands in the
        // dataset bucket — Phase 1 rejection fires, identical to a literal index + dataset mix.
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("employees", new DataSourceReference("s3_parent"), "s3://emp/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("employees", dataset));

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> DatasetRewriter.rewrite(relationOf("logs_*,employees"), project)
        );
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("mixing indices and datasets"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("logs_*"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("employees"));
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

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs"), project);

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

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs"), project);
        assertThat(paramValue((UnresolvedExternalRelation) rewritten, "region"), equalTo("eu-west-2"));
    }

    public void testMultipleDatasetsProduceUnionAll() {
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset ds1 = new Dataset("ds1", new DataSourceReference("s3_parent"), "s3://a/", null, Map.of());
        Dataset ds2 = new Dataset("ds2", new DataSourceReference("s3_parent"), "s3://b/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("ds1", ds1, "ds2", ds2));

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("ds1,ds2"), project);

        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) rewritten;
        assertThat(union.children(), hasSize(2));
        assertThat(union.children().get(0), instanceOf(UnresolvedExternalRelation.class));
        assertThat(union.children().get(1), instanceOf(UnresolvedExternalRelation.class));
    }

    public void testMixedIndicesAndDatasetsRejected() {
        DataSource parent = dataSource("s3_parent", Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> DatasetRewriter.rewrite(relationOf("some_idx,logs"), project)
        );
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("mixing indices and datasets"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("indices=[some_idx]"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("datasets=[logs]"));
    }

    public void testIndexModeNonStandardRejected() {
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
                () -> DatasetRewriter.rewrite(relationOfWithMode("logs", entry.getKey()), project)
            );
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString(entry.getValue()));
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("logs"));
        }
    }

    public void testSecretSettingUnwrappedToPlaintext() {
        DataSource parent = dataSource(
            "s3_parent",
            Map.of("access_key", new DataSourceSetting("AKIAEXAMPLE", true), "region", new DataSourceSetting("us-east-1", false))
        );
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = projectWith(Map.of("s3_parent", parent), Map.of("logs", dataset));

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs"), project);
        assertThat(paramValue((UnresolvedExternalRelation) rewritten, "access_key"), equalTo("AKIAEXAMPLE"));
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
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(dataSources))
            .datasets(datasets)
            .build();
    }

    private static String tablePathString(UnresolvedExternalRelation relation) {
        Object value = ((Literal) relation.tablePath()).value();
        return value instanceof BytesRef br ? BytesRefs.toString(br) : value.toString();
    }

    private static String paramValue(UnresolvedExternalRelation relation, String key) {
        Expression expression = relation.params().get(key);
        Object value = ((Literal) expression).value();
        return value instanceof BytesRef br ? BytesRefs.toString(br) : value.toString();
    }
}
