/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Companion to {@link DatasetRewriterTests} that intentionally does NOT carry the
 * {@code @Before assumeTrue(ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled())} guard. Pins the
 * contract from PR #148859 that {@link DatasetRewriter#rewrite} does not read the feature flag —
 * given a {@link ProjectMetadata} that contains datasets (constructed directly via
 * {@code Builder.datasets(...)}, bypassing the CRUD gate), the rewriter must process them the same
 * way under release-tests ({@code build.snapshot=false}, flag off) as under snapshot builds.
 *
 * <p>This class is small on purpose: the heavy behavioral coverage stays in
 * {@link DatasetRewriterTests}. These tests are diagnostics — if {@link DatasetRewriterTests} ever
 * fails again in release-tests CI, the same failure here gives a smaller reproduction with
 * fewer moving parts.
 */
public class DatasetRewriterFlagAgnosticTests extends ESTestCase {

    private static final IndexNameExpressionResolver RESOLVER = TestIndexNameExpressionResolver.newInstance();

    public void testSingleDatasetRewriteRegardlessOfFlagState() {
        // Mirrors testSingleDatasetRewritesToUnresolvedExternalRelation but without the
        // @Before guard. Must pass even when ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG is off.
        DataSource parent = new DataSource("s3_parent", "test", null, Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("s3_parent", parent)))
            .datasets(Map.of("logs", dataset))
            .build();

        LogicalPlan rewritten = DatasetRewriter.rewrite(relationOf("logs"), project, RESOLVER);
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
    }

    public void testMixedFromRejectedRegardlessOfFlagState() {
        // Mirrors testClosedIndexCountsAsNonDatasetInMixedRejection's exception expectation
        // without the @Before guard. Must throw VerificationException even when the flag is off.
        DataSource parent = new DataSource("s3_parent", "test", null, Map.of());
        Dataset dataset = new Dataset("logs", new DataSourceReference("s3_parent"), "s3://logs/", null, Map.of());

        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("s3_parent", parent)))
            .datasets(Map.of("logs", dataset));
        builder.put(
            IndexMetadata.builder("my_index")
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
        ProjectMetadata project = builder.build();

        VerificationException ex = expectThrows(
            VerificationException.class,
            () -> DatasetRewriter.rewrite(relationOf("my_index,logs"), project, RESOLVER)
        );
        assertThat(ex.getMessage(), containsString("mixing datasets and non-datasets"));
    }

    public void testNoDatasetsLeavesPlanUnchangedRegardlessOfFlagState() {
        // Sanity: when no datasets are registered, the rewriter no-ops via the datasets.isEmpty()
        // early-exit. This is the production-equivalent of the flag-off state (CRUD prevents
        // datasets from landing in cluster state). Must hold under both flag states.
        UnresolvedRelation relation = relationOf("my_index");
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        assertSame(relation, DatasetRewriter.rewrite(relation, project, RESOLVER));
    }

    private static UnresolvedRelation relationOf(String pattern) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, pattern), false, List.of(), IndexMode.STANDARD, null);
    }
}
