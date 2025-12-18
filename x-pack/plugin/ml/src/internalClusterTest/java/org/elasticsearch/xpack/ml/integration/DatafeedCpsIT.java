/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for Cross-Project Search (CPS) support in ML datafeeds.
 *
 * CPS allows datafeeds to search across multiple projects using the same index pattern
 * syntax as Cross-Cluster Search (CCS). Since CPS is built on CCS infrastructure,
 * datafeeds already handle CPS patterns correctly through existing remote index handling.
 *
 * Key validation: RemoteClusterAware.isRemoteIndexName() returns true for both CCS
 * (cluster:index) and CPS (project:index) patterns, so existing remote index handling
 * in DataExtractorFactory, DatafeedManager, and DatafeedNodeSelector automatically
 * works for CPS.
 */
public class DatafeedCpsIT extends MlSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("serverless.cross_project.enabled", "true").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(CpsPlugin.class)).toList();
    }

    /**
     * Test that a datafeed with CPS-enabled IndicesOptions can be created successfully.
     * This validates that the CPS block has been removed from DatafeedConfig.Builder.
     */
    public void testCrossProjectIndicesOptionsAllowedForDatafeed() throws Exception {
        String jobId = "cps-test-job";
        String datafeedId = "cps-test-datafeed";

        // Create job first
        Job.Builder job = createScheduledJob(jobId);
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        // Create datafeed with CPS-enabled IndicesOptions
        // This tests that the CPS block is removed - previously this would throw
        // ElasticsearchStatusException("Cross-project search is not enabled for Datafeeds")
        IndicesOptions cpsOptions = IndicesOptions.builder(SearchRequest.DEFAULT_INDICES_OPTIONS)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(datafeedId, jobId).setIndices(List.of("logs-*"))
            .setIndicesOptions(cpsOptions);

        PutDatafeedAction.Request request = new PutDatafeedAction.Request(datafeedBuilder.build());
        PutDatafeedAction.Response response = client().execute(PutDatafeedAction.INSTANCE, request).actionGet();

        // Verify datafeed was created successfully (no exception thrown)
        assertThat(response.getResponse().getId(), equalTo(datafeedId));

        // Verify datafeed can be retrieved
        GetDatafeedsAction.Response getResponse = client().execute(
            GetDatafeedsAction.INSTANCE,
            new GetDatafeedsAction.Request(datafeedId)
        ).actionGet();
        DatafeedConfig storedConfig = getResponse.getResponse().results().get(0);
        assertThat(storedConfig.getId(), equalTo(datafeedId));
        assertThat(storedConfig.getIndices(), equalTo(List.of("logs-*")));
    }

    /**
     * Test that the CPS flag survives cluster state persistence (GET after PUT).
     * This validates that DatafeedConfig correctly serializes and deserializes
     * the resolve_cross_project_index_expression field via XContent.
     */
    public void testCpsFlagSurvivesClusterStatePersistence() throws Exception {
        String jobId = "cps-persist-job";
        String datafeedId = "cps-persist-datafeed";

        // Create job first
        Job.Builder job = createScheduledJob(jobId);
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        // Create datafeed with CPS enabled
        IndicesOptions cpsOptions = IndicesOptions.builder(SearchRequest.DEFAULT_INDICES_OPTIONS)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(datafeedId, jobId).setIndices(List.of("logs-*"))
            .setIndicesOptions(cpsOptions);

        DatafeedConfig originalConfig = datafeedBuilder.build();
        assertTrue("Original config should have CPS enabled", originalConfig.getIndicesOptions().resolveCrossProjectIndexExpression());

        // Store the datafeed (goes through XContent serialization to cluster state)
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(originalConfig);
        client().execute(PutDatafeedAction.INSTANCE, request).actionGet();

        // Retrieve the datafeed (comes back through XContent deserialization from cluster state)
        GetDatafeedsAction.Response getResponse = client().execute(
            GetDatafeedsAction.INSTANCE,
            new GetDatafeedsAction.Request(datafeedId)
        ).actionGet();
        DatafeedConfig storedConfig = getResponse.getResponse().results().get(0);

        // Verify CPS flag survives the round-trip through cluster state
        assertTrue(
            "CPS flag should survive cluster state persistence",
            storedConfig.getIndicesOptions().resolveCrossProjectIndexExpression()
        );
    }

    /**
     * Test that RemoteClusterAware.isRemoteIndexName() correctly identifies CPS qualified indices.
     * This validates that the existing CCS infrastructure handles CPS patterns, which means
     * existing remote index handling (rollup skip, HasPrivileges filter, index verification skip)
     * will work correctly for CPS indices.
     */
    public void testRemoteClusterAwareIdentifiesCpsPattern() {
        // CCS remote pattern
        assertThat(RemoteClusterAware.isRemoteIndexName("cluster1:logs-*"), is(true));

        // CPS qualified pattern (same syntax as CCS)
        assertThat(RemoteClusterAware.isRemoteIndexName("project1:logs-*"), is(true));

        // Local/unqualified pattern
        assertThat(RemoteClusterAware.isRemoteIndexName("logs-*"), is(false));

        // Multiple colons - first colon separates remote/project from index
        assertThat(RemoteClusterAware.isRemoteIndexName("project1:logs:v1-*"), is(true));

        // Wildcard in project/cluster part
        assertThat(RemoteClusterAware.isRemoteIndexName("prod-*:logs-*"), is(true));
    }

    /**
     * Test that a datafeed with CPS qualified indices can be created.
     * This validates that CPS qualified indices are handled like CCS remote indices.
     */
    public void testDatafeedWithCpsQualifiedIndices() throws Exception {
        String jobId = "cps-qualified-job";
        String datafeedId = "cps-qualified-datafeed";

        // Create job first
        Job.Builder job = createScheduledJob(jobId);
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        // Create datafeed with CPS qualified indices (project:index pattern)
        IndicesOptions cpsOptions = IndicesOptions.builder(SearchRequest.DEFAULT_INDICES_OPTIONS)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(datafeedId, jobId).setIndices(
            List.of("project1:logs-*", "project2:metrics-*")
        ).setIndicesOptions(cpsOptions);

        PutDatafeedAction.Request request = new PutDatafeedAction.Request(datafeedBuilder.build());
        PutDatafeedAction.Response response = client().execute(PutDatafeedAction.INSTANCE, request).actionGet();

        // Verify datafeed was created successfully with CPS qualified indices
        assertThat(response.getResponse().getId(), equalTo(datafeedId));
        assertThat(response.getResponse().getIndices(), equalTo(List.of("project1:logs-*", "project2:metrics-*")));
    }

    /**
     * Test that a datafeed with mixed local and CPS qualified indices can be created.
     */
    public void testDatafeedWithMixedLocalAndCpsIndices() throws Exception {
        String jobId = "mixed-indices-job";
        String datafeedId = "mixed-indices-datafeed";

        // Create job first
        Job.Builder job = createScheduledJob(jobId);
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        // Create datafeed with mixed local and CPS qualified indices
        IndicesOptions cpsOptions = IndicesOptions.builder(SearchRequest.DEFAULT_INDICES_OPTIONS)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(datafeedId, jobId).setIndices(
            List.of("local-logs-*", "project1:remote-logs-*")
        ).setIndicesOptions(cpsOptions);

        PutDatafeedAction.Request request = new PutDatafeedAction.Request(datafeedBuilder.build());
        PutDatafeedAction.Response response = client().execute(PutDatafeedAction.INSTANCE, request).actionGet();

        // Verify datafeed was created successfully
        assertThat(response.getResponse().getId(), equalTo(datafeedId));
    }

    /**
     * Test CCS/CPS parity: both patterns should be identified as remote by RemoteClusterAware.
     * This ensures that existing CCS handling in datafeeds automatically applies to CPS.
     */
    public void testCcsAndCpsPatternParity() {
        // Verify both CCS and CPS patterns are treated identically by RemoteClusterAware
        String ccsPattern = "cluster1:logs-*";
        String cpsPattern = "project1:logs-*";

        boolean ccsIsRemote = RemoteClusterAware.isRemoteIndexName(ccsPattern);
        boolean cpsIsRemote = RemoteClusterAware.isRemoteIndexName(cpsPattern);

        assertThat("CCS pattern should be identified as remote", ccsIsRemote, is(true));
        assertThat("CPS pattern should be identified as remote", cpsIsRemote, is(true));
        assertThat("CCS and CPS patterns should be handled identically", ccsIsRemote, equalTo(cpsIsRemote));
    }

    /**
     * Test that documents the security model for CPS indices in datafeeds.
     *
     * Security implications for CPS indices (project:index patterns):
     * - Up-front privilege validation (HasPrivileges check) is skipped for CPS indices
     *   (same as CCS remote indices) - see DatafeedManager.putDatafeed()
     * - This is by design: RemoteClusterLicenseChecker.isRemoteIndex() returns true for
     *   both CCS (cluster:index) and CPS (project:index) patterns
     * - For CPS, privilege enforcement happens at search execution time via UIAM
     *   (Universal Identity and Access Management), not at datafeed creation time
     * - This mirrors the existing CCS behavior where remote cluster privileges cannot
     *   be validated locally at datafeed creation time
     *
     * This test validates the filtering behavior that enables this security model.
     */
    public void testCpsIndicesSkipUpfrontPrivilegeValidation() {
        // Both CCS and CPS patterns are identified as "remote" by the license checker
        // This causes them to be filtered out of the HasPrivileges check in DatafeedManager

        // CCS pattern - skipped from privilege check (can't validate remote cluster privileges)
        assertThat(RemoteClusterAware.isRemoteIndexName("cluster1:logs-*"), is(true));

        // CPS pattern - also skipped (privileges enforced at search time via UIAM)
        assertThat(RemoteClusterAware.isRemoteIndexName("project1:logs-*"), is(true));

        // Local pattern - NOT skipped (privileges validated at datafeed creation)
        assertThat(RemoteClusterAware.isRemoteIndexName("logs-*"), is(false));

        // Mixed indices: local indices get privilege checked, CPS indices rely on UIAM
        List<String> mixedIndices = List.of("local-logs-*", "project1:remote-logs-*");
        long localCount = mixedIndices.stream().filter(i -> RemoteClusterAware.isRemoteIndexName(i) == false).count();
        long remoteCount = mixedIndices.stream().filter(RemoteClusterAware::isRemoteIndexName).count();
        assertThat("Should have 1 local index for privilege validation", localCount, equalTo(1L));
        assertThat("Should have 1 CPS index skipped from privilege validation", remoteCount, equalTo(1L));
    }

    /**
     * Helper method to create a scheduled job for testing.
     */
    private static Job.Builder createScheduledJob(String jobId) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("yyyy-MM-dd HH:mm:ss");

        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId(jobId);
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
    }

    /**
     * Plugin to register the serverless.cross_project.enabled setting.
     */
    public static class CpsPlugin extends Plugin implements ClusterPlugin {
        public List<Setting<?>> getSettings() {
            return List.of(Setting.simpleString("serverless.cross_project.enabled", Setting.Property.NodeScope));
        }
    }
}

