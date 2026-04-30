/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MlAnomaliesIndexUpdate;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

/**
 * Integration tests for the reindexed-v7 heal logic in {@link MlAnomaliesIndexUpdate}.
 *
 * Each test sets up the broken post-upgrade state (a {@code .reindexed-v7-ml-anomalies-*}
 * index with {@code job_id} mapped as {@code text} and live {@code .ml-anomalies-*} aliases)
 * then drives the heal and asserts the actual cluster-state outcome: aliases moved, target
 * index exists with the correct keyword mapping, bad index is left alias-free.
 */
public class MlAnomaliesIndexUpdateIT extends MlSingleNodeTestCase {

    /**
     * job_id mapped as text — the broken dynamic-mapping shape produced by the buggy upgrade.
     * The outer {@code _doc} wrapper is required by {@link CreateIndexRequest#mapping(String)}.
     */
    private static final String BROKEN_MAPPING = """
        {"_doc":{"properties":{"job_id":{"type":"text","fields":{"keyword":{"type":"keyword"}}}}}}""";

    /**
     * job_id mapped as text for a pre-existing target index that the heal should consider
     * non-reusable, forcing it to create the next suffix instead.
     */
    private static final String TEXT_MAPPING = """
        {"_doc":{"properties":{"job_id":{"type":"text"}}}}""";

    @Before
    public void waitForTemplates() throws Exception {
        waitForMlTemplates();
    }

    public void testHeals_GivenSharedIndexWithBadMapping_HealsToNewIndex() {
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        String targetIndex = ".ml-anomalies-shared-000001";
        List<String> jobs = List.of("jobA", "jobB");

        createBadIndex(badIndex, BROKEN_MAPPING, jobs);
        runHeal();

        ClusterState state = clusterService().state();

        // Bad index still exists but has no ml-anomalies aliases
        assertBadIndexHasNoMlAliases(badIndex, state);

        // Target index was created and aliases were moved to it
        assertAliasesOnIndex(targetIndex, jobs, state);

        // The heal created the target via the ML template → job_id must be keyword
        assertJobIdIsKeyword(targetIndex, state);
    }

    public void testHeals_GivenCustomResultsIndexWithBadMapping_HealsToNewIndex() {
        String badIndex = ".reindexed-v7-ml-anomalies-custom-foo-000001";
        String expectedTarget = ".ml-anomalies-custom-foo-000001";
        List<String> jobs = List.of("fooJob");

        createBadIndex(badIndex, BROKEN_MAPPING, jobs);
        runHeal();

        ClusterState state = clusterService().state();

        assertBadIndexHasNoMlAliases(badIndex, state);
        assertAliasesOnIndex(expectedTarget, jobs, state);
    }

    public void testHeals_GivenSharedIndexWithGoodMapping_ReusesExistingIndex() {
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        String targetIndex = ".ml-anomalies-shared-000001";
        List<String> jobs = List.of("jobA");

        // Pre-create the target through the normal template — it will get keyword mapping
        client().admin().indices().create(new CreateIndexRequest(targetIndex)).actionGet();
        createBadIndex(badIndex, BROKEN_MAPPING, jobs);

        runHeal();

        ClusterState state = clusterService().state();

        // Aliases moved to the pre-existing target
        assertAliasesOnIndex(targetIndex, jobs, state);
        assertBadIndexHasNoMlAliases(badIndex, state);

        // No -000002 index was created
        assertNull(
            "heal should reuse existing target, not create a new one",
            state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(".ml-anomalies-shared-000002")
        );
    }

    /**
     * Regression guard for the cross-customer index-resolution bug: when two custom results
     * groups share a name prefix (e.g. {@code foo} and {@code foobar}), the unfiltered
     * {@code .ml-anomalies-custom-foo*} glob resolves both families, and a suffix tie in
     * {@link org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias#latestIndex} can pick the
     * wrong winner. Without strict family filtering the heal would move {@code foo}'s aliases
     * onto the unrelated {@code foobar-000001} index, corrupting both customers' results.
     */
    public void testHeals_GivenCustomPrefixSibling_DoesNotReuseUnrelatedIndex() {
        String badIndex = ".reindexed-v7-ml-anomalies-custom-foo-000001";
        String unrelatedSibling = ".ml-anomalies-custom-foobar-000001";
        String expectedTarget = ".ml-anomalies-custom-foo-000001";
        List<String> fooJobs = List.of("fooJob");

        // Pre-create the unrelated sibling via the ML template so it gets the correct
        // keyword job_id mapping — the only conditions under which the buggy resolver
        // would have happily reused it as the foo family's heal target.
        client().admin().indices().create(new CreateIndexRequest(unrelatedSibling)).actionGet();

        createBadIndex(badIndex, BROKEN_MAPPING, fooJobs);
        runHeal();

        ClusterState state = clusterService().state();

        // Heal picked the correct foo target, not the prefix-matching foobar sibling.
        assertThat(
            "foo target should have been created, not reused from unrelated sibling",
            state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(expectedTarget),
            is(notNullValue())
        );
        assertAliasesOnIndex(expectedTarget, fooJobs, state);
        assertJobIdIsKeyword(expectedTarget, state);
        assertBadIndexHasNoMlAliases(badIndex, state);

        // Unrelated sibling must be untouched — no foo-job aliases leaked onto it.
        assertNoMlAliasesOnIndex(unrelatedSibling, state);
    }

    public void testHeals_GivenSharedIndexWithSuffixCollision_PicksNextIndex() {
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        String existingBlocker = ".ml-anomalies-shared-000001";
        String expectedTarget = ".ml-anomalies-shared-000002";
        List<String> jobs = List.of("jobA");

        // Pre-create -000001 with text mapping (not keyword) so the heal considers it non-reusable.
        // The explicit request mapping for job_id takes precedence over the template mapping.
        client().admin().indices().create(new CreateIndexRequest(existingBlocker).mapping(TEXT_MAPPING)).actionGet();
        createBadIndex(badIndex, BROKEN_MAPPING, jobs);

        runHeal();

        ClusterState state = clusterService().state();

        // Heal must have picked the next suffix
        assertAliasesOnIndex(expectedTarget, jobs, state);
        assertBadIndexHasNoMlAliases(existingBlocker, state);
        assertBadIndexHasNoMlAliases(badIndex, state);
    }

    /**
     * Builds the heal updater against the real single-node cluster and runs it.
     * Uses a mock auditor to avoid async writes to the notifications index.
     */
    private void runHeal() {
        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        MlAnomaliesIndexUpdate updater = new MlAnomaliesIndexUpdate(
            TestIndexNameExpressionResolver.newInstance(),
            client(),
            auditor,
            () -> true
        );
        updater.runUpdate(clusterService().state());
    }

    /** Creates an index with the broken mapping and the canonical ML read+write aliases. */
    private void createBadIndex(String name, String mappingSource, List<String> jobIds) {
        createResultsIndexWithAliases(name, mappingSource, jobIds);
    }

    /** Asserts that no {@code .ml-anomalies-*} alias lives on the given bad index. */
    private static void assertBadIndexHasNoMlAliases(String badIndex, ClusterState state) {
        assertNoMlAliasesOnIndex(badIndex, state);
    }

    /** Asserts that no {@code .ml-anomalies-*} alias lives on the given index. */
    private static void assertNoMlAliasesOnIndex(String index, ClusterState state) {
        Map<String, List<AliasMetadata>> aliasesMap = state.metadata()
            .getProject(Metadata.DEFAULT_PROJECT_ID)
            .findAllAliases(new String[] { index });
        List<AliasMetadata> mlAliases = aliasesMap.getOrDefault(index, List.of())
            .stream()
            .filter(a -> a.alias().startsWith(".ml-anomalies-"))
            .toList();
        assertThat("index [" + index + "] should have no ml-anomalies aliases", mlAliases, is(empty()));
    }

    /**
     * Asserts that both the read alias and write alias for each job in {@code jobs}
     * are present on {@code targetIndex}.
     */
    private static void assertAliasesOnIndex(String targetIndex, List<String> jobs, ClusterState state) {
        Map<String, List<AliasMetadata>> aliasesMap = state.metadata()
            .getProject(Metadata.DEFAULT_PROJECT_ID)
            .findAllAliases(new String[] { targetIndex });
        List<AliasMetadata> targetAliases = aliasesMap.getOrDefault(targetIndex, List.of());
        for (String jobId : jobs) {
            String readAlias = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
            String writeAlias = AnomalyDetectorsIndex.resultsWriteAlias(jobId);
            boolean hasRead = targetAliases.stream().anyMatch(a -> a.alias().equals(readAlias));
            boolean hasWrite = targetAliases.stream().anyMatch(a -> a.alias().equals(writeAlias));
            assertTrue("read alias [" + readAlias + "] should be on [" + targetIndex + "]", hasRead);
            assertTrue("write alias [" + writeAlias + "] should be on [" + targetIndex + "]", hasWrite);
        }
    }

    /**
     * Asserts that the {@code job_id} field in the given index's mapping is type {@code keyword},
     * confirming the ML template was applied correctly when the target was created.
     */
    @SuppressWarnings("unchecked")
    private static void assertJobIdIsKeyword(String indexName, ClusterState state) {
        IndexMetadata meta = state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(indexName);
        assertThat("index [" + indexName + "] should exist in cluster state", meta, is(notNullValue()));
        assertNotNull("index [" + indexName + "] should have mappings", meta.mapping());

        Map<String, Object> properties = (Map<String, Object>) meta.mapping().sourceAsMap().get("properties");
        assertThat("mapping should have properties", properties, is(notNullValue()));
        assertThat("properties should contain job_id", properties, hasKey("job_id"));

        Map<String, Object> jobIdField = (Map<String, Object>) properties.get("job_id");
        assertEquals("job_id should be mapped as keyword after heal", "keyword", jobIdField.get("type"));
    }

    private ClusterService clusterService() {
        return getInstanceFromNode(ClusterService.class);
    }
}
