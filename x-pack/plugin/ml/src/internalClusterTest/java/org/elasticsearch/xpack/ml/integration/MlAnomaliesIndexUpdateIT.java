/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
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

    /**
     * Creates an index named {@code name} with the given mapping source and puts read+write
     * {@code .ml-anomalies-*} aliases on it for each supplied job id, mirroring the alias
     * shape that the broken upgrade left behind.
     */
    private void createBadIndex(String name, String mappingSource, List<String> jobIds) {
        client().admin().indices().create(new CreateIndexRequest(name).mapping(mappingSource)).actionGet();

        var aliasesReq = client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        for (String jobId : jobIds) {
            aliasesReq.addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(name)
                    .alias(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
                    .isHidden(true)
            );
            aliasesReq.addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(name)
                    .alias(AnomalyDetectorsIndex.resultsWriteAlias(jobId))
                    .writeIndex(true)
                    .isHidden(true)
            );
        }
        aliasesReq.get();
    }

    /** Asserts that no {@code .ml-anomalies-*} alias lives on the given bad index. */
    private static void assertBadIndexHasNoMlAliases(String badIndex, ClusterState state) {
        Map<String, List<AliasMetadata>> aliasesMap = state.metadata()
            .getProject(Metadata.DEFAULT_PROJECT_ID)
            .findAllAliases(new String[] { badIndex });
        List<AliasMetadata> remaining = aliasesMap.getOrDefault(badIndex, List.of())
            .stream()
            .filter(a -> a.alias().startsWith(".ml-anomalies-"))
            .toList();
        assertThat("bad index [" + badIndex + "] should have no live ml-anomalies aliases after heal", remaining, is(empty()));
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
