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
import org.elasticsearch.xpack.ml.notifications.SystemAuditor;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Integration tests for the reindexed-anomalies heal logic in {@link MlAnomaliesIndexUpdate}.
 *
 * Each test sets up the broken post-upgrade state (a {@code .reindexed-*-ml-anomalies-*}
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

    public void testHeals_GivenV7AndV8ReindexedSharedResults_DifferentJobs_HealsBothToSingleTarget() {
        String v7 = ".reindexed-v7-ml-anomalies-shared-000001";
        String v8 = ".reindexed-v8-ml-anomalies-shared";
        String targetIndex = ".ml-anomalies-shared-000001";

        createBadIndex(v7, BROKEN_MAPPING, List.of("jobA"));
        createBadIndex(v8, BROKEN_MAPPING, List.of("jobB"));

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        new MlAnomaliesIndexUpdate(
            clusterService(),
            TestIndexNameExpressionResolver.newInstance(),
            client(),
            auditor,
            systemAuditor,
            () -> true
        ).runUpdate(clusterService().state());

        ClusterState state = clusterService().state();
        assertBadIndexHasNoMlAliases(v7, state);
        assertBadIndexHasNoMlAliases(v8, state);
        assertAliasesOnIndex(targetIndex, List.of("jobA", "jobB"), state);
        assertJobIdIsKeyword(targetIndex, state);
        verify(systemAuditor, times(2)).warning(anyString());
        verify(auditor).warning(eq("jobA"), anyString());
        verify(auditor).warning(eq("jobB"), anyString());
    }

    public void testHeals_GivenReindexedV8SharedIndexWithBadMapping_HealsToNewIndex() {
        String badIndex = ".reindexed-v8-ml-anomalies-shared";
        String targetIndex = ".ml-anomalies-shared-000001";
        List<String> jobs = List.of("v8OnlyJob");

        createBadIndex(badIndex, BROKEN_MAPPING, jobs);
        runHeal();

        ClusterState state = clusterService().state();
        assertBadIndexHasNoMlAliases(badIndex, state);
        assertAliasesOnIndex(targetIndex, jobs, state);
        assertJobIdIsKeyword(targetIndex, state);
    }

    /**
     * Regression for the rollover conflict observed on ECH deployment
     * {@code 922f01fd42834c778dc547a221c83622}:
     *
     * <ul>
     *   <li>{@code .reindexed-v7-ml-anomalies-shared-000001} has the broken {@code job_id:text}
     *       mapping and is the write claimant for the {@code .ml-anomalies-.write-shared} alias.</li>
     *   <li>{@code .reindexed-v8-ml-anomalies-shared} has a healthy keyword mapping (UA's
     *       8&rarr;9 reindex applied the template correctly) but also holds the same job's
     *       read + write aliases as non-write claims (UA copies aliases during reindex).</li>
     * </ul>
     *
     * Before this fix, heal moved v7's aliases to a fresh target while leaving v8's claims
     * intact. The rollover loop then discovered v8 (matches {@code .ml-anomalies-*} via its
     * alias), created {@code .reindexed-v8-ml-anomalies-shared-000001}, and the atomic
     * alias-update tripped {@code "alias [.ml-anomalies-.write-shared] has more than one
     * write index"} because both the heal target and the new rollover destination claimed
     * the write alias.
     *
     * Post-fix, heal strips the aliases from every {@code .reindexed-*-ml-anomalies-*} index
     * (cluster-wide, scoped), so v8 is alias-free by the time the rollover loop runs and is
     * no longer a rollover candidate.
     */
    public void testHeals_GivenV7BrokenAndV8HealthyShareJobAliases_RolloverDoesNotConflict() {
        String v7 = ".reindexed-v7-ml-anomalies-shared-000001";
        String v8 = ".reindexed-v8-ml-anomalies-shared";
        String targetIndex = ".ml-anomalies-shared-000001";
        String jobId = "shared";

        // v7-000001 broken; helper sets writeIndex=true on .write-shared.
        createBadIndex(v7, BROKEN_MAPPING, List.of(jobId));

        // v8 has the template-applied (healthy keyword) mapping — no explicit mapping arg.
        client().admin().indices().create(new CreateIndexRequest(v8)).actionGet();

        // Add the same job's aliases to v8 as non-write so ES accepts the dual setup alongside
        // v7's writeIndex=true claim.
        var addV8Aliases = client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        addV8Aliases.addAliasAction(
            IndicesAliasesRequest.AliasActions.add().index(v8).alias(AnomalyDetectorsIndex.jobResultsAliasedName(jobId)).isHidden(true)
        );
        addV8Aliases.addAliasAction(
            IndicesAliasesRequest.AliasActions.add()
                .index(v8)
                .alias(AnomalyDetectorsIndex.resultsWriteAlias(jobId))
                .writeIndex(false)
                .isHidden(true)
        );
        addV8Aliases.get();

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        // runUpdate must complete without throwing — the rollover loop must not see v8 as a
        // candidate because heal evacuated its aliases cluster-wide.
        new MlAnomaliesIndexUpdate(
            clusterService(),
            TestIndexNameExpressionResolver.newInstance(),
            client(),
            auditor,
            systemAuditor,
            () -> true
        ).runUpdate(clusterService().state());

        ClusterState state = clusterService().state();
        assertBadIndexHasNoMlAliases(v7, state);
        assertBadIndexHasNoMlAliases(v8, state);
        assertAliasesOnIndex(targetIndex, List.of(jobId), state);
        assertJobIdIsKeyword(targetIndex, state);

        // Specifically: the rollover loop must NOT have created .reindexed-v8-ml-anomalies-shared-000001.
        assertNull(
            "rollover must not fire on v8 once heal has cleared its aliases",
            state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(".reindexed-v8-ml-anomalies-shared-000001")
        );
    }

    public void testHeals_RunsHealBeforeRolloverLegacyIndex_NoException() {
        // Legacy-shaped name (no 6-digit suffix) on the current index version still triggers the
        // rollover path, while a broken reindexed-v7 index triggers heal — runUpdate must complete
        // without alias conflicts (heal runs first, then rollover reads fresh cluster state).
        String legacy = ".ml-anomalies-healrollover";
        client().admin().indices().create(new CreateIndexRequest(legacy)).actionGet();

        var aliasReq = client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        aliasReq.addAliasAction(
            IndicesAliasesRequest.AliasActions.add()
                .index(legacy)
                .alias(AnomalyDetectorsIndex.jobResultsAliasedName("legacyRolloverJob"))
                .isHidden(true)
        );
        aliasReq.addAliasAction(
            IndicesAliasesRequest.AliasActions.add()
                .index(legacy)
                .alias(AnomalyDetectorsIndex.resultsWriteAlias("legacyRolloverJob"))
                .writeIndex(true)
                .isHidden(true)
        );
        aliasReq.get();

        createBadIndex(".reindexed-v7-ml-anomalies-shared-000001", BROKEN_MAPPING, List.of("healRolloverJob"));

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        new MlAnomaliesIndexUpdate(
            clusterService(),
            TestIndexNameExpressionResolver.newInstance(),
            client(),
            auditor,
            systemAuditor,
            () -> true
        ).runUpdate(clusterService().state());
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
     * Uses mock auditors to avoid async writes to the notifications index.
     */
    private void runHeal() {
        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        MlAnomaliesIndexUpdate updater = new MlAnomaliesIndexUpdate(
            clusterService(),
            TestIndexNameExpressionResolver.newInstance(),
            client(),
            auditor,
            systemAuditor,
            () -> true
        );
        updater.runUpdate(clusterService().state());
        verify(systemAuditor).warning(anyString());
        verify(auditor, atLeastOnce()).warning(any(), anyString());
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
