/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.ml.MlAssignmentNotifier;
import org.elasticsearch.xpack.ml.MlDailyMaintenanceService;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class MlDailyMaintenanceServiceRolloverIndicesIT extends BaseMlIntegTestCase {

    private MlDailyMaintenanceService maintenanceService;

    @Before
    public void createComponents() throws Exception {
        ThreadPool threadPool = mockThreadPool();

        ClusterService clusterService = internalCluster().clusterService(internalCluster().getMasterName());

        initClusterAndJob();

        maintenanceService = new MlDailyMaintenanceService(
            settings(IndexVersion.current()).build(),
            ClusterName.DEFAULT,
            threadPool,
            client(),
            clusterService,
            mock(MlAssignmentNotifier.class),
            TestIndexNameExpressionResolver.newInstance(),
            true,
            true,
            true,
            true
        );
    }

    /**
     * In production the only way to create a model snapshot is to open a job, and
     * opening a job ensures that the state index exists. This suite does not open jobs
     * but instead inserts snapshot and state documents directly to the results and
     * state indices. This means it needs to create the state index explicitly. This
     * method should not be copied to test suites that run jobs in the way they are
     * run in production.
     */
    @Before
    public void addMlState() {
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        createStateIndexAndAliasIfNecessary(
            client(),
            ClusterState.EMPTY_STATE,
            TestIndexNameExpressionResolver.newInstance(),
            TEST_REQUEST_TIMEOUT,
            future
        );
        future.actionGet();
    }

    private void initClusterAndJob() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);
    }

    public void testTriggerIndicesIfNecessaryTask_givenNoIndices() throws Exception {
        // The null case, nothing to do.

        // Delete the .ml-state-000001 index for this particular test
        DeleteIndexRequest request = new DeleteIndexRequest(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001");
        client().admin().indices().delete(request).actionGet();

        // set the rollover max size to 0B so we can roll the indices unconditionally
        // It's not the conditions or even the rollover itself we are testing but the state of the indices and aliases afterwards.
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        Map<String, Consumer<ActionListener<AcknowledgedResponse>>> params = Map.of(
            AnomalyDetectorsIndex.jobResultsIndexPattern(),
            (listener) -> maintenanceService.triggerRollResultsIndicesIfNecessaryTask(listener),
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            (listener) -> maintenanceService.triggerRollStateIndicesIfNecessaryTask(listener)
        );

        for (Map.Entry<String, Consumer<ActionListener<AcknowledgedResponse>>> param : params.entrySet()) {
            String indexPattern = param.getKey();
            Consumer<ActionListener<AcknowledgedResponse>> function = param.getValue();
            {
                GetIndexResponse getIndexResponse = client().admin()
                    .indices()
                    .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                    .setIndices(indexPattern)
                    .get();
                assertThat(getIndexResponse.toString(), getIndexResponse.getIndices().length, is(0));
                var aliases = getIndexResponse.getAliases();
                assertThat(aliases.size(), is(0));
            }

            blockingCall(function);

            {
                GetIndexResponse getIndexResponse = client().admin()
                    .indices()
                    .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                    .setIndices(indexPattern)
                    .get();
                assertThat(getIndexResponse.toString(), getIndexResponse.getIndices().length, is(0));
                var aliases = getIndexResponse.getAliases();
                assertThat(aliases.size(), is(0));
            }
        }
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenMinusOneRolloverMaxSize() throws Exception {
        // The null case, nothing to do.

        // set the rollover max size to -1B so the indices should not be rolled over
        maintenanceService.setRolloverMaxSize(ByteSizeValue.MINUS_ONE);

        // Create jobs that will use the default results indices - ".ml-anomalies-shared-*"
        Job.Builder[] jobs_with_default_index = { createJob("job_using_default_index"), createJob("another_job_using_default_index") };

        // Create jobs that will use custom results indices - ".ml-anomalies-custom-fred-*"
        Job.Builder[] jobs_with_custom_index = {
            createJob("job_using_custom_index").setResultsIndexName("fred"),
            createJob("another_job_using_custom_index").setResultsIndexName("fred") };

        runTestScenarioWithNoRolloverOccurring(jobs_with_default_index, "shared");
        runTestScenarioWithNoRolloverOccurring(jobs_with_custom_index, "custom-fred");
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenUnmetConditions() throws Exception {
        // Create jobs that will use the default results indices - ".ml-anomalies-shared-*"
        Job.Builder[] jobs_with_default_index = { createJob("job_using_default_index"), createJob("another_job_using_default_index") };

        // Create jobs that will use custom results indices - ".ml-anomalies-custom-fred-*"
        Job.Builder[] jobs_with_custom_index = {
            createJob("job_using_custom_index").setResultsIndexName("fred"),
            createJob("another_job_using_custom_index").setResultsIndexName("fred") };

        runTestScenarioWithNoRolloverOccurring(jobs_with_default_index, "shared");
        runTestScenarioWithNoRolloverOccurring(jobs_with_custom_index, "custom-fred");
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_withMixedIndexTypes() throws Exception {
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // 1. Create a job using the default shared index
        Job.Builder sharedJob = createJob("shared-job");
        putJob(sharedJob);
        assertIndicesAndAliases(
            "After shared job creation",
            AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared*",
            Map.of(
                AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000001",
                List.of(writeAlias(sharedJob.getId()), readAlias(sharedJob.getId()))
            )
        );

        // 2. Create a job using a custom index
        Job.Builder customJob = createJob("custom-job").setResultsIndexName("my-custom");
        putJob(customJob);
        assertIndicesAndAliases(
            "After custom job creation",
            AnomalyDetectorsIndex.jobResultsIndexPrefix() + "custom-my-custom*",
            Map.of(
                AnomalyDetectorsIndex.jobResultsIndexPrefix() + "custom-my-custom-000001",
                List.of(writeAlias(customJob.getId()), readAlias(customJob.getId()))
            )
        );

        // 3. Trigger a single maintenance run
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        // 4. Verify BOTH indices were rolled over correctly
        assertIndicesAndAliases(
            "After rollover (shared)",
            AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared*",
            Map.of(
                AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000001",
                List.of(readAlias(sharedJob.getId())),
                AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000002",
                List.of(writeAlias(sharedJob.getId()), readAlias(sharedJob.getId()))
            )
        );

        assertIndicesAndAliases(
            "After rollover (custom)",
            AnomalyDetectorsIndex.jobResultsIndexPrefix() + "custom-my-custom*",
            Map.of(
                AnomalyDetectorsIndex.jobResultsIndexPrefix() + "custom-my-custom-000001",
                List.of(readAlias(customJob.getId())),
                AnomalyDetectorsIndex.jobResultsIndexPrefix() + "custom-my-custom-000002",
                List.of(writeAlias(customJob.getId()), readAlias(customJob.getId()))
            )
        );
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenNoJobAliases() throws Exception {
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        String indexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000001";
        String rolledIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000002";
        String indexWildcard = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared*";

        // 1. Create an index that looks like an ML results index but has no aliases
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        client().admin().indices().create(createIndexRequest).actionGet();

        // Expect the index to exist with no aliases
        assertIndicesAndAliases("Before rollover attempt", indexWildcard, Map.of(indexName, List.of()));

        // 2. Trigger maintenance
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        // Verify that the index was rolled over, even though it had no ML aliases
        assertIndicesAndAliases("After rollover attempt", indexWildcard, Map.of(indexName, List.of(), rolledIndexName, List.of()));
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask() throws Exception {
        // replace the default set of conditions with an empty set so we can roll the index unconditionally
        // It's not the conditions or even the rollover itself we are testing but the state of the indices and aliases afterwards.
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // Create jobs that will use the default results indices - ".ml-anomalies-shared-*"
        Job.Builder[] jobs_with_default_index = { createJob("job_using_default_index"), createJob("another_job_using_default_index") };

        // Create jobs that will use custom results indices - ".ml-anomalies-custom-fred-*"
        Job.Builder[] jobs_with_custom_index = {
            createJob("job_using_custom_index").setResultsIndexName("fred"),
            createJob("another_job_using_custom_index").setResultsIndexName("fred") };

        runTestScenario(jobs_with_default_index, "shared");
        runTestScenario(jobs_with_custom_index, "custom-fred");
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_withMissingReadAlias() throws Exception {
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        String jobId = "job-with-missing-read-alias";
        Job.Builder job = createJob(jobId);
        putJob(job);

        String indexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000001";
        String rolledIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000002";
        String indexWildcard = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared*";

        // 1. Manually remove the read alias to create an inconsistent state
        client().admin()
            .indices()
            .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .removeAlias(indexName, readAlias(jobId))
            .get();

        assertIndicesAndAliases("Before rollover (missing read alias)", indexWildcard, Map.of(indexName, List.of(writeAlias(jobId))));

        // 2. Trigger maintenance
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        // 3. Verify the index rolled over and the aliases were healed on the new index
        assertIndicesAndAliases(
            "After rollover (missing read alias)",
            indexWildcard,
            Map.of(
                indexName,
                List.of(readAlias(jobId)), // Old index should now have read alias
                rolledIndexName,
                List.of(writeAlias(jobId), readAlias(jobId)) // New index has both aliases
            )
        );
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_withOrphanedReadAlias() throws Exception {
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        String jobId = "job-with-orphaned-read-alias";
        Job.Builder job = createJob(jobId);
        putJob(job);

        String indexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000001";
        String rolledIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000002";
        String indexWildcard = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared*";

        // 1. Manually remove the write alias to create an inconsistent state
        client().admin()
            .indices()
            .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .removeAlias(indexName, writeAlias(jobId))
            .get();

        assertIndicesAndAliases("Before rollover (orphaned read alias)", indexWildcard, Map.of(indexName, List.of(readAlias(jobId))));

        // 2. Trigger maintenance
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        // 3. Verify the index rolled over and the aliases were healed on the new index
        assertIndicesAndAliases(
            "After rollover (orphaned read alias)",
            indexWildcard,
            Map.of(
                indexName,
                List.of(readAlias(jobId)), // The orphaned read alias remains on the old index
                rolledIndexName,
                List.of(writeAlias(jobId), readAlias(jobId)) // New index has a full set of correct aliases
            )
        );
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenWriteAliasOnMultipleIndices() throws Exception {
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        String jobId = "job-with-duplicate-write-alias";
        Job.Builder job = createJob(jobId);
        putJob(job);

        String indexName1 = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000001";
        String indexName2 = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000002";
        String indexName3 = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000003";
        String indexWildcard = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared*";

        // 1. Create a second index and add the same write alias to it, creating an inconsistent state
        createIndex(indexName2);
        client().admin()
            .indices()
            .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName2).alias(writeAlias(jobId)).isHidden(true))
            .get();

        assertIndicesAndAliases(
            "Before rollover (duplicate write alias)",
            indexWildcard,
            Map.of(indexName1, List.of(writeAlias(jobId), readAlias(jobId)), indexName2, List.of(writeAlias(jobId)))
        );

        // 2. Trigger maintenance and expect it to fail because the rollover alias is not unique
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        // 3. Verify that the state has not changed
        assertIndicesAndAliases(
            "After failed rollover (duplicate write alias)",
            indexWildcard,
            Map.of(
                indexName1,
                List.of(readAlias(jobId)),
                indexName2,
                List.of(readAlias(jobId)),
                indexName3,
                List.of(writeAlias(jobId), readAlias(jobId))
            )
        );
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenWriteAliasOnWrongIndex() throws Exception {
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        String jobId = "job-with-misplaced-write-alias";
        Job.Builder job = createJob(jobId);
        putJob(job);

        String indexName1 = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000001";
        String indexName2 = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000002";
        String indexName3 = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared-000003";
        String indexWildcard = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared*";

        // 1. Create a second, newer index, leaving the write alias on the old one
        createIndex(indexName2);

        assertIndicesAndAliases(
            "Before rollover (misplaced write alias)",
            indexWildcard,
            Map.of(indexName1, List.of(writeAlias(jobId), readAlias(jobId)), indexName2, List.of())
        );

        // 2. Trigger a maintenance run and expect it to gracefully repair the wrongly seated write alias
        // because the write alias points to indexName1.
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        // 3. Verify that the job aliases are now in a healthy state
        assertIndicesAndAliases(
            "After rollover (misplaced write alias)",
            indexWildcard,
            Map.of(
                indexName1,
                List.of(readAlias(jobId)),
                indexName2,
                List.of(readAlias(jobId)),
                indexName3,
                List.of(writeAlias(jobId), readAlias(jobId))
            )
        );
    }

    public void testTriggerRollStateIndicesIfNecessaryTask() throws Exception {
        // 1. Ensure that rollover tasks will always execute
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // 2. Check the state index exists and has the expected write alias
        assertIndicesAndAliases(
            "Before rollover (state)",
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            Map.of(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001", List.of(AnomalyDetectorsIndex.jobStateIndexWriteAlias()))
        );

        // 3. Trigger a single maintenance run
        blockingCall(maintenanceService::triggerRollStateIndicesIfNecessaryTask);

        // 4. Verify state index was rolled over correctly
        assertIndicesAndAliases(
            "After rollover (state)",
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            Map.of(
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001",
                List.of(),
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000002",
                List.of(AnomalyDetectorsIndex.jobStateIndexWriteAlias())
            )
        );

        // 5. Trigger another maintenance run
        blockingCall(maintenanceService::triggerRollStateIndicesIfNecessaryTask);

        // 6. Verify state index was rolled over correctly
        assertIndicesAndAliases(
            "After rollover (state)",
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            Map.of(
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001",
                List.of(),
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000002",
                List.of(),
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000003",
                List.of(AnomalyDetectorsIndex.jobStateIndexWriteAlias())
            )
        );
    }

    public void testTriggerRollStateIndicesIfNecessaryTask_givenMinusOneRolloverMaxSize() throws Exception {
        // The null case, nothing to do.

        // set the rollover max size to -1B so the indices should not be rolled over
        maintenanceService.setRolloverMaxSize(ByteSizeValue.MINUS_ONE);
        {
            GetIndexResponse getIndexResponse = client().admin()
                .indices()
                .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                .setIndices(AnomalyDetectorsIndex.jobStateIndexPattern())
                .get();
            logger.warn("get_index_response: {}", getIndexResponse.toString());
            assertIndicesAndAliases(
                "Before rollover (state)",
                AnomalyDetectorsIndex.jobStateIndexPattern(),
                Map.of(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001", List.of(AnomalyDetectorsIndex.jobStateIndexWriteAlias()))
            );
        }

        blockingCall(maintenanceService::triggerRollStateIndicesIfNecessaryTask);

        {
            assertIndicesAndAliases(
                "After rollover (state)",
                AnomalyDetectorsIndex.jobStateIndexPattern(),
                Map.of(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001", List.of(AnomalyDetectorsIndex.jobStateIndexWriteAlias()))
            );
        }
    }

    public void testTriggerRollStateIndicesIfNecessaryTask_givenMissingWriteAlias() throws Exception {
        // 1. Ensure that rollover tasks will always attempt to execute
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // 2. Remove the write alias to create an inconsistent state
        client().admin()
            .indices()
            .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .removeAlias(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001", AnomalyDetectorsIndex.jobStateIndexWriteAlias())
            .get();

        assertIndicesAndAliases(
            "Before rollover (state, missing alias)",
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            Map.of(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001", List.of())
        );

        // 3. Trigger a maintenance run and expect it to gracefully handle the missing write alias
        blockingCall(maintenanceService::triggerRollStateIndicesIfNecessaryTask);

        // 4. Verify the index rolled over correctly and the write alias was added
        assertIndicesAndAliases(
            "After rollover (state, missing alias)",
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            Map.of(
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001",
                List.of(),
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000002",
                List.of(AnomalyDetectorsIndex.jobStateIndexWriteAlias())
            )
        );
    }

    public void testTriggerRollStateIndicesIfNecessaryTask_givenWriteAliasOnWrongIndex() throws Exception {
        // 1. Ensure that rollover tasks will always attempt to execute
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // 2. Create a second, newer state index
        createIndex(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000002");

        // 3. Verify the initial state (write alias is on the older index)
        assertIndicesAndAliases(
            "Before rollover (state, alias on wrong index)",
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            Map.of(
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001",
                List.of(AnomalyDetectorsIndex.jobStateIndexWriteAlias()),
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000002",
                List.of()
            )
        );

        // 4. The service finds .ml-state-000002 as the latest, but the rollover alias points to ...000001
        // Trigger a maintenance run and expect it to gracefully repair the wrongly seated write alias
        blockingCall(maintenanceService::triggerRollStateIndicesIfNecessaryTask);

        // 5. Verify the index rolled over correctly and the write alias was moved to the latest index
        assertIndicesAndAliases(
            "After rollover (state, alias on wrong index)",
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            Map.of(
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001",
                List.of(),
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000002",
                List.of(),
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000003",
                List.of(AnomalyDetectorsIndex.jobStateIndexWriteAlias())
            )
        );
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenIndexWithIlmPolicy() throws Exception {
        // Delete the pre-existing .ml-state-000001 index for this particular test
        // We create it anew with an ILM policy attached
        DeleteIndexRequest request = new DeleteIndexRequest(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001");
        client().admin().indices().delete(request).actionGet();

        // Set the rollover max size to 0 so that the ML maintenance service would normally roll over the index
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // 1. Create an ILM policy, it doesn't matter exactly what it is for the purpose of this test
        String policyName = "test-ilm-policy";
        Map<String, Phase> phases = Map.of(
            "delete",
            new Phase("delete", TimeValue.ZERO, Map.of(DeleteAction.NAME, DeleteAction.NO_SNAPSHOT_DELETE))
        );
        LifecyclePolicy policy = new LifecyclePolicy(policyName, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, policy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).actionGet());

        // 2. Create an index with the ILM policy applied
        String indexName = AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Map.of(
                "index.number_of_shards",
                1,
                "index.number_of_replicas",
                0,
                "index.lifecycle.name",
                policyName,
                "index.lifecycle.rollover_alias",
                "dummy-rollover-alias"
            )
        );
        client().admin().indices().create(createIndexRequest).actionGet();

        assertIndicesAndAliases("Before rollover attempt (with ILM)", indexName, Map.of(indexName, List.of()));

        // 3. Trigger maintenance
        blockingCall(maintenanceService::triggerRollStateIndicesIfNecessaryTask);

        // 4. Verify that no new index was created, as ILM-managed indices should be ignored
        assertIndicesAndAliases("After rollover attempt (with ILM)", indexName, Map.of(indexName, List.of()));
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_whenIlmIsDisabledInMl() throws Exception {
        // 1. Create a new maintenance service with ILM disabled
        ThreadPool threadPool = mockThreadPool();
        ClusterService clusterService = internalCluster().clusterService(internalCluster().getMasterName());
        MlDailyMaintenanceService ilmDisabledService = new MlDailyMaintenanceService(
            settings(IndexVersion.current()).build(),
            ClusterName.DEFAULT,
            threadPool,
            client(),
            clusterService,
            mock(MlAssignmentNotifier.class),
            TestIndexNameExpressionResolver.newInstance(),
            true,
            true,
            true,
            false // isIlmEnabled = false
        );
        ilmDisabledService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // 2. Create an ILM policy and an index that uses it
        String policyName = "test-ilm-policy-for-disabled-test";
        Map<String, Phase> phases = Map.of(
            "delete",
            new Phase("delete", TimeValue.ZERO, Map.of(DeleteAction.NAME, DeleteAction.NO_SNAPSHOT_DELETE))
        );

        LifecyclePolicy policy = new LifecyclePolicy(policyName, phases);

        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, policy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).actionGet());

        String indexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "ilm-disabled-test-000001";
        String rolledIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "ilm-disabled-test-000002";
        createIndex(
            indexName,
            Settings.builder().put("index.lifecycle.name", policyName).put("index.lifecycle.rollover_alias", "dummy-alias").build()
        );

        assertIndicesAndAliases("Before rollover (ILM disabled)", indexName, Map.of(indexName, List.of()));

        // 3. Trigger maintenance on the service where ILM is disabled
        blockingCall(ilmDisabledService::triggerRollResultsIndicesIfNecessaryTask);

        // 4. Verify that a rollover DID occur, because the service's isIlmEnabled flag was false
        assertIndicesAndAliases(
            "After rollover (ILM disabled)",
            indexName.replace("000001", "*"),
            Map.of(indexName, List.of(), rolledIndexName, List.of())
        );
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenIndexWithEmptyIlmPolicySetting() throws Exception {
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // 1. Create an index with an empty "index.lifecycle.name" setting
        String indexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "empty-ilm-policy-000001";
        String rolledIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "empty-ilm-policy-000002";
        createIndex(indexName, Settings.builder().put("index.lifecycle.name", "").build());

        assertIndicesAndAliases("Before rollover (empty ILM setting)", indexName, Map.of(indexName, List.of()));

        // 2. Trigger maintenance
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        // 3. Verify that a rollover DID occur, because an empty policy name means ILM is not active
        assertIndicesAndAliases(
            "After rollover (empty ILM setting)",
            indexName.replace("000001", "*"),
            Map.of(indexName, List.of(), rolledIndexName, List.of())
        );
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenMixedGroupWithLatestIndexOnIlm() throws Exception {
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);

        // 1. Create an ILM policy
        String policyName = "test-ilm-policy-for-mixed-group";
        Map<String, Phase> phases = Map.of(
            "delete",
            new Phase("delete", TimeValue.ZERO, Map.of(DeleteAction.NAME, DeleteAction.NO_SNAPSHOT_DELETE))
        );
        LifecyclePolicy policy = new LifecyclePolicy(policyName, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, policy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).actionGet());

        // 2. Create a group of indices where the LATEST one is managed by ILM
        String indexName1 = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "mixed-group-000001";
        String indexName2 = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "mixed-group-000002";
        createIndex(indexName1);
        createIndex(
            indexName2,
            Settings.builder().put("index.lifecycle.name", policyName).put("index.lifecycle.rollover_alias", "dummy-alias").build()
        );

        String indexWildcard = indexName1.replace("000001", "*");
        assertIndicesAndAliases("Before rollover (mixed group)", indexWildcard, Map.of(indexName1, List.of(), indexName2, List.of()));

        // 3. Trigger maintenance
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        // 4. Verify that NO rollover occurred, because the latest index in the group is ILM-managed
        GetIndexResponse finalIndexResponse = client().admin()
            .indices()
            .prepareGetIndex(TimeValue.THIRTY_SECONDS)
            .setIndices(indexWildcard)
            .get();
        assertThat(finalIndexResponse.getIndices().length, is(2)); // No new index should be created
    }

    private void runTestScenarioWithNoRolloverOccurring(Job.Builder[] jobs, String indexNamePart) throws Exception {
        String firstJobId = jobs[0].getId();
        String secondJobId = jobs[1].getId();
        String indexWildcard = AnomalyDetectorsIndex.jobResultsIndexPrefix() + indexNamePart + "*";
        String firstIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + indexNamePart + "-000001";

        // 1. Create the first job, which creates the first index and aliases
        putJob(jobs[0]);
        assertIndicesAndAliases(
            "Before first rollover attempt",
            indexWildcard,
            Map.of(firstIndexName, List.of(writeAlias(firstJobId), readAlias(firstJobId)))
        );

        // 2. Trigger the first rollover attempt
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);
        assertIndicesAndAliases(
            "After first rollover attempt",
            indexWildcard,
            Map.of(firstIndexName, List.of(writeAlias(firstJobId), readAlias(firstJobId)))
        );

        // 3. Create the second job, which adds its aliases to the current write index
        putJob(jobs[1]);
        assertIndicesAndAliases(
            "After second job creation",
            indexWildcard,
            Map.of(firstIndexName, List.of(writeAlias(firstJobId), readAlias(firstJobId), writeAlias(secondJobId), readAlias(secondJobId)))
        );

        // 4. Trigger the second rollover attempt
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);
        assertIndicesAndAliases(
            "After second job creation",
            indexWildcard,
            Map.of(firstIndexName, List.of(writeAlias(firstJobId), readAlias(firstJobId), writeAlias(secondJobId), readAlias(secondJobId)))
        );
    }

    private void runTestScenario(Job.Builder[] jobs, String indexNamePart) throws Exception {
        String firstJobId = jobs[0].getId();
        String secondJobId = jobs[1].getId();
        String indexWildcard = AnomalyDetectorsIndex.jobResultsIndexPrefix() + indexNamePart + "*";
        String firstIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + indexNamePart + "-000001";
        String secondIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + indexNamePart + "-000002";
        String thirdIndexName = AnomalyDetectorsIndex.jobResultsIndexPrefix() + indexNamePart + "-000003";

        // 1. Create the first job, which creates the first index and aliases
        putJob(jobs[0]);
        assertIndicesAndAliases(
            "Before first rollover",
            indexWildcard,
            Map.of(firstIndexName, List.of(writeAlias(firstJobId), readAlias(firstJobId)))
        );

        // 2. Trigger the first rollover
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);
        assertIndicesAndAliases(
            "After first rollover",
            indexWildcard,
            Map.of(firstIndexName, List.of(readAlias(firstJobId)), secondIndexName, List.of(writeAlias(firstJobId), readAlias(firstJobId)))
        );

        // 3. Create the second job, which adds its aliases to the current write index
        putJob(jobs[1]);
        assertIndicesAndAliases(
            "After second job creation",
            indexWildcard,
            Map.of(
                firstIndexName,
                List.of(readAlias(firstJobId)),
                secondIndexName,
                List.of(writeAlias(firstJobId), readAlias(firstJobId), writeAlias(secondJobId), readAlias(secondJobId))
            )
        );

        // 4. Trigger the second rollover
        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);
        assertIndicesAndAliases(
            "After second rollover",
            indexWildcard,
            Map.of(
                firstIndexName,
                List.of(readAlias(firstJobId)),
                secondIndexName,
                List.of(readAlias(firstJobId), readAlias(secondJobId)),
                thirdIndexName,
                List.of(writeAlias(firstJobId), readAlias(firstJobId), writeAlias(secondJobId), readAlias(secondJobId))
            )
        );
    }

    private void assertIndicesAndAliases(String context, String indexWildcard, Map<String, List<String>> expectedAliases) {
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices(indexWildcard)
            .get();

        var indices = Arrays.asList(getIndexResponse.getIndices());
        assertThat("Context: " + context, indices.size(), is(expectedAliases.size()));
        assertThat("Index mismatch. Context: " + context, indices, containsInAnyOrder(expectedAliases.keySet().toArray(String[]::new)));

        var aliases = getIndexResponse.getAliases();

        StringBuilder sb = new StringBuilder(context).append(". Aliases found:\n");

        expectedAliases.forEach((indexName, expectedAliasList) -> {
            assertThat("Context: " + context, indices.size(), is(expectedAliases.size()));
            if (expectedAliasList.isEmpty()) {
                List<AliasMetadata> actualAliasMetadata = aliases.get(indexName);
                assertThat("Context: " + context, actualAliasMetadata, is(nullValue()));
            } else {
                List<AliasMetadata> actualAliasMetadata = aliases.get(indexName);
                List<String> actualAliasList = actualAliasMetadata.stream().map(AliasMetadata::alias).toList();
                assertThat(
                    "Alias mismatch for index [" + indexName + "]. Context: " + context,
                    actualAliasList,
                    containsInAnyOrder(expectedAliasList.toArray(String[]::new))
                );
                sb.append("  Index [").append(indexName).append("]: ").append(actualAliasList).append("\n");
            }
        });
        logger.info(sb.toString().trim());
    }

    private String readAlias(String jobId) {
        return AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
    }

    private String writeAlias(String jobId) {
        return AnomalyDetectorsIndex.resultsWriteAlias(jobId);
    }

    private <T> void blockingCall(Consumer<ActionListener<T>> function) throws InterruptedException {
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> latch.countDown(), e -> {
            exceptionHolder.set(e);
            latch.countDown();
        });
        function.accept(listener);
        latch.await();
        if (exceptionHolder.get() != null) {
            fail(exceptionHolder.get().getMessage());
        }
    }

    private PutJobAction.Response putJob(Job.Builder job) {
        PutJobAction.Request request = new PutJobAction.Request(job);
        return client().execute(PutJobAction.INSTANCE, request).actionGet();
    }
}
