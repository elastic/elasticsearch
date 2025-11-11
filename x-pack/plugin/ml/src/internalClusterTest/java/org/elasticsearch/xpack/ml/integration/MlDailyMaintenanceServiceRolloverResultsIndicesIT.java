/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class MlDailyMaintenanceServiceRolloverResultsIndicesIT extends BaseMlIntegTestCase {

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
            true
        );
    }

    private void initClusterAndJob() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenNoIndices() throws Exception {
        // The null case, nothing to do.

        // set the rollover max size to 0B so we can roll the index unconditionally
        // It's not the conditions or even the rollover itself we are testing but the state of the indices and aliases afterwards.
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);
        {
            GetIndexResponse getIndexResponse = client().admin()
                .indices()
                .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                .setIndices(".ml-anomalies*")
                .get();
            logger.warn("get_index_response: {}", getIndexResponse.toString());
            assertThat(getIndexResponse.getIndices().length, is(0));
            var aliases = getIndexResponse.getAliases();
            assertThat(aliases.size(), is(0));
        }

        blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

        {
            GetIndexResponse getIndexResponse = client().admin()
                .indices()
                .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                .setIndices(".ml-anomalies*")
                .get();
            logger.warn("get_index_response: {}", getIndexResponse.toString());
            assertThat(getIndexResponse.getIndices().length, is(0));
            var aliases = getIndexResponse.getAliases();
            assertThat(aliases.size(), is(0));
        }
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenMinusOnRolloverMaxSize() throws Exception {
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
                assertThat("Context: " + context, aliases.size(), is(0));
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
        logger.warn(sb.toString().trim());
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

    private void deleteJob(String jobId) {
        try {
            client().execute(DeleteJobAction.INSTANCE, new DeleteJobAction.Request(jobId)).actionGet();
        } catch (Exception e) {
            // noop
        }
    }
}
