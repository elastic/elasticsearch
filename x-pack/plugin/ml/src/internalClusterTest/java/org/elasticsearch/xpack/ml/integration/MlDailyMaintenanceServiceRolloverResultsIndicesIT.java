/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MlAssignmentNotifier;
import org.elasticsearch.xpack.ml.MlDailyMaintenanceService;

import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class MlDailyMaintenanceServiceRolloverResultsIndicesIT extends BaseMlIntegTestCase {

    private MlDailyMaintenanceService maintenanceService;

    @Before
    public void createComponents() throws Exception {
        Settings settings = nodeSettings(0, Settings.EMPTY);
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

        // replace the default set of conditions with an empty set so we can roll the index unconditionally
        // It's not the conditions or even the rollover itself we are testing but the state of the indices and aliases afterwards.
        maintenanceService.setRolloverConditions(RolloverConditions.newBuilder().build());
    }

    private void initClusterAndJob() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);
    }

    public void testTriggerRollResultsIndicesIfNecessaryTask_givenNoIndices() throws Exception {
        // The null case, nothing to do.

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

    public void testTriggerRollResultsIndicesIfNecessaryTask() throws Exception {

        // Create jobs that will use the default results indices - ".ml-anomalies-shared-*"
        Job.Builder[] jobs_with_default_index = { createJob("job_using_default_index"), createJob("another_job_using_default_index") };

        // Create jobs that will use custom results indices - ".ml-anomalies-custom-fred-*"
        Job.Builder[] jobs_with_custom_index = {
            createJob("job_using_custom_index").setResultsIndexName("fred"),
            createJob("another_job_using_custom_index").setResultsIndexName("fred") };

        Job.Builder[][] job_lists = { jobs_with_default_index, jobs_with_custom_index };

        for (Job.Builder[] job_list : job_lists) {
            putJob(job_list[0]);
            String jobId = job_list[0].getId();
            String index = (jobId.contains("job_using_custom_index")) ? "custom-fred" : "shared";
            {
                GetIndexResponse getIndexResponse = client().admin()
                    .indices()
                    .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                    .setIndices(".ml-anomalies-" + index + "*")
                    .get();
                logger.warn("get_index_response: {}", getIndexResponse.toString());
                assertThat(getIndexResponse.getIndices().length, is(1));
                var aliases = getIndexResponse.getAliases();
                assertThat(aliases.size(), is(1));

                StringBuilder sb = new StringBuilder("Before Rollover. Aliases found:\n");

                List<AliasMetadata> aliasMetadata = aliases.get(".ml-anomalies-" + index + "-000001");

                assertThat(aliasMetadata.size(), is(2));

                List<String> aliasesList = new ArrayList<>(aliasMetadata.stream().map(AliasMetadata::alias).toList());

                assertThat(aliasesList, containsInAnyOrder(".ml-anomalies-.write-" + jobId, ".ml-anomalies-" + jobId));

                sb.append("  Index [").append(".ml-anomalies-shared-000001").append("]: ").append(aliasesList).append("\n");

                logger.warn(sb.toString().trim());
            }

            blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

            // Check indices and aliases after rollover, there should be a read alias for ".ml-anomalies-<index>-000001"
            // and read/write aliases for ".ml-anomalies-<index>-000002". There should be no other aliases pointing to these indices.
            {
                GetIndexResponse getIndexResponse = client().admin()
                    .indices()
                    .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                    .setIndices(".ml-anomalies-" + index + "*")
                    .get();
                logger.warn("get_index_response: {}", getIndexResponse.toString());
                assertThat(getIndexResponse.getIndices().length, is(2));
                var aliases = getIndexResponse.getAliases();
                assertThat(aliases.size(), is(2));

                StringBuilder sb = new StringBuilder("After Rollover. Aliases found:\n");
                List<AliasMetadata> aliasMetadata1 = aliases.get(".ml-anomalies-" + index + "-000001");
                List<AliasMetadata> aliasMetadata2 = aliases.get(".ml-anomalies-" + index + "-000002");

                assertThat(aliasMetadata1.size(), is(1));
                assertThat(aliasMetadata2.size(), is(2));

                List<String> aliases1List = new ArrayList<>(aliasMetadata1.stream().map(AliasMetadata::alias).toList());
                List<String> aliases2List = new ArrayList<>(aliasMetadata2.stream().map(AliasMetadata::alias).toList());

                assertThat(aliases1List, containsInAnyOrder(".ml-anomalies-" + jobId));
                assertThat(aliases2List, containsInAnyOrder(".ml-anomalies-.write-" + jobId, ".ml-anomalies-" + jobId));

                sb.append("  Index [")
                    .append(".ml-anomalies-")
                    .append(index)
                    .append("-000001")
                    .append("]: ")
                    .append(aliases1List)
                    .append("\n");
                sb.append("  Index [")
                    .append(".ml-anomalies-")
                    .append(index)
                    .append("-000002")
                    .append("]: ")
                    .append(aliases2List)
                    .append("\n");

                logger.warn(sb.toString().trim());
            }

            // Now open another job.
            putJob(job_list[1]);

            // Check indices and aliases, there should be new read/write aliases for the new job
            // pointing to ".ml-anomalies-<index>-000002".
            {
                GetIndexResponse getIndexResponse = client().admin()
                    .indices()
                    .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                    .setIndices(".ml-anomalies-" + index + "*")
                    .get();
                logger.warn("get_index_response: {}", getIndexResponse.toString());
                assertThat(getIndexResponse.getIndices().length, is(2));
                var aliases = getIndexResponse.getAliases();
                assertThat(aliases.size(), is(2));

                StringBuilder sb = new StringBuilder("After 2nd Job creation. Aliases found:\n");
                List<AliasMetadata> aliasMetadata1 = aliases.get(".ml-anomalies-" + index + "-000001");
                List<AliasMetadata> aliasMetadata2 = aliases.get(".ml-anomalies-" + index + "-000002");

                assertThat(aliasMetadata1.size(), is(1));
                assertThat(aliasMetadata2.size(), is(4));

                List<String> aliases1List = new ArrayList<>(aliasMetadata1.stream().map(AliasMetadata::alias).toList());
                List<String> aliases2List = new ArrayList<>(aliasMetadata2.stream().map(AliasMetadata::alias).toList());

                assertThat(aliases1List, containsInAnyOrder(".ml-anomalies-" + jobId));
                assertThat(
                    aliases2List,
                    containsInAnyOrder(
                        ".ml-anomalies-.write-" + jobId,
                        ".ml-anomalies-" + jobId,
                        ".ml-anomalies-.write-another_" + jobId,
                        ".ml-anomalies-another_" + jobId
                    )
                );

                sb.append("  Index [")
                    .append(".ml-anomalies-")
                    .append(index)
                    .append("-000001")
                    .append("]: ")
                    .append(aliases1List)
                    .append("\n");
                sb.append("  Index [")
                    .append(".ml-anomalies-")
                    .append(index)
                    .append("-000002")
                    .append("]: ")
                    .append(aliases2List)
                    .append("\n");

                logger.warn(sb.toString().trim());
            }

            // Now trigger another rollover event
            blockingCall(maintenanceService::triggerRollResultsIndicesIfNecessaryTask);

            // Check indices and aliases, there should be a new index ".ml-anomalies-<index>-000003",
            // with read/write aliases for both jobs pointing to it. There should be read aliases for
            // both jobs pointing to ".ml-anomalies-<index>-000002" and a read alias for the initial job
            // pointing to ".ml-anomalies-<index>-000001" and no other aliases referencing any of the 3 indices.
            {
                GetIndexResponse getIndexResponse = client().admin()
                    .indices()
                    .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                    .setIndices(".ml-anomalies-" + index + "*")
                    .get();
                logger.warn("get_index_response: {}", getIndexResponse.toString());
                assertThat(getIndexResponse.getIndices().length, is(3));
                var aliases = getIndexResponse.getAliases();
                assertThat(aliases.size(), is(3));

                StringBuilder sb = new StringBuilder("After 2nd Rollover. Aliases found:\n");
                List<AliasMetadata> aliasMetadata1 = aliases.get(".ml-anomalies-" + index + "-000001");
                List<AliasMetadata> aliasMetadata2 = aliases.get(".ml-anomalies-" + index + "-000002");
                List<AliasMetadata> aliasMetadata3 = aliases.get(".ml-anomalies-" + index + "-000003");

                assertThat(aliasMetadata1.size(), is(1));
                assertThat(aliasMetadata2.size(), is(2));
                assertThat(aliasMetadata3.size(), is(4));

                List<String> aliases1List = new ArrayList<>(aliasMetadata1.stream().map(AliasMetadata::alias).toList());
                List<String> aliases2List = new ArrayList<>(aliasMetadata2.stream().map(AliasMetadata::alias).toList());
                List<String> aliases3List = new ArrayList<>(aliasMetadata3.stream().map(AliasMetadata::alias).toList());

                assertThat(aliases1List, containsInAnyOrder(".ml-anomalies-" + jobId));
                assertThat(aliases2List, containsInAnyOrder(".ml-anomalies-" + jobId, ".ml-anomalies-another_" + jobId));
                assertThat(
                    aliases3List,
                    containsInAnyOrder(
                        ".ml-anomalies-.write-" + jobId,
                        ".ml-anomalies-" + jobId,
                        ".ml-anomalies-.write-another_" + jobId,
                        ".ml-anomalies-another_" + jobId
                    )
                );

                sb.append("  Index [")
                    .append(".ml-anomalies-")
                    .append(index)
                    .append("-000001")
                    .append("]: ")
                    .append(aliases1List)
                    .append("\n");
                sb.append("  Index [")
                    .append(".ml-anomalies-")
                    .append(index)
                    .append("-000002")
                    .append("]: ")
                    .append(aliases2List)
                    .append("\n");
                sb.append("  Index [")
                    .append(".ml-anomalies-")
                    .append(index)
                    .append("-000003")
                    .append("]: ")
                    .append(aliases3List)
                    .append("\n");

                logger.warn(sb.toString().trim());
            }
        }
    }

    private <T> void blockingCall(Consumer<ActionListener<T>> function) throws InterruptedException {
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> { latch.countDown(); }, e -> {
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
