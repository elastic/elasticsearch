/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.MlUpgradeAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.ml.ResultsIndexUpgradeService;
import org.junit.After;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeedBuilder;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.indexDocs;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class MlUpgradeIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanup() throws Exception {
        cleanUp();
    }

    public void testMigrationWhenItIsNotNecessary() throws Exception {
        String jobId1 = "no-migration-test1";
        String jobId2 = "no-migration-test2";
        String jobId3 = "no-migration-test3";

        String dataIndex = createDataIndex().v2();
        List<Job> jobs = createJobsWithData(jobId1, jobId2, jobId3, dataIndex);
        Job job1 = jobs.get(0);
        Job job2 = jobs.get(1);
        Job job3 = jobs.get(2);

        String job1Index = job1.getResultsIndexName();
        String job2Index = job2.getResultsIndexName();
        String job3Index = job3.getResultsIndexName();

        assertThat(indexExists(job1Index), is(true));
        assertThat(indexExists(job2Index), is(true));
        assertThat(indexExists(job3Index), is(true));

        long job1Total = getTotalDocCount(job1Index);
        long job2Total = getTotalDocCount(job2Index);
        long job3Total = getTotalDocCount(job3Index);

        AcknowledgedResponse resp = ESIntegTestCase.client().execute(MlUpgradeAction.INSTANCE,
            new MlUpgradeAction.Request()).actionGet();
        assertThat(resp.isAcknowledged(), is(true));

        // Migration should have done nothing
        assertThat(indexExists(job1Index), is(true));
        assertThat(indexExists(job2Index), is(true));
        assertThat(indexExists(job3Index), is(true));

        assertThat(getTotalDocCount(job1Index), equalTo(job1Total));
        assertThat(getTotalDocCount(job2Index), equalTo(job2Total));
        assertThat(getTotalDocCount(job3Index), equalTo(job3Total));

        ClusterState state = admin().cluster().state(new ClusterStateRequest()).actionGet().getState();
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
        String[] indices = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.strictExpandOpenAndForbidClosed(),
            AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*");

        // Our backing index size should be two as we have a shared and custom index
        assertThat(indices.length, equalTo(2));
    }

    public void testMigration() throws Exception {
        String jobId1 = "migration-test1";
        String jobId2 = "migration-test2";
        String jobId3 = "migration-test3";

        String dataIndex = createDataIndex().v2();
        List<Job> jobs = createJobsWithData(jobId1, jobId2, jobId3, dataIndex);
        Job job1 = jobs.get(0);
        Job job2 = jobs.get(1);
        Job job3 = jobs.get(2);

        String job1Index = job1.getResultsIndexName();
        String job2Index = job2.getResultsIndexName();
        String job3Index = job3.getResultsIndexName();

        assertThat(indexExists(job1Index), is(true));
        assertThat(indexExists(job2Index), is(true));
        assertThat(indexExists(job3Index), is(true));

        long job1Total = getJobResultsCount(job1.getId());
        long job2Total = getJobResultsCount(job2.getId());
        long job3Total = getJobResultsCount(job3.getId());

        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();

        ResultsIndexUpgradeService resultsIndexUpgradeService = new ResultsIndexUpgradeService(indexNameExpressionResolver,
            ThreadPool.Names.SAME,
            indexMetaData -> true);

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        resultsIndexUpgradeService.upgrade(ESIntegTestCase.client(),
            new MlUpgradeAction.Request(),
            ESIntegTestCase.client().admin().cluster().prepareState().get().getState(),
            future);

        AcknowledgedResponse response = future.get();
        assertThat(response.isAcknowledged(), is(true));

        assertThat(indexExists(job1Index), is(false));
        assertThat(indexExists(job2Index), is(false));
        assertThat(indexExists(job3Index), is(false));

        ClusterState state = admin().cluster().state(new ClusterStateRequest()).actionGet().getState();
        String[] indices = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.strictExpandOpenAndForbidClosed(),
            AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*");

        // Our backing index size should be four as we have a shared and custom index and upgrading doubles the number of indices
        Assert.assertThat(indices.length, equalTo(4));

        refresh(indices);
        assertThat(getJobResultsCount(job1.getId()), equalTo(job1Total));
        assertThat(getJobResultsCount(job2.getId()), equalTo(job2Total));
        assertThat(getJobResultsCount(job3.getId()), equalTo(job3Total));


        // WE should still be able to write, and the aliases should allow to read from the appropriate indices
        postDataToJob(jobId1);
        postDataToJob(jobId2);
        postDataToJob(jobId3);
       // We should also be able to create new jobs and old jobs should be unaffected.
        String jobId4 = "migration-test4";
        Job job4 = createAndOpenJobAndStartDataFeedWithData(jobId4, dataIndex, false);
        waitUntilJobIsClosed(jobId4);

        indices = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.strictExpandOpenAndForbidClosed(),
            AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*");
        refresh(indices);

        long newJob1Total = getJobResultsCount(job1.getId());
        assertThat(newJob1Total, greaterThan(job1Total));

        long newJob2Total = getJobResultsCount(job2.getId());
        assertThat(newJob2Total, greaterThan(job2Total));

        long newJob3Total = getJobResultsCount(job3.getId());
        assertThat(newJob3Total, greaterThan(job3Total));

        assertThat(getJobResultsCount(jobId4), greaterThan(0L));
        assertThat(getJobResultsCount(jobId1), equalTo(newJob1Total));
        assertThat(getJobResultsCount(jobId2), equalTo(newJob2Total));
        assertThat(getJobResultsCount(jobId3), equalTo(newJob3Total));
    }

    //I think this test name could be a little bit longer....
    public void testMigrationWithManuallyCreatedIndexThatNeedsMigrating() throws Exception {
        String jobId1 = "migration-failure-test1";
        String jobId2 = "migration-failure-test2";
        String jobId3 = "migration-failure-test3";

        String dataIndex = createDataIndex().v2();
        List<Job> jobs = createJobsWithData(jobId1, jobId2, jobId3, dataIndex);
        Job job1 = jobs.get(0);
        Job job2 = jobs.get(1);
        Job job3 = jobs.get(2);

        String job1Index = job1.getResultsIndexName();
        String job2Index = job2.getResultsIndexName();
        String job3Index = job3.getResultsIndexName();

        // This index name should match one of the automatically created migration indices
        String manuallyCreatedIndex = job1Index + "-" + Version.CURRENT.major;
        client().admin().indices().prepareCreate(manuallyCreatedIndex).execute().actionGet();

        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();

        ResultsIndexUpgradeService resultsIndexUpgradeService = new ResultsIndexUpgradeService(indexNameExpressionResolver,
            ThreadPool.Names.SAME,
            indexMetaData -> true); //indicates that this manually created index needs migrated

        resultsIndexUpgradeService.upgrade(ESIntegTestCase.client(),
            new MlUpgradeAction.Request(),
            ESIntegTestCase.client().admin().cluster().prepareState().get().getState(),
            ActionListener.wrap(
                resp -> fail(),
                exception -> {
                    assertThat(exception, instanceOf(IllegalStateException.class));
                    assertThat(exception.getMessage(),
                        equalTo("Index [" + manuallyCreatedIndex + "] already exists and is not the current version."));
                }
            ));
    }

    public void testMigrationWithExistingIndexWithData() throws Exception {
        String jobId1 = "partial-migration-test1";
        String jobId2 = "partial-migration-test2";
        String jobId3 = "partial-migration-test3";

        String dataIndex = createDataIndex().v2();
        List<Job> jobs = createJobsWithData(jobId1, jobId2, jobId3, dataIndex);
        Job job1 = jobs.get(0);
        Job job2 = jobs.get(1);
        Job job3 = jobs.get(2);

        String job1Index = job1.getResultsIndexName();
        String job2Index = job2.getResultsIndexName();
        String job3Index = job3.getResultsIndexName();

        assertThat(indexExists(job1Index), is(true));
        assertThat(indexExists(job2Index), is(true));
        assertThat(indexExists(job3Index), is(true));

        long job1Total = getJobResultsCount(job1.getId());
        long job2Total = getJobResultsCount(job2.getId());
        long job3Total = getJobResultsCount(job3.getId());

        //lets manually create a READ index with reindexed data already
        // Should still get aliased appropriately without any additional/duplicate data.
        String alreadyMigratedIndex = job1Index + "-" + Version.CURRENT.major + "r";
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(job1Index);
        reindexRequest.setDestIndex(alreadyMigratedIndex);
        client().execute(ReindexAction.INSTANCE, reindexRequest).actionGet();

        //New write index as well, should still get aliased appropriately
        String alreadyMigratedWriteIndex = job1Index + "-" + Version.CURRENT.major;
        client().admin().indices().prepareCreate(alreadyMigratedWriteIndex).execute().actionGet();

        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();

        ResultsIndexUpgradeService resultsIndexUpgradeService = new ResultsIndexUpgradeService(indexNameExpressionResolver,
            ThreadPool.Names.SAME,
            //indicates that this manually created index is already migrated and should not be included in our migration steps
            indexMetaData -> !(indexMetaData.getIndex().getName().equals(alreadyMigratedIndex) ||
                indexMetaData.getIndex().getName().equals(alreadyMigratedWriteIndex)));

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        resultsIndexUpgradeService.upgrade(ESIntegTestCase.client(),
            new MlUpgradeAction.Request(),
            ESIntegTestCase.client().admin().cluster().prepareState().get().getState(),
            future);

        AcknowledgedResponse response = future.get();
        assertThat(response.isAcknowledged(), is(true));

        assertThat(indexExists(job1Index), is(false));
        assertThat(indexExists(job2Index), is(false));
        assertThat(indexExists(job3Index), is(false));

        ClusterState state = admin().cluster().state(new ClusterStateRequest()).actionGet().getState();
        String[] indices = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.strictExpandOpenAndForbidClosed(),
            AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*");

        // Our backing index size should be four as we have a shared and custom index and upgrading doubles the number of indices
        Assert.assertThat(indices.length, equalTo(4));
        refresh(indices);

        assertThat(getJobResultsCount(job1.getId()), equalTo(job1Total));
        assertThat(getJobResultsCount(job2.getId()), equalTo(job2Total));
        assertThat(getJobResultsCount(job3.getId()), equalTo(job3Total));

        // WE should still be able to write, and the aliases should allow to read from the appropriate indices
        postDataToJob(jobId1);
        postDataToJob(jobId2);
        postDataToJob(jobId3);

        refresh(indices);

        long newJob1Total = getJobResultsCount(job1.getId());
        assertThat(newJob1Total, greaterThan(job1Total));

        long newJob2Total = getJobResultsCount(job2.getId());
        assertThat(newJob2Total, greaterThan(job2Total));

        long newJob3Total = getJobResultsCount(job3.getId());
        assertThat(newJob3Total, greaterThan(job3Total));
    }

    private long getTotalDocCount(String indexName) {
        SearchResponse searchResponse = ESIntegTestCase.client().prepareSearch(indexName)
            .setSize(10_000)
            .setTrackTotalHits(true)
            .setQuery(QueryBuilders.matchAllQuery())
            .execute().actionGet();
        return searchResponse.getHits().getTotalHits().value;
    }

    private long getJobResultsCount(String jobId) {
        String index = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + jobId;
        return getTotalDocCount(index);
    }

    private void postDataToJob(String jobId) throws Exception {
        openJob(jobId);
        ESTestCase.assertBusy(() -> Assert.assertEquals(getJobStats(jobId).get(0).getState(), JobState.OPENED));
        startDatafeed(jobId + "-datafeed", 0L, System.currentTimeMillis());
        waitUntilJobIsClosed(jobId);
    }

    private Job createAndOpenJobAndStartDataFeedWithData(String jobId, String dataIndex, boolean isCustom) throws Exception {
        Job.Builder jobbuilder = createScheduledJob(jobId);
        if (isCustom) {
            jobbuilder.setResultsIndexName(jobId);
        }
        registerJob(jobbuilder);

        Job job = putJob(jobbuilder).getResponse();

        openJob(job.getId());
        ESTestCase.assertBusy(() -> Assert.assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig.Builder builder = createDatafeedBuilder(job.getId() + "-datafeed",
            job.getId(),
            Collections.singletonList(dataIndex));
        builder.setQueryDelay(TimeValue.timeValueSeconds(5));
        builder.setFrequency(TimeValue.timeValueSeconds(5));
        DatafeedConfig datafeedConfig = builder.build();
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, System.currentTimeMillis());
        waitUntilJobIsClosed(jobId);
        return job;
    }

    private Tuple<Long, String> createDataIndex() {
        ESIntegTestCase.client().admin().indices().prepareCreate("data-for-migration-1")
            .addMapping("type", "time", "type=date")
            .get();
        long numDocs = ESTestCase.randomIntBetween(32, 512);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs(logger, "data-for-migration-1", numDocs, twoWeeksAgo, oneWeekAgo);
        return new Tuple<>(numDocs, "data-for-migration-1");
    }

    private List<Job> createJobsWithData(String sharedJobId1, String sharedJobId2, String customJobId, String dataIndex) throws Exception {

        Job job1 = createAndOpenJobAndStartDataFeedWithData(sharedJobId1, dataIndex, false);
        Job job2 = createAndOpenJobAndStartDataFeedWithData(sharedJobId2, dataIndex, false);
        Job job3 = createAndOpenJobAndStartDataFeedWithData(customJobId, dataIndex, true);

        return Arrays.asList(job1, job2, job3);
    }
}
