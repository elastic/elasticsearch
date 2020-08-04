/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

/**
 * Test that ML does not touch unnecessary indices when removing job index aliases
 */
public class JobStorageDeletionTaskIT extends BaseMlIntegTestCase {

    private static long bucketSpan = AnalysisConfig.Builder.DEFAULT_BUCKET_SPAN.getMillis();
    private static final String UNRELATED_INDEX = "unrelated-data";

    private JobResultsProvider jobResultsProvider;
    private JobResultsPersister jobResultsPersister;

    @Before
    public void createComponents() {
        Settings settings = nodeSettings(0);
        ThreadPool tp = mock(ThreadPool.class);
        ClusterSettings clusterSettings = new ClusterSettings(settings,
            new HashSet<>(Arrays.asList(InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                ClusterService.USER_DEFINED_METADATA,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING)));
        ClusterService clusterService = new ClusterService(settings, clusterSettings, tp);
        OriginSettingClient originSettingClient = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        ResultsPersisterService resultsPersisterService = new ResultsPersisterService(originSettingClient, clusterService, settings);
        jobResultsProvider = new JobResultsProvider(client(), settings, new IndexNameExpressionResolver());
        jobResultsPersister = new JobResultsPersister(
            originSettingClient, resultsPersisterService, new AnomalyDetectionAuditor(client(), "test_node"));
    }

    public void testUnrelatedIndexNotTouched() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);

        client().admin().indices().prepareCreate(UNRELATED_INDEX).get();

        enableIndexBlock(UNRELATED_INDEX, IndexMetadata.SETTING_READ_ONLY);

        Job.Builder job = createJob("delete-aliases-test-job", new ByteSizeValue(2, ByteSizeUnit.MB));
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
        awaitJobOpenedAndAssigned(job.getId(), null);

        DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(job.getId());
        deleteJobRequest.setForce(true);
        client().execute(DeleteJobAction.INSTANCE, deleteJobRequest).actionGet();

        // If the deletion of aliases touches the unrelated index with the block
        // then the line above will throw a ClusterBlockException

        disableIndexBlock(UNRELATED_INDEX, IndexMetadata.SETTING_READ_ONLY);
    }

    public void testDeleteDedicatedJobWithDataInShared() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);
        String jobIdDedicated = "delete-test-job-dedicated";

        Job.Builder job = createJob(jobIdDedicated, new ByteSizeValue(2, ByteSizeUnit.MB)).setResultsIndexName(jobIdDedicated);
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).actionGet();
        String dedicatedIndex = job.build().getInitialResultsIndexName();
        awaitJobOpenedAndAssigned(job.getId(), null);
        createBuckets(jobIdDedicated, 1, 10);

        String jobIdShared = "delete-test-job-shared";
        job = createJob(jobIdShared, new ByteSizeValue(2, ByteSizeUnit.MB));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).actionGet();
        awaitJobOpenedAndAssigned(job.getId(), null);
        createBuckets(jobIdShared, 1, 10);

        // Manually switching over alias info
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
            .addAliasAction(IndicesAliasesRequest.AliasActions
                .add()
                .alias(AnomalyDetectorsIndex.jobResultsAliasedName(jobIdDedicated))
                .isHidden(true)
                .index(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared")
                .writeIndex(false)
                .filter(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobIdDedicated))))
            .addAliasAction(IndicesAliasesRequest.AliasActions
                .add()
                .alias(AnomalyDetectorsIndex.resultsWriteAlias(jobIdDedicated))
                .index(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "shared")
                .isHidden(true)
                .writeIndex(true))
            .addAliasAction(IndicesAliasesRequest.AliasActions
                .remove()
                .alias(AnomalyDetectorsIndex.resultsWriteAlias(jobIdDedicated))
                .index(dedicatedIndex));

        client().admin().indices().aliases(aliasesRequest).actionGet();

        createBuckets(jobIdDedicated, 11, 10);
        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*").get();
        AtomicReference<QueryPage<Bucket>> bucketHandler = new AtomicReference<>();
        AtomicReference<Exception> failureHandler = new AtomicReference<>();
        blockingCall(listener ->  jobResultsProvider.buckets(jobIdDedicated,
            new BucketsQueryBuilder().from(0).size(22),
            listener::onResponse,
            listener::onFailure,
            client()), bucketHandler, failureHandler);
        assertThat(failureHandler.get(), is(nullValue()));
        assertThat(bucketHandler.get().count(), equalTo(22L));

        DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(jobIdDedicated);
        deleteJobRequest.setForce(true);
        client().execute(DeleteJobAction.INSTANCE, deleteJobRequest).get();

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*").get();
        // Make sure our shared index job is OK
        bucketHandler = new AtomicReference<>();
        failureHandler = new AtomicReference<>();
        blockingCall(listener ->  jobResultsProvider.buckets(jobIdShared,
            new BucketsQueryBuilder().from(0).size(21),
            listener::onResponse,
            listener::onFailure,
            client()), bucketHandler, failureHandler);
        assertThat(failureHandler.get(), is(nullValue()));
        assertThat(bucketHandler.get().count(), equalTo(11L));

        // Make sure dedicated index is gone
        assertThat(client().admin()
            .indices()
            .prepareGetIndex()
            .setIndices(dedicatedIndex)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .get()
            .indices().length, equalTo(0));

        // Make sure all results referencing the dedicated job are gone
        assertThat(client().prepareSearch()
            .setIndices(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*")
            .setIndicesOptions(IndicesOptions.lenientExpandOpenHidden())
            .setTrackTotalHits(true)
            .setSize(0)
            .setSource(SearchSourceBuilder.searchSource()
                .query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobIdDedicated))))
            .get()
            .getHits()
            .getTotalHits()
            .value, equalTo(0L));
    }

    private void createBuckets(String jobId, int from, int count) {
        JobResultsPersister.Builder builder = jobResultsPersister.bulkPersisterBuilder(jobId);
        for (int i = from; i <= count + from; ++i) {
            Bucket bucket = new Bucket(jobId, new Date(bucketSpan * i), bucketSpan);
            builder.persistBucket(bucket);
        }
        builder.executeRequest();
    }

}
