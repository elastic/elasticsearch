/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelState;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class ModelSnapshotRetentionIT extends MlNativeAutodetectIntegTestCase {

    private static final long MS_IN_DAY = TimeValue.timeValueDays(1).millis();

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
            MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
            future
        );
        future.actionGet();
    }

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void testModelSnapshotRetentionNoDailyThinning() throws Exception {

        String jobId = "no-daily-thinning";

        int numDocsPerSnapshot = randomIntBetween(1, 4);
        int numSnapshotsPerDay = randomIntBetween(1, 4);
        int modelSnapshotRetentionDays = randomIntBetween(1, 10);
        int numPriorDays = randomIntBetween(1, 5);

        createJob(jobId, modelSnapshotRetentionDays, modelSnapshotRetentionDays);

        List<String> expectedModelSnapshotDocIds = new ArrayList<>();
        List<String> expectedModelStateDocIds = new ArrayList<>();

        long now = System.currentTimeMillis();
        long timeMs = now;
        // We add 1 to make the maths easier, because the retention period includes
        // the cutoff time, yet is measured from the timestamp of the latest snapshot
        int numSnapshotsTotal = numSnapshotsPerDay * (modelSnapshotRetentionDays + numPriorDays) + 1;
        for (int i = numSnapshotsTotal; i > 0; --i) {
            String snapshotId = String.valueOf(i);
            createModelSnapshot(jobId, snapshotId, new Date(timeMs), numDocsPerSnapshot, i == numSnapshotsTotal);
            if (timeMs >= now - MS_IN_DAY * modelSnapshotRetentionDays) {
                expectedModelSnapshotDocIds.add(ModelSnapshot.documentId(jobId, snapshotId));
                for (int j = 1; j <= numDocsPerSnapshot; ++j) {
                    expectedModelStateDocIds.add(ModelState.documentId(jobId, snapshotId, j));
                }
            }
            timeMs -= (MS_IN_DAY / numSnapshotsPerDay);
        }
        refresh(".ml*");

        deleteExpiredData();

        Collections.sort(expectedModelSnapshotDocIds);
        Collections.sort(expectedModelStateDocIds);
        assertThat(getAvailableModelSnapshotDocIds(jobId), is(expectedModelSnapshotDocIds));
        assertThat(getAvailableModelStateDocIds(), is(expectedModelStateDocIds));
    }

    public void testModelSnapshotRetentionWithDailyThinning() throws Exception {

        String jobId = "with-daily-thinning";

        int numDocsPerSnapshot = randomIntBetween(1, 4);
        int numSnapshotsPerDay = randomIntBetween(1, 4);
        int modelSnapshotRetentionDays = randomIntBetween(2, 10);
        int numPriorDays = randomIntBetween(1, 5);
        int dailyModelSnapshotRetentionAfterDays = randomIntBetween(0, modelSnapshotRetentionDays - 1);

        createJob(jobId, modelSnapshotRetentionDays, dailyModelSnapshotRetentionAfterDays);

        List<String> expectedModelSnapshotDocIds = new ArrayList<>();
        List<String> expectedModelStateDocIds = new ArrayList<>();

        long now = System.currentTimeMillis();
        long timeMs = now;
        // We add 1 to make the maths easier, because the retention period includes
        // the cutoff time, yet is measured from the timestamp of the latest snapshot
        int numSnapshotsTotal = numSnapshotsPerDay * (modelSnapshotRetentionDays + numPriorDays) + 1;
        for (int i = numSnapshotsTotal; i > 0; --i) {
            String snapshotId = String.valueOf(i);
            createModelSnapshot(jobId, snapshotId, new Date(timeMs), numDocsPerSnapshot, i == numSnapshotsTotal);
            // We should retain:
            // - Nothing older than modelSnapshotRetentionDays
            // - Everything newer than dailyModelSnapshotRetentionAfterDays
            // - The first snapshot of each day in between
            if (timeMs >= now - MS_IN_DAY * modelSnapshotRetentionDays
                && (timeMs >= now - MS_IN_DAY * dailyModelSnapshotRetentionAfterDays
                    || (now - timeMs) % MS_IN_DAY < MS_IN_DAY / numSnapshotsPerDay)) {
                expectedModelSnapshotDocIds.add(ModelSnapshot.documentId(jobId, snapshotId));
                for (int j = 1; j <= numDocsPerSnapshot; ++j) {
                    expectedModelStateDocIds.add(ModelState.documentId(jobId, snapshotId, j));
                }
            }
            timeMs -= (MS_IN_DAY / numSnapshotsPerDay);
        }
        refresh(".ml*");

        deleteExpiredData();

        Collections.sort(expectedModelSnapshotDocIds);
        Collections.sort(expectedModelStateDocIds);
        assertThat(getAvailableModelSnapshotDocIds(jobId), is(expectedModelSnapshotDocIds));
        assertThat(getAvailableModelStateDocIds(), is(expectedModelStateDocIds));
    }

    private List<String> getAvailableModelSnapshotDocIds(String jobId) throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.existsQuery(ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName()));
        searchRequest.source(new SearchSourceBuilder().query(query).size(10000));

        return getDocIdsFromSearch(searchRequest);
    }

    private List<String> getAvailableModelStateDocIds() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(AnomalyDetectorsIndex.jobStateIndexPattern());
        searchRequest.source(new SearchSourceBuilder().size(10000));

        return getDocIdsFromSearch(searchRequest);
    }

    private List<String> getDocIdsFromSearch(SearchRequest searchRequest) throws Exception {
        List<String> docIds = new ArrayList<>();
        assertResponse(client().execute(TransportSearchAction.TYPE, searchRequest), searchResponse -> {
            for (SearchHit searchHit : searchResponse.getHits()) {
                docIds.add(searchHit.getId());
            }
        });
        Collections.sort(docIds);
        return docIds;
    }

    private void createJob(String jobId, long modelSnapshotRetentionDays, long dailyModelSnapshotRetentionAfterDays) {
        Detector detector = new Detector.Builder("count", null).build();

        Job.Builder builder = new Job.Builder();
        builder.setId(jobId);
        builder.setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(detector)));
        builder.setDataDescription(new DataDescription.Builder());
        builder.setModelSnapshotRetentionDays(modelSnapshotRetentionDays);
        builder.setDailyModelSnapshotRetentionAfterDays(dailyModelSnapshotRetentionAfterDays);

        PutJobAction.Request putJobRequest = new PutJobAction.Request(builder);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();
    }

    private void createModelSnapshot(String jobId, String snapshotId, Date timestamp, int numDocs, boolean isActive) throws IOException {
        persistModelSnapshotDoc(jobId, snapshotId, timestamp, numDocs, isActive);
        persistModelStateDocs(jobId, snapshotId, numDocs);
        if (isActive) {
            JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setModelSnapshotId(snapshotId).build();
            UpdateJobAction.Request updateJobRequest = UpdateJobAction.Request.internal(jobId, jobUpdate);
            client().execute(UpdateJobAction.INSTANCE, updateJobRequest).actionGet();
        }
    }

    private void persistModelSnapshotDoc(String jobId, String snapshotId, Date timestamp, int numDocs, boolean immediateRefresh)
        throws IOException {
        ModelSnapshot.Builder modelSnapshotBuilder = new ModelSnapshot.Builder();
        modelSnapshotBuilder.setJobId(jobId).setSnapshotId(snapshotId).setTimestamp(timestamp).setSnapshotDocCount(numDocs);

        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.resultsWriteAlias(jobId)).id(
            ModelSnapshot.documentId(jobId, snapshotId)
        ).setRequireAlias(true);
        if (immediateRefresh) {
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
        modelSnapshotBuilder.build().toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        indexRequest.source(xContentBuilder);

        DocWriteResponse indexResponse = client().execute(TransportIndexAction.TYPE, indexRequest).actionGet();
        assertThat(indexResponse.getResult(), is(DocWriteResponse.Result.CREATED));
    }

    private void persistModelStateDocs(String jobId, String snapshotId, int numDocs) {
        assertThat(numDocs, greaterThan(0));

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i <= numDocs; ++i) {
            IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.jobStateIndexWriteAlias()).id(
                ModelState.documentId(jobId, snapshotId, i)
            )
                // The exact contents of the model state doesn't matter - we are not going to try and restore it
                .source(Collections.singletonMap("compressed", Collections.singletonList("foo")))
                .setRequireAlias(true);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = client().execute(TransportBulkAction.TYPE, bulkRequest).actionGet();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
    }
}
