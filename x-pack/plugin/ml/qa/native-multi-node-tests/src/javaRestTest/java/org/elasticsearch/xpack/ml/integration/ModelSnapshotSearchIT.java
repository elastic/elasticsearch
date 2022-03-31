/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
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
import java.util.Collections;
import java.util.Date;

import static org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class ModelSnapshotSearchIT extends MlNativeAutodetectIntegTestCase {

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

    public void testGetModelSnapshotsSortedByMinVersion() throws Exception {

        String jobId = "job-with-snapshot";

        createJob(jobId);

        long now = System.currentTimeMillis();
        int numSnapshotsTotal = 5;
        for (int i = numSnapshotsTotal; i > 0; --i) {
            String snapshotId = String.valueOf(i);
            createModelSnapshot(jobId, snapshotId, new Date(now), 2, i == numSnapshotsTotal);
        }
        refresh(".ml*");

        GetModelSnapshotsAction.Request request = new GetModelSnapshotsAction.Request(jobId, null);
        request.setSort("min_version");
        // Should not throw
        client().execute(GetModelSnapshotsAction.INSTANCE, request).actionGet();
    }

    private void createJob(String jobId) {
        Detector detector = new Detector.Builder("count", null).build();

        Job.Builder builder = new Job.Builder().setId(jobId)
            .setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(detector)))
            .setDataDescription(new DataDescription.Builder());
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

        IndexResponse indexResponse = client().execute(IndexAction.INSTANCE, indexRequest).actionGet();
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

        BulkResponse bulkResponse = client().execute(BulkAction.INSTANCE, bulkRequest).actionGet();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
    }
}
