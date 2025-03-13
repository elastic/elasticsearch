/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ml.annotations.AnnotationTests.randomAnnotation;
import static org.hamcrest.Matchers.containsString;

public class DeleteJobIT extends MlNativeAutodetectIntegTestCase {

    private static final String DATA_INDEX = "delete-job-annotations-test-data";
    private static final String TIME_FIELD = "time";

    @Before
    public void setUpData() {
        client().admin().indices().prepareCreate(DATA_INDEX).setMapping(TIME_FIELD, "type=date,format=epoch_millis").get();
    }

    @After
    public void tearDownData() {
        client().admin().indices().prepareDelete(DATA_INDEX).get();
        cleanUp();
    }

    public void testDeleteJobDeletesAnnotations() throws Exception {
        String jobIdA = "delete-annotations-a";
        String datafeedIdA = jobIdA + "-feed";
        String jobIdB = "delete-annotations-b";
        String datafeedIdB = jobIdB + "-feed";

        // No annotations so far
        assertThatNumberOfAnnotationsIsEqualTo(0);

        runJob(jobIdA, datafeedIdA);
        client().index(randomAnnotationIndexRequest(jobIdA, InternalUsers.XPACK_USER.principal())).actionGet();
        client().index(randomAnnotationIndexRequest(jobIdA, InternalUsers.XPACK_USER.principal())).actionGet();
        client().index(randomAnnotationIndexRequest(jobIdA, "real_user")).actionGet();
        // 3 jobA annotations (2 _xpack, 1 real_user)
        assertThatNumberOfAnnotationsIsEqualTo(3);

        runJob(jobIdB, datafeedIdB);
        client().index(randomAnnotationIndexRequest(jobIdB, InternalUsers.XPACK_USER.principal())).actionGet();
        client().index(randomAnnotationIndexRequest(jobIdB, "other_real_user")).actionGet();
        // 3 jobA annotations (2 _xpack, 1 real_user) and 2 jobB annotations (1 _xpack, 1 real_user)
        assertThatNumberOfAnnotationsIsEqualTo(5);

        deleteDatafeed(datafeedIdA);
        deleteJob(jobIdA);
        // 1 jobA annotation (real_user) and 2 jobB annotations (1 _xpack, 1 real_user)
        assertThatNumberOfAnnotationsIsEqualTo(3);

        deleteDatafeed(datafeedIdB);
        deleteJob(jobIdB);
        // 1 jobA annotation (real_user) and 1 jobB annotation (real_user)
        assertThatNumberOfAnnotationsIsEqualTo(2);
    }

    public void testDeletingMultipleJobsInOneRequestIsImpossible() {
        String jobIdA = "delete-multiple-jobs-a";
        String jobIdB = "delete-multiple-jobs-b";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> deleteJob(jobIdA + "," + jobIdB));
        assertThat(e.getMessage(), containsString("Invalid job_id"));
    }

    private void runJob(String jobId, String datafeedId) throws Exception {
        Detector.Builder detector = new Detector.Builder().setFunction("count");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build())).setBucketSpan(
            TimeValue.timeValueHours(1)
        );
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField(TIME_FIELD);
        Job.Builder job = new Job.Builder(jobId).setAnalysisConfig(analysisConfig).setDataDescription(dataDescription);

        putJob(job);

        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedConfig.setIndices(Collections.singletonList(DATA_INDEX));
        DatafeedConfig datafeedA = datafeedConfig.build();

        putDatafeed(datafeedA);

        openJob(jobId);
        // Run up to a day ago
        long now = System.currentTimeMillis();
        startDatafeed(datafeedId, 0, now - TimeValue.timeValueHours(24).getMillis());
        waitUntilJobIsClosed(jobId);
    }

    private static IndexRequest randomAnnotationIndexRequest(String jobId, String createUsername) throws IOException {
        Annotation annotation = new Annotation.Builder(randomAnnotation(jobId)).setCreateUsername(createUsername).build();
        try (XContentBuilder xContentBuilder = annotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
            return new IndexRequest(AnnotationIndex.WRITE_ALIAS_NAME).source(xContentBuilder)
                .setRequireAlias(true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
    }
}
