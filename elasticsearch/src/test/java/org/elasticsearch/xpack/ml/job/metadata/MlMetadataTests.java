/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.metadata;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobTests;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunnerTests.createDatafeedConfig;
import static org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunnerTests.createDatafeedJob;
import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MlMetadataTests extends AbstractSerializingTestCase<MlMetadata> {

    @Override
    protected MlMetadata createTestInstance() {
        MlMetadata.Builder builder = new MlMetadata.Builder();
        int numJobs = randomIntBetween(0, 10);
        for (int i = 0; i < numJobs; i++) {
            Job job = JobTests.createRandomizedJob();
            if (randomBoolean()) {
                DatafeedConfig datafeedConfig = DatafeedConfigTests.createRandomizedDatafeedConfig(job.getId());
                if (datafeedConfig.getAggregations() != null) {
                    AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(job.getAnalysisConfig().getDetectors());
                    analysisConfig.setSummaryCountFieldName("doc_count");
                    Job.Builder jobBuilder = new Job.Builder(job);
                    jobBuilder.setAnalysisConfig(analysisConfig);
                    job = jobBuilder.build();
                }
                builder.putJob(job, false);
                builder.putDatafeed(datafeedConfig);
            } else {
                builder.putJob(job, false);
            }
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<MlMetadata> instanceReader() {
        return MlMetadata::new;
    }

    @Override
    protected MlMetadata parseInstance(XContentParser parser) {
        return MlMetadata.ML_METADATA_PARSER.apply(parser, null).build();
    }

    @Override
    protected XContentBuilder toXContent(MlMetadata instance, XContentType contentType) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        // In Metadata.Builder#toXContent(...) custom metadata always gets wrapped in an start and end object,
        // so we simulate that here. The MlMetadata depends on that as it direct starts to write a start array.
        builder.startObject();
        instance.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder;
    }

    public void testPutJob() {
        Job job1 = buildJobBuilder("1").build();
        Job job2 = buildJobBuilder("2").build();

        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putJob(job2, false);

        MlMetadata result = builder.build();
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("1"), nullValue());
        assertThat(result.getJobs().get("2"), sameInstance(job2));
        assertThat(result.getDatafeeds().get("2"), nullValue());

        builder = new MlMetadata.Builder(result);

        MlMetadata.Builder builderReference = builder;
        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class, () -> builderReference.putJob(job2, false));
        assertEquals("The job cannot be created with the Id '2'. The Id is already used.", e.getMessage());
        Job job2Attempt2 = buildJobBuilder("2").build();
        builder.putJob(job2Attempt2, true);

        result = builder.build();
        assertThat(result.getJobs().size(), equalTo(2));
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getJobs().get("2"), sameInstance(job2Attempt2));
    }

    public void testRemoveJob() {
        Job.Builder jobBuilder = buildJobBuilder("1");
        jobBuilder.setDeleted(true);
        Job job1 = jobBuilder.build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);

        MlMetadata result = builder.build();
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("1"), nullValue());

        builder = new MlMetadata.Builder(result);
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("1"), nullValue());

        builder.deleteJob("1", new PersistentTasksInProgress(0L, Collections.emptyMap()));
        result = builder.build();
        assertThat(result.getJobs().get("1"), nullValue());
        assertThat(result.getDatafeeds().get("1"), nullValue());
    }

    public void testRemoveJob_failBecauseJobIsOpen() {
        Job job1 = buildJobBuilder("1").build();
        MlMetadata.Builder builder1 = new MlMetadata.Builder();
        builder1.putJob(job1, false);

        MlMetadata result = builder1.build();
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("1"), nullValue());

        PersistentTaskInProgress<OpenJobAction.Request> task =
                new PersistentTaskInProgress<>(
                        new PersistentTaskInProgress<>(0L, OpenJobAction.NAME, new OpenJobAction.Request("1"), null),
                        JobState.CLOSED
                );
        MlMetadata.Builder builder2 = new MlMetadata.Builder(result);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> builder2.deleteJob("1", new PersistentTasksInProgress(0L, Collections.singletonMap(0L, task))));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

    public void testRemoveJob_failDatafeedRefersToJob() {
        Job job1 = createDatafeedJob().build();
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> builder.deleteJob(job1.getId(), new PersistentTasksInProgress(0L, Collections.emptyMap())));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        String expectedMsg = "Cannot delete job [" + job1.getId() + "] while datafeed [" + datafeedConfig1.getId() + "] refers to it";
        assertThat(e.getMessage(), equalTo(expectedMsg));
    }

    public void testRemoveJob_failBecauseJobDoesNotExist() {
        MlMetadata.Builder builder1 = new MlMetadata.Builder();
        expectThrows(ResourceNotFoundException.class,
                () -> builder1.deleteJob("1", new PersistentTasksInProgress(0L, Collections.emptyMap())));
    }

    public void testCrudDatafeed() {
        Job job1 = createDatafeedJob().build();
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1);

        MlMetadata result = builder.build();
        assertThat(result.getJobs().get("foo"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("datafeed1"), sameInstance(datafeedConfig1));

        builder = new MlMetadata.Builder(result);
        builder.removeDatafeed("datafeed1", new PersistentTasksInProgress(0, Collections.emptyMap()));
        result = builder.build();
        assertThat(result.getJobs().get("foo"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("datafeed1"), nullValue());
    }

    public void testPutDatafeed_failBecauseJobDoesNotExist() {
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", "missing-job").build();
        MlMetadata.Builder builder = new MlMetadata.Builder();

        expectThrows(ResourceNotFoundException.class, () -> builder.putDatafeed(datafeedConfig1));
    }

    public void testPutDatafeed_failBecauseDatafeedIdIsAlreadyTaken() {
        Job job1 = createDatafeedJob().build();
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1);

        expectThrows(ResourceAlreadyExistsException.class, () -> builder.putDatafeed(datafeedConfig1));
    }

    public void testPutDatafeed_failBecauseJobAlreadyHasDatafeed() {
        Job job1 = createDatafeedJob().build();
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        DatafeedConfig datafeedConfig2 = createDatafeedConfig("datafeed2", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> builder.putDatafeed(datafeedConfig2));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

    public void testPutDatafeed_failBecauseJobIsNotCompatibleForDatafeed() {
        Job.Builder job1 = createDatafeedJob();
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(job1.build().getAnalysisConfig());
        analysisConfig.setLatency(3600L);
        job1.setAnalysisConfig(analysisConfig);
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1.build(), false);

        expectThrows(IllegalArgumentException.class, () -> builder.putDatafeed(datafeedConfig1));
    }

    public void testRemoveDatafeed_failBecauseDatafeedStarted() {
        Job job1 = createDatafeedJob().build();
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1);

        MlMetadata result = builder.build();
        assertThat(result.getJobs().get("foo"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("datafeed1"), sameInstance(datafeedConfig1));

        StartDatafeedAction.Request request = new StartDatafeedAction.Request("datafeed1", 0L);
        PersistentTaskInProgress<StartDatafeedAction.Request> taskInProgress =
                new PersistentTaskInProgress<>(0, StartDatafeedAction.NAME, request, null);
        PersistentTasksInProgress tasksInProgress =
                new PersistentTasksInProgress(1, Collections.singletonMap(taskInProgress.getId(), taskInProgress));

        MlMetadata.Builder builder2 = new MlMetadata.Builder(result);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> builder2.removeDatafeed("datafeed1", tasksInProgress));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

}
