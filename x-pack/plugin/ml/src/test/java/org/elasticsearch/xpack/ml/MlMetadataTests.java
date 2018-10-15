/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.persistent.PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT;
import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedConfig;
import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedJob;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
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
                AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(job.getAnalysisConfig());
                analysisConfig.setLatency(null);
                DatafeedConfig datafeedConfig = DatafeedConfigTests.createRandomizedDatafeedConfig(
                        job.getId(), job.getAnalysisConfig().getBucketSpan().millis());
                if (datafeedConfig.hasAggregations()) {
                    analysisConfig.setSummaryCountFieldName("doc_count");
                }
                job = new Job.Builder(job).setAnalysisConfig(analysisConfig).build();
                builder.putJob(job, false);
                builder.putDatafeed(datafeedConfig, Collections.emptyMap());
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
    protected MlMetadata doParseInstance(XContentParser parser) {
        return MlMetadata.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
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
        jobBuilder.setDeleting(true);
        Job job1 = jobBuilder.build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);

        MlMetadata result = builder.build();
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("1"), nullValue());

        builder = new MlMetadata.Builder(result);
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("1"), nullValue());

        builder.deleteJob("1", new PersistentTasksCustomMetaData(0L, Collections.emptyMap()));
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

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("1", null, JobState.CLOSED, tasksBuilder);
        MlMetadata.Builder builder2 = new MlMetadata.Builder(result);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> builder2.deleteJob("1", tasksBuilder.build()));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

    public void testRemoveJob_failDatafeedRefersToJob() {
        Job job1 = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> builder.deleteJob(job1.getId(), new PersistentTasksCustomMetaData(0L, Collections.emptyMap())));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        String expectedMsg = "Cannot delete job [" + job1.getId() + "] because datafeed [" + datafeedConfig1.getId() + "] refers to it";
        assertThat(e.getMessage(), equalTo(expectedMsg));
    }

    public void testRemoveJob_failBecauseJobDoesNotExist() {
        MlMetadata.Builder builder1 = new MlMetadata.Builder();
        expectThrows(ResourceNotFoundException.class,
                () -> builder1.deleteJob("1", new PersistentTasksCustomMetaData(0L, Collections.emptyMap())));
    }

    public void testCrudDatafeed() {
        Job job1 = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());

        MlMetadata result = builder.build();
        assertThat(result.getJobs().get("job_id"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("datafeed1"), sameInstance(datafeedConfig1));

        builder = new MlMetadata.Builder(result);
        builder.removeDatafeed("datafeed1", new PersistentTasksCustomMetaData(0, Collections.emptyMap()));
        result = builder.build();
        assertThat(result.getJobs().get("job_id"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("datafeed1"), nullValue());
    }

    public void testPutDatafeed_failBecauseJobDoesNotExist() {
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", "missing-job").build();
        MlMetadata.Builder builder = new MlMetadata.Builder();

        expectThrows(ResourceNotFoundException.class, () -> builder.putDatafeed(datafeedConfig1, Collections.emptyMap()));
    }

    public void testPutDatafeed_failBecauseJobIsBeingDeleted() {
        Job job1 = createDatafeedJob().setDeleting(true).build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);

        expectThrows(ResourceNotFoundException.class, () -> builder.putDatafeed(datafeedConfig1, Collections.emptyMap()));
    }

    public void testPutDatafeed_failBecauseDatafeedIdIsAlreadyTaken() {
        Job job1 = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());

        expectThrows(ResourceAlreadyExistsException.class, () -> builder.putDatafeed(datafeedConfig1, Collections.emptyMap()));
    }

    public void testPutDatafeed_failBecauseJobAlreadyHasDatafeed() {
        Job job1 = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        DatafeedConfig datafeedConfig2 = createDatafeedConfig("datafeed2", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> builder.putDatafeed(datafeedConfig2, Collections.emptyMap()));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

    public void testPutDatafeed_failBecauseJobIsNotCompatibleForDatafeed() {
        Job.Builder job1 = createDatafeedJob();
        Date now = new Date();
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(job1.build(now).getAnalysisConfig());
        analysisConfig.setLatency(TimeValue.timeValueHours(1));
        job1.setAnalysisConfig(analysisConfig);
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1.build(now), false);

        expectThrows(ElasticsearchStatusException.class, () -> builder.putDatafeed(datafeedConfig1, Collections.emptyMap()));
    }

    public void testPutDatafeed_setsSecurityHeaders() {
        Job datafeedJob = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed1", datafeedJob.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(datafeedJob, false);

        Map<String, String> headers = new HashMap<>();
        headers.put("unrelated_header", "unrelated_header_value");
        headers.put(AuthenticationServiceField.RUN_AS_USER_HEADER, "permitted_run_as_user");
        builder.putDatafeed(datafeedConfig, headers);
        MlMetadata metadata = builder.build();
        assertThat(metadata.getDatafeed("datafeed1").getHeaders().size(), equalTo(1));
        assertThat(metadata.getDatafeed("datafeed1").getHeaders(),
                hasEntry(AuthenticationServiceField.RUN_AS_USER_HEADER, "permitted_run_as_user"));
    }

    public void testUpdateDatafeed() {
        Job job1 = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());
        MlMetadata beforeMetadata = builder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedConfig1.getId());
        update.setScrollSize(5000);
        MlMetadata updatedMetadata =
                new MlMetadata.Builder(beforeMetadata).updateDatafeed(update.build(), null, Collections.emptyMap()).build();

        DatafeedConfig updatedDatafeed = updatedMetadata.getDatafeed(datafeedConfig1.getId());
        assertThat(updatedDatafeed.getJobId(), equalTo(datafeedConfig1.getJobId()));
        assertThat(updatedDatafeed.getIndices(), equalTo(datafeedConfig1.getIndices()));
        assertThat(updatedDatafeed.getTypes(), equalTo(datafeedConfig1.getTypes()));
        assertThat(updatedDatafeed.getScrollSize(), equalTo(5000));
    }

    public void testUpdateDatafeed_failBecauseDatafeedDoesNotExist() {
        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder("job_id");
        update.setScrollSize(5000);
        expectThrows(ResourceNotFoundException.class,
                () -> new MlMetadata.Builder().updateDatafeed(update.build(), null, Collections.emptyMap()).build());
    }

    public void testUpdateDatafeed_failBecauseDatafeedIsNotStopped() {
        Job job1 = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());
        MlMetadata beforeMetadata = builder.build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams(datafeedConfig1.getId(), 0L);
        tasksBuilder.addTask(MlTasks.datafeedTaskId("datafeed1"), StartDatafeedAction.TASK_NAME, params, INITIAL_ASSIGNMENT);
        PersistentTasksCustomMetaData tasksInProgress = tasksBuilder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedConfig1.getId());
        update.setScrollSize(5000);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> new MlMetadata.Builder(beforeMetadata).updateDatafeed(update.build(), tasksInProgress, null));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

    public void testUpdateDatafeed_failBecauseNewJobIdDoesNotExist() {
        Job job1 = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());
        MlMetadata beforeMetadata = builder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedConfig1.getId());
        update.setJobId(job1.getId() + "_2");

        expectThrows(ResourceNotFoundException.class,
                () -> new MlMetadata.Builder(beforeMetadata).updateDatafeed(update.build(), null, Collections.emptyMap()));
    }

    public void testUpdateDatafeed_failBecauseNewJobHasAnotherDatafeedAttached() {
        Job job1 = createDatafeedJob().build(new Date());
        Job.Builder job2 = new Job.Builder(job1);
        job2.setId(job1.getId() + "_2");
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        DatafeedConfig datafeedConfig2 = createDatafeedConfig("datafeed2", job2.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putJob(job2.build(), false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());
        builder.putDatafeed(datafeedConfig2, Collections.emptyMap());
        MlMetadata beforeMetadata = builder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedConfig1.getId());
        update.setJobId(job2.getId());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> new MlMetadata.Builder(beforeMetadata).updateDatafeed(update.build(), null, Collections.emptyMap()));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        assertThat(e.getMessage(), equalTo("A datafeed [datafeed2] already exists for job [job_id_2]"));
    }

    public void testUpdateDatafeed_setsSecurityHeaders() {
        Job datafeedJob = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed1", datafeedJob.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(datafeedJob, false);
        builder.putDatafeed(datafeedConfig, Collections.emptyMap());
        MlMetadata beforeMetadata = builder.build();
        assertTrue(beforeMetadata.getDatafeed("datafeed1").getHeaders().isEmpty());

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedConfig.getId());
        update.setQueryDelay(TimeValue.timeValueMinutes(5));

        Map<String, String> headers = new HashMap<>();
        headers.put("unrelated_header", "unrelated_header_value");
        headers.put(AuthenticationServiceField.RUN_AS_USER_HEADER, "permitted_run_as_user");
        MlMetadata afterMetadata = new MlMetadata.Builder(beforeMetadata).updateDatafeed(update.build(), null, headers).build();
        Map<String, String> updatedHeaders = afterMetadata.getDatafeed("datafeed1").getHeaders();
        assertThat(updatedHeaders.size(), equalTo(1));
        assertThat(updatedHeaders, hasEntry(AuthenticationServiceField.RUN_AS_USER_HEADER, "permitted_run_as_user"));
    }

    public void testRemoveDatafeed_failBecauseDatafeedStarted() {
        Job job1 = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig1 = createDatafeedConfig("datafeed1", job1.getId()).build();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(job1, false);
        builder.putDatafeed(datafeedConfig1, Collections.emptyMap());

        MlMetadata result = builder.build();
        assertThat(result.getJobs().get("job_id"), sameInstance(job1));
        assertThat(result.getDatafeeds().get("datafeed1"), sameInstance(datafeedConfig1));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("datafeed1", 0L);
        tasksBuilder.addTask(MlTasks.datafeedTaskId("datafeed1"), StartDatafeedAction.TASK_NAME, params, INITIAL_ASSIGNMENT);
        PersistentTasksCustomMetaData tasksInProgress = tasksBuilder.build();

        MlMetadata.Builder builder2 = new MlMetadata.Builder(result);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> builder2.removeDatafeed("datafeed1", tasksInProgress));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

    public void testExpandJobIds() {
        MlMetadata mlMetadata = newMlMetadataWithJobs("bar-1", "foo-1", "foo-2").build();

        assertThat(mlMetadata.expandJobIds("_all", false), contains("bar-1", "foo-1", "foo-2"));
        assertThat(mlMetadata.expandJobIds("*", false), contains("bar-1", "foo-1", "foo-2"));
        assertThat(mlMetadata.expandJobIds("foo-*", false), contains("foo-1", "foo-2"));
        assertThat(mlMetadata.expandJobIds("foo-1,bar-*", false), contains("bar-1", "foo-1"));
    }

    public void testExpandDatafeedIds() {
        MlMetadata.Builder mlMetadataBuilder = newMlMetadataWithJobs("bar-1", "foo-1", "foo-2");
        mlMetadataBuilder.putDatafeed(createDatafeedConfig("bar-1-feed", "bar-1").build(), Collections.emptyMap());
        mlMetadataBuilder.putDatafeed(createDatafeedConfig("foo-1-feed", "foo-1").build(), Collections.emptyMap());
        mlMetadataBuilder.putDatafeed(createDatafeedConfig("foo-2-feed", "foo-2").build(), Collections.emptyMap());
        MlMetadata mlMetadata = mlMetadataBuilder.build();


        assertThat(mlMetadata.expandDatafeedIds("_all", false), contains("bar-1-feed", "foo-1-feed", "foo-2-feed"));
        assertThat(mlMetadata.expandDatafeedIds("*", false), contains("bar-1-feed", "foo-1-feed", "foo-2-feed"));
        assertThat(mlMetadata.expandDatafeedIds("foo-*", false), contains("foo-1-feed", "foo-2-feed"));
        assertThat(mlMetadata.expandDatafeedIds("foo-1-feed,bar-1*", false), contains("bar-1-feed", "foo-1-feed"));
    }

    private static MlMetadata.Builder newMlMetadataWithJobs(String... jobIds) {
        MlMetadata.Builder builder = new MlMetadata.Builder();
        for (String jobId : jobIds) {
            Job job = buildJobBuilder(jobId).build();
            builder.putJob(job, false);
        }
        return builder;
    }

    @Override
    protected MlMetadata mutateInstance(MlMetadata instance) {
        Map<String, Job> jobs = instance.getJobs();
        Map<String, DatafeedConfig> datafeeds = instance.getDatafeeds();
        MlMetadata.Builder metadataBuilder = new MlMetadata.Builder();

        for (Map.Entry<String, Job> entry : jobs.entrySet()) {
            metadataBuilder.putJob(entry.getValue(), true);
        }
        for (Map.Entry<String, DatafeedConfig> entry : datafeeds.entrySet()) {
            metadataBuilder.putDatafeed(entry.getValue(), Collections.emptyMap());
        }

        switch (between(0, 1)) {
        case 0:
            metadataBuilder.putJob(JobTests.createRandomizedJob(), true);
            break;
        case 1:
            // Because we check if the job for the datafeed exists and we don't
            // allow two datafeeds to exist for a single job we have to add both
            // a job and a datafeed here
            Job randomJob = JobTests.createRandomizedJob();
            AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(randomJob.getAnalysisConfig());
            analysisConfig.setLatency(null);
            DatafeedConfig datafeedConfig = DatafeedConfigTests.createRandomizedDatafeedConfig(randomJob.getId(),
                    randomJob.getAnalysisConfig().getBucketSpan().millis());
            if (datafeedConfig.hasAggregations()) {
                analysisConfig.setSummaryCountFieldName("doc_count");
            }
            randomJob = new Job.Builder(randomJob).setAnalysisConfig(analysisConfig).build();
            metadataBuilder.putJob(randomJob, false);
            metadataBuilder.putDatafeed(datafeedConfig, Collections.emptyMap());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return metadataBuilder.build();
    }
}
