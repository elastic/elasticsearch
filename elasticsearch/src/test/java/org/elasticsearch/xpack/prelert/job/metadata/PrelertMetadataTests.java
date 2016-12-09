/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.JobTests;
import org.elasticsearch.xpack.prelert.job.SchedulerStatus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobRunnerTests.createScheduledJob;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PrelertMetadataTests extends ESTestCase {

    public void testSerialization() throws Exception {
        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();

        Job job1 = JobTests.createRandomizedJob();
        Job job2 = JobTests.createRandomizedJob();
        Job job3 = JobTests.createRandomizedJob();

        builder.putJob(job1, false);
        builder.putJob(job2, false);
        builder.putJob(job3, false);

        builder.updateStatus(job1.getId(), JobStatus.OPENING, null);
        builder.assignToNode(job2.getId(), "node1");
        builder.updateStatus(job2.getId(), JobStatus.OPENING, null);
        builder.assignToNode(job3.getId(), "node1");
        builder.updateStatus(job3.getId(), JobStatus.OPENING, null);

        PrelertMetadata expected = builder.build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        expected.writeTo(new OutputStreamStreamOutput(out));

        PrelertMetadata result = (PrelertMetadata)
                PrelertMetadata.PROTO.readFrom(new InputStreamStreamInput(new ByteArrayInputStream(out.toByteArray())));
        assertThat(result, equalTo(expected));
    }

    public void testFromXContent() throws IOException {
        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();

        Job job1 = JobTests.createRandomizedJob();
        Job job2 = JobTests.createRandomizedJob();
        Job job3 = JobTests.createRandomizedJob();

        builder.putJob(job1, false);
        builder.putJob(job2, false);
        builder.putJob(job3, false);

        builder.updateStatus(job1.getId(), JobStatus.OPENING, null);
        builder.assignToNode(job1.getId(), "node1");
        builder.updateStatus(job2.getId(), JobStatus.OPENING, null);
        builder.assignToNode(job2.getId(), "node1");
        builder.updateStatus(job3.getId(), JobStatus.OPENING, null);
        builder.assignToNode(job3.getId(), "node1");

        PrelertMetadata expected = builder.build();

        XContentBuilder xBuilder = XContentFactory.contentBuilder(XContentType.SMILE);
        xBuilder.prettyPrint();
        xBuilder.startObject();
        expected.toXContent(xBuilder, ToXContent.EMPTY_PARAMS);
        xBuilder.endObject();
        XContentBuilder shuffled = shuffleXContent(xBuilder);
        final XContentParser parser = XContentFactory.xContent(shuffled.bytes()).createParser(shuffled.bytes());
        MetaData.Custom custom = expected.fromXContent(parser);
        assertTrue(custom instanceof PrelertMetadata);
        PrelertMetadata result = (PrelertMetadata) custom;
        assertThat(result, equalTo(expected));
    }

    public void testPutJob() {
        Job job1 = buildJobBuilder("1").build();
        Job job2 = buildJobBuilder("2").build();

        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();
        builder.putJob(job1, false);
        builder.putJob(job2, false);

        PrelertMetadata result = builder.build();
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getAllocations().get("1").getStatus(), equalTo(JobStatus.CLOSED));
        assertThat(result.getSchedulerStatuses().get("1"), nullValue());
        assertThat(result.getJobs().get("2"), sameInstance(job2));
        assertThat(result.getAllocations().get("2").getStatus(), equalTo(JobStatus.CLOSED));
        assertThat(result.getSchedulerStatuses().get("2"), nullValue());

        builder = new PrelertMetadata.Builder(result);

        PrelertMetadata.Builder builderReference = builder;
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
        Job job1 = buildJobBuilder("1").build();
        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();
        builder.putJob(job1, false);

        PrelertMetadata result = builder.build();
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getAllocations().get("1").getStatus(), equalTo(JobStatus.CLOSED));
        assertThat(result.getSchedulerStatuses().get("1"), nullValue());

        builder = new PrelertMetadata.Builder(result);
        builder.removeJob("1");
        result = builder.build();
        assertThat(result.getJobs().get("1"), nullValue());
        assertThat(result.getAllocations().get("1"), nullValue());
        assertThat(result.getSchedulerStatuses().get("1"), nullValue());
    }

    public void testRemoveJob_failBecauseJobIsOpen() {
        Job job1 = buildJobBuilder("1").build();
        PrelertMetadata.Builder builder1 = new PrelertMetadata.Builder();
        builder1.putJob(job1, false);
        builder1.updateStatus("1", JobStatus.OPENING, null);
        builder1.updateStatus("1", JobStatus.OPENED, null);

        PrelertMetadata result = builder1.build();
        assertThat(result.getJobs().get("1"), sameInstance(job1));
        assertThat(result.getAllocations().get("1").getStatus(), equalTo(JobStatus.OPENED));
        assertThat(result.getSchedulerStatuses().get("1"), nullValue());

        PrelertMetadata.Builder builder2 = new PrelertMetadata.Builder(result);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> builder2.removeJob("1"));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

    public void testRemoveJob_failBecauseJobDoesNotExist() {
        PrelertMetadata.Builder builder1 = new PrelertMetadata.Builder();
        expectThrows(ResourceNotFoundException.class, () -> builder1.removeJob("1"));
    }

    public void testCrudScheduledJob() {
        Job job1 = createScheduledJob().build();
        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();
        builder.putJob(job1, false);

        PrelertMetadata result = builder.build();
        assertThat(result.getJobs().get("foo"), sameInstance(job1));
        assertThat(result.getAllocations().get("foo").getStatus(), equalTo(JobStatus.CLOSED));
        assertThat(result.getSchedulerStatuses().get("foo"), equalTo(SchedulerStatus.STOPPED));

        builder = new PrelertMetadata.Builder(result);
        builder.removeJob("foo");
        result = builder.build();
        assertThat(result.getJobs().get("foo"), nullValue());
        assertThat(result.getAllocations().get("foo"), nullValue());
        assertThat(result.getSchedulerStatuses().get("foo"), nullValue());
    }

    public void testDeletedScheduledJob_failBecauseSchedulerStarted() {
        Job job1 = createScheduledJob().build();
        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();
        builder.putJob(job1, false);
        builder.updateStatus("foo", JobStatus.OPENING, null);
        builder.updateStatus("foo", JobStatus.OPENED, null);
        builder.updateSchedulerStatus("foo", SchedulerStatus.STARTED);

        PrelertMetadata result = builder.build();
        assertThat(result.getJobs().get("foo"), sameInstance(job1));
        assertThat(result.getAllocations().get("foo").getStatus(), equalTo(JobStatus.OPENED));
        assertThat(result.getSchedulerStatuses().get("foo"), equalTo(SchedulerStatus.STARTED));

        PrelertMetadata.Builder builder2 = new PrelertMetadata.Builder(result);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> builder2.removeJob("foo"));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
    }

    public void testUpdateAllocation_setFinishedTime() {
        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();
        builder.putJob(buildJobBuilder("_job_id").build(), false);
        builder.updateStatus("_job_id", JobStatus.OPENING, null);

        builder.updateStatus("_job_id", JobStatus.OPENED, null);
        PrelertMetadata prelertMetadata = builder.build();
        assertThat(prelertMetadata.getJobs().get("_job_id").getFinishedTime(), nullValue());

        builder.updateStatus("_job_id", JobStatus.CLOSED, null);
        prelertMetadata = builder.build();
        assertThat(prelertMetadata.getJobs().get("_job_id").getFinishedTime(), notNullValue());
    }

}
