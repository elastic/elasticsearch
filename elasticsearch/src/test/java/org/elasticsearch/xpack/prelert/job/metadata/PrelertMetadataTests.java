/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.JobTests;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
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
        assertThat(result.getJobs().get("2"), sameInstance(job2));

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
