/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.support.AbstractSerializingTestCase;

public class AllocationTests extends AbstractSerializingTestCase<Allocation> {

    @Override
    protected Allocation createTestInstance() {
        String nodeId = randomBoolean() ? randomAsciiOfLength(10) : null;
        String jobId = randomAsciiOfLength(10);
        boolean ignoreDowntime = randomBoolean();
        JobStatus jobStatus = randomFrom(JobStatus.values());
        String statusReason = randomBoolean() ? randomAsciiOfLength(10) : null;
        SchedulerState schedulerState = new SchedulerState(JobSchedulerStatus.STARTING, randomPositiveLong(), randomPositiveLong());
        return new Allocation(nodeId, jobId, ignoreDowntime, jobStatus, statusReason, schedulerState);
    }

    @Override
    protected Writeable.Reader<Allocation> instanceReader() {
        return Allocation::new;
    }

    @Override
    protected Allocation parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Allocation.PARSER.apply(parser, () -> matcher).build();
    }

    public void testUnsetIgnoreDownTime() {
        Allocation allocation = new Allocation("_node_id", "_job_id", true, JobStatus.OPENING, null, null);
        assertTrue(allocation.isIgnoreDowntime());
        Allocation.Builder builder = new Allocation.Builder(allocation);
        builder.setStatus(JobStatus.OPENED);
        allocation = builder.build();
        assertFalse(allocation.isIgnoreDowntime());
    }

}
