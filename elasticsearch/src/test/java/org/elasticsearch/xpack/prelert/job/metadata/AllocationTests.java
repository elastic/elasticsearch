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
        String nodeId = randomAsciiOfLength(10);
        String jobId = randomAsciiOfLength(10);
        JobStatus jobStatus = randomFrom(JobStatus.values());
        SchedulerState schedulerState = new SchedulerState(JobSchedulerStatus.STARTING, randomPositiveLong(), randomPositiveLong());
        return new Allocation(nodeId, jobId, jobStatus, schedulerState);
    }

    @Override
    protected Writeable.Reader<Allocation> instanceReader() {
        return Allocation::new;
    }

    @Override
    protected Allocation parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Allocation.PARSER.apply(parser, () -> matcher).build();
    }

    @Override
    protected boolean skipJacksonTest() {
        return true;
    }
}
