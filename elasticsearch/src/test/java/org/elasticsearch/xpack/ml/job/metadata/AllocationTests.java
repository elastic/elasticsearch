/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.JobStatus;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

public class AllocationTests extends AbstractSerializingTestCase<Allocation> {

    @Override
    protected Allocation createTestInstance() {
        String nodeId = randomBoolean() ? randomAsciiOfLength(10) : null;
        String jobId = randomAsciiOfLength(10);
        boolean ignoreDowntime = randomBoolean();
        JobStatus jobStatus = randomFrom(JobStatus.values());
        String statusReason = randomBoolean() ? randomAsciiOfLength(10) : null;
        return new Allocation(nodeId, jobId, ignoreDowntime, jobStatus, statusReason);
    }

    @Override
    protected Writeable.Reader<Allocation> instanceReader() {
        return Allocation::new;
    }

    @Override
    protected Allocation parseInstance(XContentParser parser) {
        return Allocation.PARSER.apply(parser, null).build();
    }

    public void testUnsetIgnoreDownTime() {
        Allocation allocation = new Allocation("_node_id", "_job_id", true, JobStatus.OPENING, null);
        assertTrue(allocation.isIgnoreDowntime());
        Allocation.Builder builder = new Allocation.Builder(allocation);
        builder.setStatus(JobStatus.OPENED);
        allocation = builder.build();
        assertFalse(allocation.isIgnoreDowntime());
    }

}
