/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class JobTaskStatusTests extends AbstractSerializingTestCase<JobTaskStatus> {

    @Override
    protected JobTaskStatus createTestInstance() {
        return new JobTaskStatus(randomFrom(JobState.values()), randomLong());
    }

    @Override
    protected Writeable.Reader<JobTaskStatus> instanceReader() {
        return JobTaskStatus::new;
    }

    @Override
    protected JobTaskStatus doParseInstance(XContentParser parser) {
        return JobTaskStatus.fromXContent(parser);
    }
}
