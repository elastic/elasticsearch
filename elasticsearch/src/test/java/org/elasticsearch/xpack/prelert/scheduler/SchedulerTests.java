/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.scheduler;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.support.AbstractSerializingTestCase;

public class SchedulerTests extends AbstractSerializingTestCase<Scheduler> {

    @Override
    protected Scheduler createTestInstance() {
        return new Scheduler(SchedulerConfigTests.createRandomizedSchedulerConfig(randomAsciiOfLength(10)),
                randomFrom(SchedulerStatus.values()));
    }

    @Override
    protected Writeable.Reader<Scheduler> instanceReader() {
        return Scheduler::new;
    }

    @Override
    protected Scheduler parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Scheduler.PARSER.apply(parser, () -> matcher);
    }
}