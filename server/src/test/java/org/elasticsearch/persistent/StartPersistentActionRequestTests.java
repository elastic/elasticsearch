/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.StartPersistentTaskAction.Request;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;

public class StartPersistentActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomTimeValue(), UUIDs.base64UUID(), randomAlphaOfLengthBetween(1, 20), randomTestParams());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String taskId = instance.getTaskId();
        String taskName = instance.getTaskName();
        PersistentTaskParams params = instance.getParams();
        switch (between(0, 2)) {
            case 0 -> taskId = randomValueOtherThan(taskId, UUIDs::base64UUID);
            case 1 -> taskName = randomValueOtherThan(taskName, () -> randomAlphaOfLengthBetween(1, 20));
            case 2 -> params = randomValueOtherThan(params, StartPersistentActionRequestTests::randomTestParams);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new Request(instance.masterNodeTimeout(), taskId, taskName, params);
    }

    private static TestParams randomTestParams() {
        TestParams testParams = new TestParams();
        if (randomBoolean()) {
            testParams.setTestParam(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            testParams.setExecutorNodeAttr(randomAlphaOfLengthBetween(1, 20));
        }
        return testParams;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(new Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new))
        );
    }
}
