/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.xpack.persistent.StartPersistentTaskAction.Request;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.test.AbstractStreamableTestCase;

import java.util.Collections;

public class StartPersistentActionRequestTests extends AbstractStreamableTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        TestParams testParams;
        if (randomBoolean()) {
            testParams = new TestParams();
            if (randomBoolean()) {
                testParams.setTestParam(randomAlphaOfLengthBetween(1, 20));
            }
            if (randomBoolean()) {
                testParams.setExecutorNodeAttr(randomAlphaOfLengthBetween(1, 20));
            }
        } else {
            testParams = null;
        }
        return new Request(UUIDs.base64UUID(), randomAlphaOfLengthBetween(1, 20), testParams);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(
                new Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new)
        ));
    }
}