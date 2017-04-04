/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.util.Collections;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiOfLength;

public class PersistentTasksExecutorResponseTests extends AbstractStreamableTestCase<PersistentTaskResponse> {

    @Override
    protected PersistentTaskResponse createTestInstance() {
        if (randomBoolean()) {
            return new PersistentTaskResponse(
                    new PersistentTask<PersistentTaskRequest>(randomLong(), randomAsciiOfLength(10),
                            new TestPersistentTasksPlugin.TestRequest("test"),
                            PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT));
        } else {
            return new PersistentTaskResponse(null);
        }
    }

    @Override
    protected PersistentTaskResponse createBlankInstance() {
        return new PersistentTaskResponse();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(
                new NamedWriteableRegistry.Entry(PersistentTaskRequest.class, TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME, TestPersistentTasksPlugin.TestRequest::new)
        ));
    }
}