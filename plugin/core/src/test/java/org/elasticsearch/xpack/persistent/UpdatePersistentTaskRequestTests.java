/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.Status;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.UpdatePersistentTaskStatusAction.Request;

import java.util.Collections;

public class UpdatePersistentTaskRequestTests extends AbstractStreamableTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(UUIDs.base64UUID(), randomLong(), new Status(randomAlphaOfLength(10)));
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(
                new NamedWriteableRegistry.Entry(Task.Status.class, TestPersistentTasksExecutor.NAME, Status::new)
        ));
    }
}