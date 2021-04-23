/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;


public class PersistentTasksExecutorResponseTests extends AbstractWireSerializingTestCase<PersistentTaskResponse> {

    @Override
    protected PersistentTaskResponse createTestInstance() {
        if (randomBoolean()) {
            return new PersistentTaskResponse(
                    new PersistentTask<PersistentTaskParams>(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME,
                            new TestPersistentTasksPlugin.TestParams("test"),
                            randomLong(), PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT));
        } else {
            return new PersistentTaskResponse((PersistentTask<?>) null);
        }
    }

    @Override
    protected Writeable.Reader<PersistentTaskResponse> instanceReader() {
        return PersistentTaskResponse::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class,
                        TestPersistentTasksExecutor.NAME, TestPersistentTasksPlugin.TestParams::new)
        ));
    }
}
