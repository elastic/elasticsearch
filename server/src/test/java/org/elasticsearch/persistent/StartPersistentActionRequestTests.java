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
        TestParams testParams = new TestParams();
        if (randomBoolean()) {
            testParams.setTestParam(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            testParams.setExecutorNodeAttr(randomAlphaOfLengthBetween(1, 20));
        }
        return new Request(randomTimeValue(), UUIDs.base64UUID(), randomAlphaOfLengthBetween(1, 20), testParams);
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
