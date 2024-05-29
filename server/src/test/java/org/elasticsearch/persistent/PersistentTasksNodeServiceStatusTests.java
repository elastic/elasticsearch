/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.PersistentTasksNodeService.Status;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;

public class PersistentTasksNodeServiceStatusTests extends AbstractWireSerializingTestCase<Status> {

    @Override
    protected Status createTestInstance() {
        return new Status(randomFrom(AllocatedPersistentTask.State.values()));
    }

    @Override
    protected Status mutateInstance(Status instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Status> instanceReader() {
        return Status::new;
    }

    public void testToString() {
        assertThat(createTestInstance().toString(), containsString("state"));
    }
}
