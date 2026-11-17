/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.contextengine.memory.SessionMemoryTestUtils;

public class ListSessionMemoryActionResponseTests extends AbstractWireSerializingTestCase<ListSessionMemoryAction.Response> {

    @Override
    protected Writeable.Reader<ListSessionMemoryAction.Response> instanceReader() {
        return ListSessionMemoryAction.Response::new;
    }

    @Override
    protected ListSessionMemoryAction.Response createTestInstance() {
        return new ListSessionMemoryAction.Response(randomList(0, 5, SessionMemoryTestUtils::randomSessionMemory));
    }

    @Override
    protected ListSessionMemoryAction.Response mutateInstance(ListSessionMemoryAction.Response instance) {
        return new ListSessionMemoryAction.Response(
            randomValueOtherThan(instance.memories(), () -> randomList(0, 5, SessionMemoryTestUtils::randomSessionMemory))
        );
    }
}
