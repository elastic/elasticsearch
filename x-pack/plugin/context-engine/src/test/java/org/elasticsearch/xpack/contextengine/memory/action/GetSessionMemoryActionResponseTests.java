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

public class GetSessionMemoryActionResponseTests extends AbstractWireSerializingTestCase<GetSessionMemoryAction.Response> {

    @Override
    protected Writeable.Reader<GetSessionMemoryAction.Response> instanceReader() {
        return GetSessionMemoryAction.Response::new;
    }

    @Override
    protected GetSessionMemoryAction.Response createTestInstance() {
        return new GetSessionMemoryAction.Response(randomList(0, 5, SessionMemoryTestUtils::randomSessionMemory));
    }

    @Override
    protected GetSessionMemoryAction.Response mutateInstance(GetSessionMemoryAction.Response instance) {
        return new GetSessionMemoryAction.Response(
            randomValueOtherThan(instance.memories(), () -> randomList(0, 5, SessionMemoryTestUtils::randomSessionMemory))
        );
    }
}
