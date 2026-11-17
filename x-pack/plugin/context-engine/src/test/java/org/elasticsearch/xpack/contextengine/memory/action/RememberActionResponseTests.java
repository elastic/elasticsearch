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

public class RememberActionResponseTests extends AbstractWireSerializingTestCase<RememberAction.Response> {

    @Override
    protected Writeable.Reader<RememberAction.Response> instanceReader() {
        return RememberAction.Response::new;
    }

    @Override
    protected RememberAction.Response createTestInstance() {
        return new RememberAction.Response(SessionMemoryTestUtils.randomSessionMemory());
    }

    @Override
    protected RememberAction.Response mutateInstance(RememberAction.Response instance) {
        return new RememberAction.Response(randomValueOtherThan(instance.memory(), SessionMemoryTestUtils::randomSessionMemory));
    }
}
