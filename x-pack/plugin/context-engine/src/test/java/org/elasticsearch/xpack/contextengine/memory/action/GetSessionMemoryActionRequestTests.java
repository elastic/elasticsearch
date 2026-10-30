/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetSessionMemoryActionRequestTests extends AbstractWireSerializingTestCase<GetSessionMemoryAction.Request> {

    @Override
    protected Writeable.Reader<GetSessionMemoryAction.Request> instanceReader() {
        return GetSessionMemoryAction.Request::new;
    }

    @Override
    protected GetSessionMemoryAction.Request createTestInstance() {
        return new GetSessionMemoryAction.Request(
            randomAlphaOfLength(12),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(6),
            randomIntBetween(1, 1000)
        );
    }

    @Override
    protected GetSessionMemoryAction.Request mutateInstance(GetSessionMemoryAction.Request instance) {
        return new GetSessionMemoryAction.Request(
            randomValueOtherThan(instance.sessionId(), () -> randomAlphaOfLength(12)),
            instance.space(),
            instance.aiIndexId(),
            instance.size()
        );
    }

    public void testValidate() {
        assertThat(createTestInstance().validate(), nullValue());

        ActionRequestValidationException e = new GetSessionMemoryAction.Request("", null, null, 0).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[session_id] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[size] must be greater than 0"));
    }
}
