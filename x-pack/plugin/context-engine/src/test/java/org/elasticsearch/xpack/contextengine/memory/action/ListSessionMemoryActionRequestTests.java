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

public class ListSessionMemoryActionRequestTests extends AbstractWireSerializingTestCase<ListSessionMemoryAction.Request> {

    @Override
    protected Writeable.Reader<ListSessionMemoryAction.Request> instanceReader() {
        return ListSessionMemoryAction.Request::new;
    }

    @Override
    protected ListSessionMemoryAction.Request createTestInstance() {
        return new ListSessionMemoryAction.Request(
            randomBoolean() ? null : randomAlphaOfLength(12),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(6),
            randomIntBetween(1, 1000)
        );
    }

    @Override
    protected ListSessionMemoryAction.Request mutateInstance(ListSessionMemoryAction.Request instance) {
        return new ListSessionMemoryAction.Request(
            instance.sessionId(),
            instance.userId(),
            instance.agentId(),
            instance.space(),
            instance.aiIndexId(),
            randomValueOtherThan(instance.size(), () -> randomIntBetween(1, 1000))
        );
    }

    public void testValidate() {
        assertThat(createTestInstance().validate(), nullValue());
        assertThat(new ListSessionMemoryAction.Request(null, null, null, null, null, 10).validate(), nullValue());

        ActionRequestValidationException e = new ListSessionMemoryAction.Request(null, null, null, null, null, 0).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[size] must be between 1 and 1000"));

        e = new ListSessionMemoryAction.Request(null, null, null, null, null, 1001).validate();
        assertThat(e.getMessage(), containsString("[size] must be between 1 and 1000"));
    }
}
