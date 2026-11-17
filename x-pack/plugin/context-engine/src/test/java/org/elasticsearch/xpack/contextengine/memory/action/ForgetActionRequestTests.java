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

public class ForgetActionRequestTests extends AbstractWireSerializingTestCase<ForgetAction.Request> {

    @Override
    protected Writeable.Reader<ForgetAction.Request> instanceReader() {
        return ForgetAction.Request::new;
    }

    @Override
    protected ForgetAction.Request createTestInstance() {
        return new ForgetAction.Request(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(12),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(8)
        );
    }

    @Override
    protected ForgetAction.Request mutateInstance(ForgetAction.Request instance) {
        return new ForgetAction.Request(
            randomValueOtherThan(instance.id(), () -> randomAlphaOfLength(10)),
            instance.sessionId(),
            instance.userId(),
            instance.space()
        );
    }

    public void testValidate() {
        assertThat(createTestInstance().validate(), nullValue());

        ActionRequestValidationException e = new ForgetAction.Request("", null, null, null).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[id] cannot be null or empty"));
    }
}
