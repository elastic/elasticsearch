/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetSessionEventsActionRequestTests extends AbstractWireSerializingTestCase<GetSessionEventsAction.Request> {

    @Override
    protected Writeable.Reader<GetSessionEventsAction.Request> instanceReader() {
        return GetSessionEventsAction.Request::new;
    }

    @Override
    protected GetSessionEventsAction.Request createTestInstance() {
        return new GetSessionEventsAction.Request(
            randomAlphaOfLength(12),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomList(1, 3, () -> randomAlphaOfLength(6)),
            randomIntBetween(1, 1000)
        );
    }

    @Override
    protected GetSessionEventsAction.Request mutateInstance(GetSessionEventsAction.Request instance) {
        return new GetSessionEventsAction.Request(
            randomValueOtherThan(instance.sessionId(), () -> randomAlphaOfLength(12)),
            instance.userId(),
            instance.space(),
            instance.aiIndexIds(),
            instance.size()
        );
    }

    public void testValidate() {
        assertThat(createTestInstance().validate(), nullValue());

        ActionRequestValidationException e = new GetSessionEventsAction.Request("", null, null, null, 0).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[session_id] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[size] must be between 1 and 1000"));
    }
}
