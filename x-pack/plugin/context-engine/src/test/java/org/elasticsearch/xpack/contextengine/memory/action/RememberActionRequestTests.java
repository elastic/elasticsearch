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
import org.elasticsearch.xpack.contextengine.memory.SessionMemory;
import org.elasticsearch.xpack.contextengine.memory.SessionMemoryTestUtils;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RememberActionRequestTests extends AbstractWireSerializingTestCase<RememberAction.Request> {

    @Override
    protected Writeable.Reader<RememberAction.Request> instanceReader() {
        return RememberAction.Request::new;
    }

    @Override
    protected RememberAction.Request createTestInstance() {
        return new RememberAction.Request(SessionMemoryTestUtils.randomSessionMemory());
    }

    @Override
    protected RememberAction.Request mutateInstance(RememberAction.Request instance) {
        return new RememberAction.Request(randomValueOtherThan(instance.memory(), SessionMemoryTestUtils::randomSessionMemory));
    }

    public void testValidateValidRequest() {
        SessionMemory memory = SessionMemoryTestUtils.randomSessionMemory().withIdAndTimestamp(null, null);
        assertThat(new RememberAction.Request(memory).validate(), nullValue());
    }

    public void testValidateMissingRequiredFields() {
        SessionMemory memory = new SessionMemory(null, null, null, null, "", null, "", null, null);
        ActionRequestValidationException e = new RememberAction.Request(memory).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[session_id] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[user_id] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("one of [content] or [media] must be provided"));
    }

    public void testValidateMediaOnly() {
        SessionMemory memory = new SessionMemory(null, null, null, null, "s", "u", null, SessionMemoryTestUtils.randomMedia(), null);
        assertThat(new RememberAction.Request(memory).validate(), nullValue());
    }

    public void testValidateMediaItems() {
        SessionMemory memory = new SessionMemory(
            null,
            null,
            null,
            null,
            "s",
            "u",
            null,
            List.of(Map.of("value", "data:image/png;base64,abc"), Map.of("type", "image")),
            null
        );
        ActionRequestValidationException e = new RememberAction.Request(memory).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[media] items must have a non-empty [type]"));
        assertThat(e.getMessage(), containsString("[media] items must have a non-empty [value]"));
    }

    public void testValidateMediaTypeAndFormat() {
        SessionMemory memory = new SessionMemory(
            null,
            null,
            null,
            null,
            "s",
            "u",
            null,
            List.of(Map.of("type", "gif", "value", "abc", "format", "hex")),
            null
        );
        ActionRequestValidationException e = new RememberAction.Request(memory).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[media] Unrecognized type [gif]"));
        assertThat(e.getMessage(), containsString("[media] Unrecognized format [hex]"));
    }

    public void testValidateRejectsTimestamp() {
        SessionMemory memory = SessionMemoryTestUtils.randomSessionMemory()
            .withIdAndTimestamp(null, SessionMemoryTestUtils.randomTimestamp());
        ActionRequestValidationException e = new RememberAction.Request(memory).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[@timestamp] is set at write time"));
    }
}
