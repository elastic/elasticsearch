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
import org.elasticsearch.xpack.contextengine.events.SessionEvent;
import org.elasticsearch.xpack.contextengine.events.SessionEventTestUtils;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SaveEventActionRequestTests extends AbstractWireSerializingTestCase<SaveEventAction.Request> {

    @Override
    protected Writeable.Reader<SaveEventAction.Request> instanceReader() {
        return SaveEventAction.Request::new;
    }

    @Override
    protected SaveEventAction.Request createTestInstance() {
        return new SaveEventAction.Request(SessionEventTestUtils.randomSessionEvent());
    }

    @Override
    protected SaveEventAction.Request mutateInstance(SaveEventAction.Request instance) {
        return new SaveEventAction.Request(randomValueOtherThan(instance.event(), SessionEventTestUtils::randomSessionEvent));
    }

    public void testValidateValidRequest() {
        SessionEvent event = SessionEventTestUtils.randomSessionEvent().withIdAndTimestamp(null, null);
        assertThat(new SaveEventAction.Request(event).validate(), nullValue());
    }

    public void testValidateMissingRequiredFields() {
        SessionEvent event = new SessionEvent(null, null, null, null, "", null, null, "agent", "");
        ActionRequestValidationException e = new SaveEventAction.Request(event).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[session_id] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[user_id] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[transcript] cannot be null or empty"));
    }

    public void testValidateRejectsTimestamp() {
        SessionEvent event = SessionEventTestUtils.randomSessionEvent().withIdAndTimestamp(null, SessionEventTestUtils.randomTimestamp());
        ActionRequestValidationException e = new SaveEventAction.Request(event).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[@timestamp] is set at write time"));
    }
}
