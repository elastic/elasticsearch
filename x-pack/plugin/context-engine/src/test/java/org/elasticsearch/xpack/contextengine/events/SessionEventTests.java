/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class SessionEventTests extends AbstractXContentSerializingTestCase<SessionEvent> {

    @Override
    protected SessionEvent doParseInstance(XContentParser parser) throws IOException {
        return SessionEvent.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<SessionEvent> instanceReader() {
        return SessionEvent::new;
    }

    @Override
    protected SessionEvent createTestInstance() {
        return SessionEventTestUtils.randomSessionEvent();
    }

    @Override
    protected SessionEvent mutateInstance(SessionEvent instance) {
        return new SessionEvent(
            instance.timestamp(),
            instance.id(),
            instance.space(),
            instance.aiIndexIds(),
            randomValueOtherThan(instance.sessionId(), () -> randomAlphaOfLength(12)),
            instance.roundId(),
            instance.userId(),
            instance.transcript()
        );
    }

    public void testWithIdAndTimestamp() {
        SessionEvent event = SessionEventTestUtils.randomSessionEvent();
        var timestamp = SessionEventTestUtils.randomTimestamp();
        SessionEvent stored = event.withIdAndTimestamp("the-id", timestamp);
        assertEquals("the-id", stored.id());
        assertEquals(timestamp, stored.timestamp());
        assertEquals(event.sessionId(), stored.sessionId());
        assertEquals(event.transcript(), stored.transcript());
    }
}
