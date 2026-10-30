/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class SessionMemoryTests extends AbstractXContentSerializingTestCase<SessionMemory> {

    @Override
    protected SessionMemory doParseInstance(XContentParser parser) throws IOException {
        return SessionMemory.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<SessionMemory> instanceReader() {
        return SessionMemory::new;
    }

    @Override
    protected SessionMemory createTestInstance() {
        return SessionMemoryTestUtils.randomSessionMemory();
    }

    @Override
    protected SessionMemory mutateInstance(SessionMemory instance) {
        return new SessionMemory(
            instance.timestamp(),
            instance.id(),
            instance.space(),
            instance.aiIndexIds(),
            randomValueOtherThan(instance.sessionId(), () -> randomAlphaOfLength(12)),
            instance.userId(),
            instance.agentId(),
            instance.type(),
            instance.content(),
            instance.attributes()
        );
    }

    public void testWithIdAndTimestamp() {
        SessionMemory memory = SessionMemoryTestUtils.randomSessionMemory();
        var timestamp = SessionMemoryTestUtils.randomTimestamp();
        SessionMemory stored = memory.withIdAndTimestamp("the-id", timestamp);
        assertEquals("the-id", stored.id());
        assertEquals(timestamp, stored.timestamp());
        assertEquals(memory.sessionId(), stored.sessionId());
        assertEquals(memory.content(), stored.content());
    }
}
