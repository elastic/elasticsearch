/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.BroadcastMessageAction.NodeRequest;

import java.io.IOException;

/**
 * Guards the symmetry of {@link BroadcastMessageAction.NodeRequest}'s wire format. An earlier version
 * wrote the parent task id (inherited {@code writeTo}) without ever reading it back, and never wrote
 * the message at all: remote nodes were left with unread bytes, which the transport layer escalates to
 * a fatal error when assertions are enabled, killing the receiving node. A non-empty test message
 * proves the payload itself crosses the wire.
 */
public class BroadcastMessageActionNodeRequestTests extends AbstractBWCWireSerializationTestCase<
    NodeRequest<BroadcastMessageActionNodeRequestTests.TestMessage>> {

    record TestMessage(String value) implements Writeable {
        TestMessage(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }

    @Override
    protected Writeable.Reader<NodeRequest<TestMessage>> instanceReader() {
        return in -> new NodeRequest<>(in, TestMessage::new);
    }

    @Override
    protected NodeRequest<TestMessage> createTestInstance() {
        var request = new NodeRequest<>(new TestMessage(randomAlphaOfLengthBetween(1, 20)));
        if (randomBoolean()) {
            // Node-level requests carry the parent (coordinating) task id; it must round-trip too.
            request.setParentTask(new TaskId(randomAlphaOfLength(22), randomNonNegativeLong()));
        }
        return request;
    }

    @Override
    protected NodeRequest<TestMessage> mutateInstance(NodeRequest<TestMessage> instance) {
        var mutated = new NodeRequest<>(new TestMessage(randomValueOtherThan(instance.message().value(), () -> randomAlphaOfLength(21))));
        mutated.setParentTask(instance.getParentTask());
        return mutated;
    }

    @Override
    protected NodeRequest<TestMessage> mutateInstanceForVersion(NodeRequest<TestMessage> instance, TransportVersion version) {
        // The wire format has no version-dependent fields.
        return instance;
    }
}
