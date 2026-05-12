/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;

public class RemoteFetchServiceRequestTests extends AbstractWireSerializingTestCase<RemoteFetchService.Request> {
    @Override
    protected Writeable.Reader<RemoteFetchService.Request> instanceReader() {
        return RemoteFetchService.Request::new;
    }

    @Override
    protected RemoteFetchService.Request createTestInstance() {
        return new RemoteFetchService.Request(
            randomAlphaOfLengthBetween(5, 12),
            randomList(1, 5, this::randomField),
            randomList(0, 8, this::randomHandle),
            randomConfiguration()
        );
    }

    @Override
    protected RemoteFetchService.Request mutateInstance(RemoteFetchService.Request instance) throws IOException {
        return switch (between(0, 3)) {
            case 0 -> new RemoteFetchService.Request(
                randomAlphaOfLengthBetween(5, 12),
                instance.fields(),
                instance.handles(),
                instance.configuration()
            );
            case 1 -> new RemoteFetchService.Request(
                instance.sessionId(),
                randomList(1, 5, this::randomField),
                instance.handles(),
                instance.configuration()
            );
            case 2 -> new RemoteFetchService.Request(
                instance.sessionId(),
                instance.fields(),
                randomList(0, 8, this::randomHandle),
                instance.configuration()
            );
            default -> new RemoteFetchService.Request(
                instance.sessionId(),
                instance.fields(),
                instance.handles(),
                randomValueOtherThan(instance.configuration(), org.elasticsearch.xpack.esql.ConfigurationTestUtils::randomConfiguration)
            );
        };
    }

    private RemoteFetchService.FetchField randomField() {
        DataType dataType = randomFrom(DataType.KEYWORD, DataType.LONG, DataType.INTEGER, DataType.DOUBLE, DataType.BOOLEAN, DataType.TEXT);
        return new RemoteFetchService.FetchField(randomAlphaOfLengthBetween(3, 10), dataType);
    }

    private RemoteFetchHandle randomHandle() {
        return new RemoteFetchHandle(
            randomAlphaOfLengthBetween(5, 12),
            randomAlphaOfLengthBetween(5, 12),
            randomIntBetween(0, 32),
            randomIntBetween(0, 16),
            randomIntBetween(0, 4096)
        );
    }

    public void testValidateRejectsEmptyFields() {
        RemoteFetchService.Request request = new RemoteFetchService.Request(
            "session-1",
            List.of(),
            List.of(new RemoteFetchHandle("node-1", "session-1", 0, 0, 0)),
            randomConfiguration()
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> request.validateForNode("node-1"));
        assertEquals("remote fetch requires at least one field", e.getMessage());
    }

    public void testValidateRejectsSessionMismatch() {
        RemoteFetchService.Request request = new RemoteFetchService.Request(
            "session-1",
            List.of(new RemoteFetchService.FetchField("field", DataType.KEYWORD)),
            List.of(new RemoteFetchHandle("node-1", "session-2", 0, 0, 0)),
            randomConfiguration()
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> request.validateForNode("node-1"));
        assertEquals("remote fetch request session [session-1] does not match handle session [session-2]", e.getMessage());
    }

    public void testValidateRejectsNodeMismatch() {
        RemoteFetchService.Request request = new RemoteFetchService.Request(
            "session-1",
            List.of(new RemoteFetchService.FetchField("field", DataType.KEYWORD)),
            List.of(new RemoteFetchHandle("node-2", "session-1", 0, 0, 0)),
            randomConfiguration()
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> request.validateForNode("node-1"));
        assertEquals("remote fetch handle node [node-2] does not match local node [node-1]", e.getMessage());
    }
}
