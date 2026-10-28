/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Verifies that a collection of {@link FlightSplit} instances survives a
 * {@code writeNamedWriteableCollection} / {@code readNamedWriteableCollectionAsList}
 * round-trip, which is the same serialization path used by {@code DataNodeRequest}.
 */
public class FlightSplitCollectionSerializationTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of(FlightSplit.ENTRY));

    public void testCollectionRoundTrip() throws IOException {
        List<ExternalSplit> original = new ArrayList<>();
        original.add(new FlightSplit("employees-part-0".getBytes(StandardCharsets.UTF_8), "grpc://host1:9090", 25));
        original.add(new FlightSplit("employees-part-1".getBytes(StandardCharsets.UTF_8), "grpc://host2:9090", 25));
        original.add(new FlightSplit("employees-part-2".getBytes(StandardCharsets.UTF_8), null, 50));

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteableCollection(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        List<ExternalSplit> deserialized = in.readNamedWriteableCollectionAsList(ExternalSplit.class);

        assertEquals(original.size(), deserialized.size());
        for (int i = 0; i < original.size(); i++) {
            FlightSplit orig = (FlightSplit) original.get(i);
            FlightSplit deser = (FlightSplit) deserialized.get(i);
            assertEquals(orig, deser);
            assertArrayEquals(orig.ticketBytes(), deser.ticketBytes());
            assertEquals(orig.location(), deser.location());
            assertEquals(orig.estimatedRows(), deser.estimatedRows());
        }
    }

    public void testEmptyCollectionRoundTrip() throws IOException {
        List<ExternalSplit> original = List.of();

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteableCollection(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        List<ExternalSplit> deserialized = in.readNamedWriteableCollectionAsList(ExternalSplit.class);

        assertEquals(0, deserialized.size());
    }

    public void testSingleSplitCollectionRoundTrip() throws IOException {
        List<ExternalSplit> original = List.of(new FlightSplit("ticket-abc".getBytes(StandardCharsets.UTF_8), "grpc://remote:47470", 1000));

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteableCollection(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        List<ExternalSplit> deserialized = in.readNamedWriteableCollectionAsList(ExternalSplit.class);

        assertEquals(1, deserialized.size());
        assertEquals(original.get(0), deserialized.get(0));
    }

    public void testLargeTicketRoundTrip() throws IOException {
        byte[] largeTicket = new byte[FlightSplit.MAX_TICKET_SIZE];
        for (int i = 0; i < largeTicket.length; i++) {
            largeTicket[i] = (byte) (i % 256);
        }
        List<ExternalSplit> original = List.of(new FlightSplit(largeTicket, "grpc://host:1234", 999));

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteableCollection(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        List<ExternalSplit> deserialized = in.readNamedWriteableCollectionAsList(ExternalSplit.class);

        assertEquals(1, deserialized.size());
        FlightSplit deser = (FlightSplit) deserialized.get(0);
        assertArrayEquals(largeTicket, deser.ticketBytes());
    }
}
