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
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.List;

public class FlightSplitTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of(FlightSplit.ENTRY));

    public void testConstruction() {
        byte[] ticket = new byte[] { 1, 2, 3 };
        FlightSplit split = new FlightSplit(ticket, "grpc://host:1234", 5000);

        assertEquals("flight", split.sourceType());
        assertArrayEquals(ticket, split.ticketBytes());
        assertEquals("grpc://host:1234", split.location());
        assertEquals(5000, split.estimatedRows());
        assertEquals(-1, split.estimatedSizeInBytes());
    }

    public void testNullTicketThrows() {
        expectThrows(IllegalArgumentException.class, () -> new FlightSplit(null, null, 0));
    }

    public void testEmptyTicketThrows() {
        expectThrows(IllegalArgumentException.class, () -> new FlightSplit(new byte[0], null, 0));
    }

    public void testOversizedTicketThrows() {
        byte[] oversized = new byte[FlightSplit.MAX_TICKET_SIZE + 1];
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FlightSplit(oversized, null, 0));
        assertTrue(e.getMessage().contains("exceeds maximum"));
    }

    public void testMaxSizeTicketAccepted() {
        byte[] maxSize = new byte[FlightSplit.MAX_TICKET_SIZE];
        maxSize[0] = 1;
        FlightSplit split = new FlightSplit(maxSize, null, 0);
        assertEquals(FlightSplit.MAX_TICKET_SIZE, split.ticketBytes().length);
    }

    public void testNamedWriteableRoundTrip() throws IOException {
        FlightSplit original = new FlightSplit(new byte[] { 10, 20, 30, 40 }, "grpc://remote:9090", 42000);

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        FlightSplit deserialized = (FlightSplit) in.readNamedWriteable(ExternalSplit.class);

        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
        assertEquals(original.sourceType(), deserialized.sourceType());
        assertArrayEquals(original.ticketBytes(), deserialized.ticketBytes());
        assertEquals(original.location(), deserialized.location());
        assertEquals(original.estimatedRows(), deserialized.estimatedRows());
    }

    public void testNamedWriteableRoundTripNullLocation() throws IOException {
        FlightSplit original = new FlightSplit(new byte[] { 99 }, null, 0);

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(original);

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        FlightSplit deserialized = (FlightSplit) in.readNamedWriteable(ExternalSplit.class);

        assertEquals(original, deserialized);
        assertNull(deserialized.location());
    }

    public void testEquality() {
        byte[] ticket = new byte[] { 1, 2, 3 };
        FlightSplit a = new FlightSplit(ticket, "loc", 100);
        FlightSplit b = new FlightSplit(ticket.clone(), "loc", 100);
        FlightSplit c = new FlightSplit(ticket, "loc", 200);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    public void testTicketBytesDefensiveCopy() {
        byte[] ticket = new byte[] { 1, 2, 3 };
        FlightSplit split = new FlightSplit(ticket, null, 0);

        byte[] returned = split.ticketBytes();
        returned[0] = 99;

        assertEquals(1, split.ticketBytes()[0]);
    }

    public void testGetWriteableName() {
        FlightSplit split = new FlightSplit(new byte[] { 1 }, null, 0);
        assertEquals("FlightSplit", split.getWriteableName());
    }

    public void testToString() {
        FlightSplit split = new FlightSplit(new byte[] { 1, 2, 3 }, "grpc://host:1234", 5000);
        String str = split.toString();
        assertTrue(str.contains("3B"));
        assertTrue(str.contains("grpc://host:1234"));
        assertTrue(str.contains("5000"));
    }

    public void testUnknownRowEstimateSerializes() throws IOException {
        FlightSplit split = new FlightSplit(new byte[] { 1, 2, 3 }, null, -1);
        assertEquals(0, split.estimatedRows());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            split.writeTo(out);
            FlightSplit read = new FlightSplit(out.bytes().streamInput());
            assertEquals(split, read);
        }
    }

    public void testIsWithinTheDatasetResource() {
        StoragePath resource = StoragePath.of("flight://127.0.0.1:47490/employees");
        assertTrue("no location means the dataset's own endpoint", new FlightSplit(new byte[] { 1 }, null, 0).isWithin(resource));
        assertTrue(new FlightSplit(new byte[] { 1 }, "grpc://127.0.0.1:47490", 0).isWithin(resource));
        assertFalse("other port", new FlightSplit(new byte[] { 1 }, "grpc://127.0.0.1:47491", 0).isWithin(resource));
        assertFalse("other host", new FlightSplit(new byte[] { 1 }, "grpc://10.0.0.2:47490", 0).isWithin(resource));
        assertFalse("localhost is not 127.0.0.1", new FlightSplit(new byte[] { 1 }, "grpc://localhost:47490", 0).isWithin(resource));
    }

    public void testMalformedLocationIsOutsideTheResourceNotAnError() {
        StoragePath resource = StoragePath.of("flight://127.0.0.1:47490/employees");
        assertFalse(new FlightSplit(new byte[] { 1 }, "grpc://bad host:1", 0).isWithin(resource));
        assertFalse(new FlightSplit(new byte[] { 1 }, "::not a uri::", 0).isWithin(resource));
    }

    public void testDefaultPortIsCompared() {
        StoragePath resource = StoragePath.of("flight://127.0.0.1/employees");
        assertTrue(new FlightSplit(new byte[] { 1 }, "grpc://127.0.0.1:47470", 0).isWithin(resource));
        assertFalse(new FlightSplit(new byte[] { 1 }, "grpc://127.0.0.1:1", 0).isWithin(resource));
    }
}
