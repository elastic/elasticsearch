/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * An {@link ExternalSplit} representing a single Arrow Flight endpoint.
 *
 * The {@code ticketBytes} field is an opaque token returned by the Flight
 * server inside a {@code FlightEndpoint}. It is not data — it is the
 * server's handle for identifying which stream to serve when the client
 * calls {@code DoGet(Ticket)}. Typical tickets are short (UUIDs, query
 * IDs, encoded partition references — tens to hundreds of bytes). The
 * constructor enforces a {@link #MAX_TICKET_SIZE} cap to prevent
 * oversized tokens from being shipped across the cluster.
 */
public class FlightSplit implements ExternalSplit {

    static final int MAX_TICKET_SIZE = 16 * 1024;

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        ExternalSplit.class,
        "FlightSplit",
        FlightSplit::new
    );

    private final byte[] ticketBytes;
    private final String location;
    private final long estimatedRows;

    public FlightSplit(byte[] ticketBytes, String location, long estimatedRows) {
        if (ticketBytes == null || ticketBytes.length == 0) {
            throw new IllegalArgumentException("ticketBytes must not be null or empty");
        }
        if (ticketBytes.length > MAX_TICKET_SIZE) {
            throw new IllegalArgumentException("ticketBytes length [" + ticketBytes.length + "] exceeds maximum [" + MAX_TICKET_SIZE + "]");
        }
        this.ticketBytes = ticketBytes;
        this.location = location;
        this.estimatedRows = estimatedRows;
    }

    public FlightSplit(StreamInput in) throws IOException {
        this.ticketBytes = in.readByteArray();
        this.location = in.readOptionalString();
        this.estimatedRows = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(ticketBytes);
        out.writeOptionalString(location);
        out.writeVLong(estimatedRows);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String sourceType() {
        return "flight";
    }

    @Override
    public long estimatedSizeInBytes() {
        return -1;
    }

    public byte[] ticketBytes() {
        return ticketBytes.clone();
    }

    public String location() {
        return location;
    }

    public long estimatedRows() {
        return estimatedRows;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlightSplit that = (FlightSplit) o;
        return estimatedRows == that.estimatedRows
            && Arrays.equals(ticketBytes, that.ticketBytes)
            && Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(location, estimatedRows);
        result = 31 * result + Arrays.hashCode(ticketBytes);
        return result;
    }

    @Override
    public String toString() {
        return "FlightSplit[ticket=" + ticketBytes.length + "B, location=" + location + ", rows=" + estimatedRows + "]";
    }
}
