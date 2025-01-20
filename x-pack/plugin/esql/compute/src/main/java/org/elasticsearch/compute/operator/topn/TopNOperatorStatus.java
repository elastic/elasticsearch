/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class TopNOperatorStatus implements Operator.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Operator.Status.class,
        "topn",
        TopNOperatorStatus::new
    );
    private final int occupiedRows;
    private final long ramBytesUsed;
    private final int pagesReceived;
    private final int pagesEmitted;
    private final long rowsReceived;
    private final long rowsEmitted;

    public TopNOperatorStatus(
        int occupiedRows,
        long ramBytesUsed,
        int pagesReceived,
        int pagesEmitted,
        long rowsReceived,
        long rowsEmitted
    ) {
        this.occupiedRows = occupiedRows;
        this.ramBytesUsed = ramBytesUsed;
        this.pagesReceived = pagesReceived;
        this.pagesEmitted = pagesEmitted;
        this.rowsReceived = rowsReceived;
        this.rowsEmitted = rowsEmitted;
    }

    TopNOperatorStatus(StreamInput in) throws IOException {
        this.occupiedRows = in.readVInt();
        this.ramBytesUsed = in.readVLong();

        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
            this.pagesReceived = in.readVInt();
            this.pagesEmitted = in.readVInt();
            this.rowsReceived = in.readVLong();
            this.rowsEmitted = in.readVLong();
        } else {
            this.pagesReceived = 0;
            this.pagesEmitted = 0;
            this.rowsReceived = 0;
            this.rowsEmitted = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(occupiedRows);
        out.writeVLong(ramBytesUsed);

        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
            out.writeVInt(pagesReceived);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public int occupiedRows() {
        return occupiedRows;
    }

    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    public int pagesReceived() {
        return pagesReceived;
    }

    public int pagesEmitted() {
        return pagesEmitted;
    }

    public long rowsReceived() {
        return rowsReceived;
    }

    public long rowsEmitted() {
        return rowsEmitted;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("occupied_rows", occupiedRows);
        builder.field("ram_bytes_used", ramBytesUsed);
        builder.field("ram_used", ByteSizeValue.ofBytes(ramBytesUsed));
        builder.field("pages_received", pagesReceived);
        builder.field("pages_emitted", pagesEmitted);
        builder.field("rows_received", rowsReceived);
        builder.field("rows_emitted", rowsEmitted);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopNOperatorStatus that = (TopNOperatorStatus) o;
        return occupiedRows == that.occupiedRows
            && ramBytesUsed == that.ramBytesUsed
            && pagesReceived == that.pagesReceived
            && pagesEmitted == that.pagesEmitted
            && rowsReceived == that.rowsReceived
            && rowsEmitted == that.rowsEmitted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(occupiedRows, ramBytesUsed, pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_11_X;
    }
}
