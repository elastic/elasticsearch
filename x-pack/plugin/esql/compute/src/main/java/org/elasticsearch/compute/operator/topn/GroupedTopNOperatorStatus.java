/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GroupedTopNOperatorStatus implements Operator.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Operator.Status.class,
        "groupedtopn",
        GroupedTopNOperatorStatus::new
    );

    private final long receiveNanos;
    private final long emitNanos;
    private final int occupiedRows;
    private final long groupCount;
    private final long ramBytesUsed;
    private final int pagesReceived;
    private final int pagesEmitted;
    private final long rowsReceived;
    private final long rowsEmitted;

    public GroupedTopNOperatorStatus(
        long receiveNanos,
        long emitNanos,
        int occupiedRows,
        long groupCount,
        long ramBytesUsed,
        int pagesReceived,
        int pagesEmitted,
        long rowsReceived,
        long rowsEmitted
    ) {
        this.receiveNanos = receiveNanos;
        this.emitNanos = emitNanos;
        this.occupiedRows = occupiedRows;
        this.groupCount = groupCount;
        this.ramBytesUsed = ramBytesUsed;
        this.pagesReceived = pagesReceived;
        this.pagesEmitted = pagesEmitted;
        this.rowsReceived = rowsReceived;
        this.rowsEmitted = rowsEmitted;
    }

    GroupedTopNOperatorStatus(StreamInput in) throws IOException {
        this.receiveNanos = in.readVLong();
        this.emitNanos = in.readVLong();
        this.occupiedRows = in.readVInt();
        this.groupCount = in.readVLong();
        this.ramBytesUsed = in.readVLong();

        this.pagesReceived = in.readVInt();
        this.pagesEmitted = in.readVInt();
        this.rowsReceived = in.readVLong();
        this.rowsEmitted = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(receiveNanos);
        out.writeVLong(emitNanos);

        out.writeVInt(occupiedRows);
        out.writeVLong(groupCount);
        out.writeVLong(ramBytesUsed);

        out.writeVInt(pagesReceived);
        out.writeVInt(pagesEmitted);
        out.writeVLong(rowsReceived);
        out.writeVLong(rowsEmitted);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public long receiveNanos() {
        return receiveNanos;
    }

    public long emitNanos() {
        return emitNanos;
    }

    public int occupiedRows() {
        return occupiedRows;
    }

    public long groupCount() {
        return groupCount;
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
        builder.field("receive_nanos", receiveNanos);
        if (builder.humanReadable()) {
            builder.field("receive_time", TimeValue.timeValueNanos(receiveNanos).toString());
        }
        builder.field("emit_nanos", emitNanos);
        if (builder.humanReadable()) {
            builder.field("emit_time", TimeValue.timeValueNanos(emitNanos).toString());
        }
        builder.field("occupied_rows", occupiedRows);
        builder.field("group_count", groupCount);
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
        GroupedTopNOperatorStatus that = (GroupedTopNOperatorStatus) o;
        return receiveNanos == that.receiveNanos
            && emitNanos == that.emitNanos
            && occupiedRows == that.occupiedRows
            && groupCount == that.groupCount
            && ramBytesUsed == that.ramBytesUsed
            && pagesReceived == that.pagesReceived
            && pagesEmitted == that.pagesEmitted
            && rowsReceived == that.rowsReceived
            && rowsEmitted == that.rowsEmitted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            receiveNanos,
            emitNanos,
            occupiedRows,
            groupCount,
            ramBytesUsed,
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted
        );
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
