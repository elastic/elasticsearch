/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

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

    public TopNOperatorStatus(int occupiedRows, long ramBytesUsed) {
        this.occupiedRows = occupiedRows;
        this.ramBytesUsed = ramBytesUsed;
    }

    TopNOperatorStatus(StreamInput in) throws IOException {
        this.occupiedRows = in.readVInt();
        this.ramBytesUsed = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(occupiedRows);
        out.writeVLong(ramBytesUsed);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("occupied_rows", occupiedRows);
        builder.field("ram_bytes_used", ramBytesUsed);
        builder.field("ram_used", ByteSizeValue.ofBytes(ramBytesUsed));
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopNOperatorStatus that = (TopNOperatorStatus) o;
        return occupiedRows == that.occupiedRows && ramBytesUsed == that.ramBytesUsed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(occupiedRows, ramBytesUsed);
    }
}
