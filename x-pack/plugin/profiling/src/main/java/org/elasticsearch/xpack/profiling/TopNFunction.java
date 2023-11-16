/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

final class TopNFunction implements Writeable, ToXContentObject, Comparable<TopNFunction> {
    String id;
    int rank;
    StackFrameMetadata metadata;
    long exclusiveCount;
    long inclusiveCount;

    TopNFunction(StreamInput in) throws IOException {
        this.id = in.readString();
        this.rank = in.readInt();
        this.exclusiveCount = in.readLong();
        this.inclusiveCount = in.readLong();
        this.metadata = new StackFrameMetadata(in);
    }

    TopNFunction(String id, StackFrameMetadata metadata) {
        this.id = id;
        this.metadata = metadata;
    }

    TopNFunction(String id, int rank, StackFrameMetadata metadata, long exclusiveCount, long inclusiveCount) {
        this.id = id;
        this.rank = rank;
        this.metadata = metadata;
        this.exclusiveCount = exclusiveCount;
        this.inclusiveCount = inclusiveCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeInt(this.rank);
        out.writeLong(this.exclusiveCount);
        out.writeLong(this.inclusiveCount);
        this.metadata.writeTo(out);
    }

    public String getId() {
        return this.id;
    }

    public int getRank() {
        return this.rank;
    }

    public StackFrameMetadata getMetadata() {
        return this.metadata;
    }

    public long getCountExclusive() {
        return this.exclusiveCount;
    }

    public long getCountInclusive() {
        return this.inclusiveCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("Id", this.id);
        builder.field("Rank", this.rank);
        builder.field("Frame", this.metadata);
        builder.field("CountExclusive", this.exclusiveCount);
        builder.field("CountInclusive", this.inclusiveCount);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopNFunction that = (TopNFunction) o;
        return Objects.equals(id, that.id)
            && Objects.equals(rank, that.rank)
            && Objects.equals(metadata, that.metadata)
            && Objects.equals(exclusiveCount, that.exclusiveCount)
            && Objects.equals(inclusiveCount, that.inclusiveCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, rank, metadata, exclusiveCount, inclusiveCount);
    }

    @Override
    public int compareTo(TopNFunction that) {
        if (this.exclusiveCount > that.exclusiveCount) {
            return 1;
        }
        if (this.exclusiveCount < that.exclusiveCount) {
            return -1;
        }
        return this.id.compareTo(that.id);
    }
}
