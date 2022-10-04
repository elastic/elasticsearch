/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class CompletionStats implements Writeable, ToXContentFragment {

    private static final String COMPLETION = "completion";
    private static final String SIZE_IN_BYTES = "size_in_bytes";
    private static final String SIZE = "size";
    private static final String FIELDS = "fields";

    private long sizeInBytes;
    @Nullable
    private FieldMemoryStats fields;

    public CompletionStats() {}

    public CompletionStats(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
        fields = in.readOptionalWriteable(FieldMemoryStats::new);
    }

    public CompletionStats(long size, @Nullable FieldMemoryStats fields) {
        this.sizeInBytes = size;
        this.fields = fields;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue getSize() {
        return new ByteSizeValue(sizeInBytes);
    }

    public FieldMemoryStats getFields() {
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        out.writeOptionalWriteable(fields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(COMPLETION);
        builder.humanReadableField(SIZE_IN_BYTES, SIZE, getSize());
        if (fields != null) {
            fields.toXContent(builder, FIELDS, SIZE_IN_BYTES, SIZE);
        }
        builder.endObject();
        return builder;
    }

    public void add(CompletionStats completion) {
        if (completion == null) {
            return;
        }
        sizeInBytes += completion.getSizeInBytes();
        if (completion.fields != null) {
            if (fields == null) {
                fields = completion.fields.copy();
            } else {
                fields.add(completion.fields);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletionStats that = (CompletionStats) o;
        return sizeInBytes == that.sizeInBytes && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sizeInBytes, fields);
    }
}
