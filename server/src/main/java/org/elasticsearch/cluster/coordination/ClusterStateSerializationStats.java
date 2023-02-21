/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ClusterStateSerializationStats implements Writeable, ToXContentObject {

    public static final ClusterStateSerializationStats EMPTY = new ClusterStateSerializationStats(0L, 0L, 0L, 0L, 0L, 0L);

    private final long fullStateCount;
    private final long totalUncompressedFullStateBytes;
    private final long totalCompressedFullStateBytes;
    private final long diffCount;
    private final long totalUncompressedDiffBytes;
    private final long totalCompressedDiffBytes;

    public ClusterStateSerializationStats(
        long fullStateCount,
        long totalUncompressedFullStateBytes,
        long totalCompressedFullStateBytes,
        long diffCount,
        long totalUncompressedDiffBytes,
        long totalCompressedDiffBytes
    ) {
        this.fullStateCount = fullStateCount;
        this.totalUncompressedFullStateBytes = totalUncompressedFullStateBytes;
        this.totalCompressedFullStateBytes = totalCompressedFullStateBytes;
        this.diffCount = diffCount;
        this.totalUncompressedDiffBytes = totalUncompressedDiffBytes;
        this.totalCompressedDiffBytes = totalCompressedDiffBytes;
    }

    public ClusterStateSerializationStats(StreamInput in) throws IOException {
        this.fullStateCount = in.readVLong();
        this.totalUncompressedFullStateBytes = in.readVLong();
        this.totalCompressedFullStateBytes = in.readVLong();
        this.diffCount = in.readVLong();
        this.totalUncompressedDiffBytes = in.readVLong();
        this.totalCompressedDiffBytes = in.readVLong();
    }

    public long getFullStateCount() {
        return fullStateCount;
    }

    public long getTotalUncompressedFullStateBytes() {
        return totalUncompressedFullStateBytes;
    }

    public long getTotalCompressedFullStateBytes() {
        return totalCompressedFullStateBytes;
    }

    public long getDiffCount() {
        return diffCount;
    }

    public long getTotalUncompressedDiffBytes() {
        return totalUncompressedDiffBytes;
    }

    public long getTotalCompressedDiffBytes() {
        return totalCompressedDiffBytes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("full_states");
        builder.field("count", fullStateCount);
        builder.humanReadableField(
            "uncompressed_size_in_bytes",
            "uncompressed_size",
            ByteSizeValue.ofBytes(totalUncompressedFullStateBytes)
        );
        builder.humanReadableField("compressed_size_in_bytes", "compressed_size", ByteSizeValue.ofBytes(totalCompressedFullStateBytes));
        builder.endObject();
        builder.startObject("diffs");
        builder.field("count", diffCount);
        builder.humanReadableField("uncompressed_size_in_bytes", "uncompressed_size", ByteSizeValue.ofBytes(totalUncompressedDiffBytes));
        builder.humanReadableField("compressed_size_in_bytes", "compressed_size", ByteSizeValue.ofBytes(totalCompressedDiffBytes));
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fullStateCount);
        out.writeVLong(totalUncompressedFullStateBytes);
        out.writeVLong(totalCompressedFullStateBytes);
        out.writeVLong(diffCount);
        out.writeVLong(totalUncompressedDiffBytes);
        out.writeVLong(totalCompressedDiffBytes);
    }

}
