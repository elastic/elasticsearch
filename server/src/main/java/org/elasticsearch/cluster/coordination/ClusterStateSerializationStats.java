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
    private final long totalUncompressedFullStateSize;
    private final long totalCompressedFullStateSize;
    private final long diffCount;
    private final long totalUncompressedDiffSize;
    private final long totalCompressedDiffSize;

    public ClusterStateSerializationStats(
        long fullStateCount,
        long totalUncompressedFullStateSize,
        long totalCompressedFullStateSize,
        long diffCount,
        long totalUncompressedDiffSize,
        long totalCompressedDiffSize
    ) {
        this.fullStateCount = fullStateCount;
        this.totalUncompressedFullStateSize = totalUncompressedFullStateSize;
        this.totalCompressedFullStateSize = totalCompressedFullStateSize;
        this.diffCount = diffCount;
        this.totalUncompressedDiffSize = totalUncompressedDiffSize;
        this.totalCompressedDiffSize = totalCompressedDiffSize;
    }

    public ClusterStateSerializationStats(StreamInput in) throws IOException {
        this.fullStateCount = in.readVLong();
        this.totalUncompressedFullStateSize = in.readVLong();
        this.totalCompressedFullStateSize = in.readVLong();
        this.diffCount = in.readVLong();
        this.totalUncompressedDiffSize = in.readVLong();
        this.totalCompressedDiffSize = in.readVLong();
    }

    public long getFullStateCount() {
        return fullStateCount;
    }

    public long getTotalUncompressedFullStateSize() {
        return totalUncompressedFullStateSize;
    }

    public long getTotalCompressedFullStateSize() {
        return totalCompressedFullStateSize;
    }

    public long getDiffCount() {
        return diffCount;
    }

    public long getTotalUncompressedDiffSize() {
        return totalUncompressedDiffSize;
    }

    public long getTotalCompressedDiffSize() {
        return totalCompressedDiffSize;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("full_states");
        builder.field("count", fullStateCount);
        builder.humanReadableField("uncompressed_size_in_bytes", "uncompressed_size", new ByteSizeValue(totalUncompressedFullStateSize));
        builder.humanReadableField("compressed_size_in_bytes", "compressed_size", new ByteSizeValue(totalCompressedFullStateSize));
        builder.endObject();
        builder.startObject("diffs");
        builder.field("count", diffCount);
        builder.humanReadableField("uncompressed_size_in_bytes", "uncompressed_size", new ByteSizeValue(totalUncompressedDiffSize));
        builder.humanReadableField("compressed_size_in_bytes", "compressed_size", new ByteSizeValue(totalCompressedDiffSize));
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fullStateCount);
        out.writeVLong(totalUncompressedFullStateSize);
        out.writeVLong(totalCompressedFullStateSize);
        out.writeVLong(diffCount);
        out.writeVLong(totalUncompressedDiffSize);
        out.writeVLong(totalCompressedDiffSize);
    }

}
