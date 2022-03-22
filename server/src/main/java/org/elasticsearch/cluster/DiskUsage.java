/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Encapsulation class used to represent the amount of disk used on a node.
 * if {@code totalBytes} is 0, {@link #getFreeDiskAsPercentage()} will always return 100.0% free
 */
public record DiskUsage(String nodeId, String nodeName, String path, long totalBytes, long freeBytes)
    implements
        ToXContentFragment,
        Writeable {

    public static DiskUsage of(StreamInput in) throws IOException {
        return new DiskUsage(in.readString(), in.readString(), in.readString(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeString(this.nodeName);
        out.writeString(this.path);
        out.writeVLong(this.totalBytes);
        out.writeVLong(this.freeBytes);
    }

    private static double truncatePercent(double pct) {
        return Math.round(pct * 10.0) / 10.0;
    }

    XContentBuilder toShortXContent(XContentBuilder builder) throws IOException {
        builder.field("path", this.path);
        builder.humanReadableField("total_bytes", "total", new ByteSizeValue(this.totalBytes));
        builder.humanReadableField("used_bytes", "used", new ByteSizeValue(this.getUsedBytes()));
        builder.humanReadableField("free_bytes", "free", new ByteSizeValue(this.freeBytes));
        builder.field("free_disk_percent", truncatePercent(this.getFreeDiskAsPercentage()));
        builder.field("used_disk_percent", truncatePercent(this.getUsedDiskAsPercentage()));
        return builder;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node_id", this.nodeId);
        builder.field("node_name", this.nodeName);
        builder = toShortXContent(builder);
        return builder;
    }

    public double getFreeDiskAsPercentage() {
        // We return 100.0% in order to fail "open", in that if we have invalid
        // numbers for the total bytes, it's as if we don't know disk usage.
        if (totalBytes == 0) {
            return 100.0;
        }
        return 100.0 * ((double) freeBytes / totalBytes);
    }

    public double getUsedDiskAsPercentage() {
        return 100.0 - getFreeDiskAsPercentage();
    }

    public long getUsedBytes() {
        return totalBytes() - freeBytes();
    }

    @Override
    public String toString() {
        return "["
            + nodeId
            + "]["
            + nodeName
            + "]["
            + path
            + "] free: "
            + new ByteSizeValue(freeBytes())
            + "["
            + Strings.format1Decimals(getFreeDiskAsPercentage(), "%")
            + "]";
    }
}
