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
import java.util.Objects;

/**
 * Encapsulation class used to represent the amount of disk used on a node.
 */
public class DiskUsage implements ToXContentFragment, Writeable {
    final String nodeId;
    final String nodeName;
    final String path;
    final long totalBytes;
    final long freeBytes;

    /**
     * Create a new DiskUsage, if {@code totalBytes} is 0, {@link #getFreeDiskAsPercentage()}
     * will always return 100.0% free
     */
    public DiskUsage(String nodeId, String nodeName, String path, long totalBytes, long freeBytes) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.freeBytes = freeBytes;
        this.totalBytes = totalBytes;
        this.path = path;
    }

    public DiskUsage(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.nodeName = in.readString();
        this.path = in.readString();
        this.totalBytes = in.readVLong();
        this.freeBytes = in.readVLong();
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

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getPath() {
        return path;
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

    public long getFreeBytes() {
        return freeBytes;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public long getUsedBytes() {
        return getTotalBytes() - getFreeBytes();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiskUsage other = (DiskUsage) o;
        return Objects.equals(nodeId, other.nodeId)
            && Objects.equals(nodeName, other.nodeName)
            && Objects.equals(totalBytes, other.totalBytes)
            && Objects.equals(freeBytes, other.freeBytes);

    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeName, path, totalBytes, freeBytes);
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
            + new ByteSizeValue(getFreeBytes())
            + "["
            + Strings.format1Decimals(getFreeDiskAsPercentage(), "%")
            + "]";
    }
}
