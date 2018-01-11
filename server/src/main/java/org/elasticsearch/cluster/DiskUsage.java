/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

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
        builder.byteSizeField("total_bytes", "total", this.totalBytes);
        builder.byteSizeField("used_bytes", "used", this.getUsedBytes());
        builder.byteSizeField("free_bytes", "free", this.freeBytes);
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
        return 100.0 * ((double)freeBytes / totalBytes);
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
        return Objects.equals(nodeId, other.nodeId) &&
                Objects.equals(nodeName, other.nodeName) &&
                Objects.equals(totalBytes, other.totalBytes) &&
                Objects.equals(freeBytes, other.freeBytes);

    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeName, path, totalBytes, freeBytes);
    }

    @Override
    public String toString() {
        return "[" + nodeId + "][" + nodeName + "][" + path + "] free: " + new ByteSizeValue(getFreeBytes()) +
                "[" + Strings.format1Decimals(getFreeDiskAsPercentage(), "%") + "]";
    }
}
