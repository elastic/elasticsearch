/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Encapsulation class used to represent the amount of disk used on a node.
 */
public record DiskUsage(String nodeId, String nodeName, String path, long totalBytes, long freeBytes)
    implements
        ToXContentFragment,
        Writeable {

    private static final Logger logger = LogManager.getLogger(DiskUsage.class);

    public DiskUsage(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readString(), in.readVLong(), in.readVLong());
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
        builder.humanReadableField("total_bytes", "total", ByteSizeValue.ofBytes(this.totalBytes));
        builder.humanReadableField("used_bytes", "used", ByteSizeValue.ofBytes(this.getUsedBytes()));
        builder.humanReadableField("free_bytes", "free", ByteSizeValue.ofBytes(this.freeBytes));
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
        return 100.0 * freeBytes / totalBytes;
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
    public String toString() {
        return "["
            + nodeId
            + "]["
            + nodeName
            + "]["
            + path
            + "] free: "
            + ByteSizeValue.ofBytes(getFreeBytes())
            + "["
            + Strings.format1Decimals(getFreeDiskAsPercentage(), "%")
            + "]";
    }

    /**
     * Finds the path with the least available disk space and returns its disk usage. It returns null if there is no
     * file system data in the NodeStats or if the total bytes are a negative number.
     */
    @Nullable
    public static DiskUsage findLeastAvailablePath(NodeStats nodeStats) {
        if (nodeStats.getFs() == null) {
            logger.warn("node [{}/{}] did not return any filesystem stats", nodeStats.getNode().getName(), nodeStats.getNode().getId());
            return null;
        }

        FsInfo.Path leastAvailablePath = null;
        for (FsInfo.Path info : nodeStats.getFs()) {
            if (leastAvailablePath == null) {
                leastAvailablePath = info;
            } else if (leastAvailablePath.getAvailable().getBytes() > info.getAvailable().getBytes()) {
                leastAvailablePath = info;
            }
        }
        if (leastAvailablePath == null) {
            logger.warn("node [{}/{}] did not return any filesystem stats", nodeStats.getNode().getName(), nodeStats.getNode().getId());
            return null;
        }

        final String nodeId = nodeStats.getNode().getId();
        final String nodeName = nodeStats.getNode().getName();
        if (logger.isTraceEnabled()) {
            logger.trace(
                "node [{}]: least available: total: {}, available: {}",
                nodeId,
                leastAvailablePath.getTotal(),
                leastAvailablePath.getAvailable()
            );
        }
        if (leastAvailablePath.getTotal().getBytes() < 0) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "node: [{}] least available path has less than 0 total bytes of disk [{}]",
                    nodeId,
                    leastAvailablePath.getTotal().getBytes()
                );
            }
            return null;
        } else {
            return new DiskUsage(
                nodeId,
                nodeName,
                leastAvailablePath.getPath(),
                leastAvailablePath.getTotal().getBytes(),
                leastAvailablePath.getAvailable().getBytes()
            );
        }
    }

    /**
     * Finds the path with the most available disk space and returns its disk usage. It returns null if there are no
     * file system data in the node stats or if the total bytes are a negative number.
     */
    @Nullable
    public static DiskUsage findMostAvailable(NodeStats nodeStats) {
        if (nodeStats.getFs() == null) {
            logger.warn("node [{}/{}] did not return any filesystem stats", nodeStats.getNode().getName(), nodeStats.getNode().getId());
            return null;
        }

        FsInfo.Path mostAvailablePath = null;
        for (FsInfo.Path info : nodeStats.getFs()) {
            if (mostAvailablePath == null) {
                mostAvailablePath = info;
            } else if (mostAvailablePath.getAvailable().getBytes() < info.getAvailable().getBytes()) {
                mostAvailablePath = info;
            }
        }
        if (mostAvailablePath == null) {
            logger.warn("node [{}/{}] did not return any filesystem stats", nodeStats.getNode().getName(), nodeStats.getNode().getId());
            return null;
        }

        final String nodeId = nodeStats.getNode().getId();
        final String nodeName = nodeStats.getNode().getName();
        if (logger.isTraceEnabled()) {
            logger.trace(
                "node [{}]: most available: total: {}, available: {}",
                nodeId,
                mostAvailablePath.getTotal(),
                mostAvailablePath.getAvailable()
            );
        }
        if (mostAvailablePath.getTotal().getBytes() < 0) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "node: [{}] most available path has less than 0 total bytes of disk [{}]",
                    nodeId,
                    mostAvailablePath.getTotal().getBytes()
                );
            }
            return null;
        } else {
            return new DiskUsage(
                nodeId,
                nodeName,
                mostAvailablePath.getPath(),
                mostAvailablePath.getTotal().getBytes(),
                mostAvailablePath.getAvailable().getBytes()
            );
        }
    }
}
