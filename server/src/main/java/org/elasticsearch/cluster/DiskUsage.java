/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
 * Represents the disk usage information for a specific path on a node in the cluster.
 * This record encapsulates disk space metrics including total and free bytes, and provides
 * methods to calculate usage percentages and find paths with least/most available space.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * DiskUsage diskUsage = new DiskUsage("node-1", "my-node", "/data", 1000000000L, 500000000L);
 * double freePercent = diskUsage.freeDiskAsPercentage(); // Returns 50.0
 * long usedBytes = diskUsage.usedBytes(); // Returns 500000000L
 * }</pre>
 *
 * @param nodeId the unique identifier of the node
 * @param nodeName the human-readable name of the node
 * @param path the filesystem path being measured
 * @param totalBytes the total size of the disk in bytes
 * @param freeBytes the available free space in bytes
 */
public record DiskUsage(String nodeId, String nodeName, String path, long totalBytes, long freeBytes)
    implements
        ToXContentFragment,
        Writeable {

    private static final Logger logger = LogManager.getLogger(DiskUsage.class);

    /**
     * Constructs a {@link DiskUsage} instance by reading from a stream input.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
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
        builder.humanReadableField("used_bytes", "used", ByteSizeValue.ofBytes(this.usedBytes()));
        builder.humanReadableField("free_bytes", "free", ByteSizeValue.ofBytes(this.freeBytes));
        builder.field("free_disk_percent", truncatePercent(this.freeDiskAsPercentage()));
        builder.field("used_disk_percent", truncatePercent(this.usedDiskAsPercentage()));
        return builder;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node_id", this.nodeId);
        builder.field("node_name", this.nodeName);
        toShortXContent(builder);
        return builder;
    }

    /**
     * Calculates the percentage of disk space that is free.
     * If total bytes is zero, returns 100.0% to fail "open" (as if we don't know disk usage).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DiskUsage diskUsage = new DiskUsage("node-1", "my-node", "/data", 1000L, 400L);
     * double freePct = diskUsage.freeDiskAsPercentage(); // Returns 40.0
     * }</pre>
     *
     * @return the percentage of free disk space (0.0 to 100.0), or 100.0 if total bytes is zero
     */
    public double freeDiskAsPercentage() {
        // We return 100.0% in order to fail "open", in that if we have invalid
        // numbers for the total bytes, it's as if we don't know disk usage.
        if (totalBytes == 0) {
            return 100.0;
        }
        return 100.0 * freeBytes / totalBytes;
    }

    /**
     * Calculates the percentage of disk space that is used.
     * This is simply 100.0 minus the free percentage.
     *
     * @return the percentage of used disk space (0.0 to 100.0)
     */
    public double usedDiskAsPercentage() {
        return 100.0 - freeDiskAsPercentage();
    }

    /**
     * Calculates the number of bytes currently in use.
     *
     * @return the number of used bytes (total bytes minus free bytes)
     */
    public long usedBytes() {
        return totalBytes - freeBytes;
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
            + ByteSizeValue.ofBytes(this.freeBytes())
            + "["
            + Strings.format1Decimals(freeDiskAsPercentage(), "%")
            + "]";
    }

    /**
     * Creates a copy of this {@link DiskUsage} with updated free bytes while preserving all other fields.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DiskUsage original = new DiskUsage("node-1", "my-node", "/data", 1000L, 400L);
     * DiskUsage updated = original.copyWithFreeBytes(300L);
     * }</pre>
     *
     * @param freeBytes the new free bytes value to use
     * @return a new {@link DiskUsage} instance with updated free bytes
     */
    public DiskUsage copyWithFreeBytes(long freeBytes) {
        return new DiskUsage(nodeId, nodeName, path, totalBytes, freeBytes);
    }

    /**
     * Finds the filesystem path with the least available disk space on the specified node.
     * This method examines all filesystem paths reported in the node statistics and returns
     * the one with the smallest amount of free space.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NodeStats nodeStats = getNodeStats("node-1");
     * DiskUsage leastAvailable = DiskUsage.findLeastAvailablePath(nodeStats);
     * if (leastAvailable != null) {
     *     logger.info("Least available path: {} with {}% free",
     *         leastAvailable.path(), leastAvailable.freeDiskAsPercentage());
     * }
     * }</pre>
     *
     * @param nodeStats the node statistics containing filesystem information
     * @return the {@link DiskUsage} for the path with least available space, or {@code null} if no valid
     *         filesystem data is available or if total bytes is negative
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
     * Finds the filesystem path with the most available disk space on the specified node.
     * This method examines all filesystem paths reported in the node statistics and returns
     * the one with the largest amount of free space.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NodeStats nodeStats = getNodeStats("node-1");
     * DiskUsage mostAvailable = DiskUsage.findMostAvailable(nodeStats);
     * if (mostAvailable != null) {
     *     logger.info("Most available path: {} with {}% free",
     *         mostAvailable.path(), mostAvailable.freeDiskAsPercentage());
     * }
     * }</pre>
     *
     * @param nodeStats the node statistics containing filesystem information
     * @return the {@link DiskUsage} for the path with most available space, or {@code null} if no valid
     *         filesystem data is available or if total bytes is negative
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
