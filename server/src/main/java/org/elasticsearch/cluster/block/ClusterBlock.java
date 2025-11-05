/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.block;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Represents a cluster block that restricts certain operations at specified levels
 * (read, write, metadata, etc.). Cluster blocks are used to prevent operations during
 * critical cluster states such as recovery, snapshot operations, or when the cluster
 * is in a degraded state.
 *
 * <p>Blocks can be global (affecting the entire cluster) or index-specific, and can
 * restrict operations at different levels such as read, write, or metadata operations.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create a write block for an index
 * ClusterBlock writeBlock = new ClusterBlock(
 *     1,
 *     "index-read-only",
 *     false,  // not retryable
 *     false,  // don't disable state persistence
 *     false,  // don't allow release resources
 *     RestStatus.FORBIDDEN,
 *     EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
 * );
 * }</pre>
 *
 * @see ClusterBlockLevel
 * @see org.elasticsearch.cluster.block.ClusterBlocks
 */
public class ClusterBlock implements Writeable, ToXContentFragment {

    private final int id;
    @Nullable
    private final String uuid;
    private final String description;
    private final EnumSet<ClusterBlockLevel> levels;
    private final boolean retryable;
    private final boolean disableStatePersistence;
    private final boolean allowReleaseResources;
    private final RestStatus status;

    /**
     * Constructs a {@link ClusterBlock} by reading from a stream input.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public ClusterBlock(StreamInput in) throws IOException {
        id = in.readVInt();
        uuid = in.readOptionalString();
        description = in.readString();
        this.levels = in.readEnumSet(ClusterBlockLevel.class);
        retryable = in.readBoolean();
        disableStatePersistence = in.readBoolean();
        status = RestStatus.readFrom(in);
        allowReleaseResources = in.readBoolean();
    }

    /**
     * Constructs a new cluster block without a UUID.
     *
     * @param id the unique identifier for this block
     * @param description a human-readable description of why this block exists
     * @param retryable whether operations should retry when encountering this block
     * @param disableStatePersistence whether to disable state persistence (global blocks only)
     * @param allowReleaseResources whether to allow resource release operations
     * @param status the REST status code to return when this block prevents an operation
     * @param levels the operation levels at which this block applies
     */
    public ClusterBlock(
        int id,
        String description,
        boolean retryable,
        boolean disableStatePersistence,
        boolean allowReleaseResources,
        RestStatus status,
        EnumSet<ClusterBlockLevel> levels
    ) {
        this(id, null, description, retryable, disableStatePersistence, allowReleaseResources, status, levels);
    }

    /**
     * Constructs a new cluster block with a UUID for index-specific blocks.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ClusterBlock block = new ClusterBlock(
     *     10,
     *     indexUUID,
     *     "Index is read-only",
     *     false,
     *     false,
     *     false,
     *     RestStatus.FORBIDDEN,
     *     EnumSet.of(ClusterBlockLevel.WRITE)
     * );
     * }</pre>
     *
     * @param id the unique identifier for this block
     * @param uuid optional UUID for index-specific blocks
     * @param description a human-readable description of why this block exists
     * @param retryable whether operations should retry when encountering this block
     * @param disableStatePersistence whether to disable state persistence (global blocks only)
     * @param allowReleaseResources whether to allow resource release operations
     * @param status the REST status code to return when this block prevents an operation
     * @param levels the operation levels at which this block applies
     */
    public ClusterBlock(
        int id,
        String uuid,
        String description,
        boolean retryable,
        boolean disableStatePersistence,
        boolean allowReleaseResources,
        RestStatus status,
        EnumSet<ClusterBlockLevel> levels
    ) {
        this.id = id;
        this.uuid = uuid;
        this.description = description;
        this.retryable = retryable;
        this.disableStatePersistence = disableStatePersistence;
        this.status = status;
        this.levels = levels;
        this.allowReleaseResources = allowReleaseResources;
    }

    /**
     * Returns the unique identifier for this cluster block.
     *
     * @return the block ID
     */
    public int id() {
        return this.id;
    }

    /**
     * Returns the optional UUID associated with this block, typically used for index-specific blocks.
     *
     * @return the block UUID, or {@code null} if not set
     */
    @Nullable
    public String uuid() {
        return uuid;
    }

    /**
     * Returns the human-readable description explaining why this block exists.
     *
     * @return the block description
     */
    public String description() {
        return this.description;
    }

    /**
     * Returns the REST status code that should be returned when this block prevents an operation.
     *
     * @return the HTTP status code
     */
    public RestStatus status() {
        return this.status;
    }

    /**
     * Returns the set of operation levels at which this block applies.
     *
     * @return the set of blocked operation levels
     */
    public EnumSet<ClusterBlockLevel> levels() {
        return this.levels;
    }

    /**
     * Determines if this block applies to the specified operation level.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ClusterBlock block = getClusterBlock();
     * if (block.contains(ClusterBlockLevel.WRITE)) {
     *     // This block prevents write operations
     * }
     * }</pre>
     *
     * @param level the operation level to check
     * @return {@code true} if this block applies to the specified level, {@code false} otherwise
     */
    public boolean contains(ClusterBlockLevel level) {
        return levels.contains(level);
    }

    /**
     * Determines if operations should enter a retry state when encountering this block.
     * Retryable blocks indicate temporary conditions that may be resolved automatically.
     *
     * @return {@code true} if operations should retry, {@code false} otherwise
     */
    public boolean retryable() {
        return this.retryable;
    }

    /**
     * Determines if global cluster state persistence should be disabled when this block is present.
     * This flag is only relevant for global blocks and is used to prevent state persistence during
     * critical cluster operations.
     *
     * @return {@code true} if state persistence should be disabled, {@code false} otherwise
     */
    public boolean disableStatePersistence() {
        return this.disableStatePersistence;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Integer.toString(id));
        if (uuid != null) {
            builder.field("uuid", uuid);
        }
        builder.field("description", description);
        builder.field("retryable", retryable);
        if (disableStatePersistence) {
            builder.field("disable_state_persistence", disableStatePersistence);
        }
        builder.startArray("levels");
        for (ClusterBlockLevel level : levels) {
            builder.value(level.name().toLowerCase(Locale.ROOT));
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeOptionalString(uuid);
        out.writeString(description);
        if (out.getTransportVersion().supports(TransportVersions.V_8_18_0)) {
            out.writeEnumSet(levels);
        } else {
            // do not send ClusterBlockLevel.REFRESH to old nodes
            out.writeEnumSet(filterLevels(levels, level -> ClusterBlockLevel.REFRESH.equals(level) == false));
        }
        out.writeBoolean(retryable);
        out.writeBoolean(disableStatePersistence);
        RestStatus.writeTo(out, status);
        out.writeBoolean(allowReleaseResources);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(",");
        if (uuid != null) {
            sb.append(uuid).append(',');
        }
        sb.append(description).append(", blocks ");
        String delimiter = "";
        for (ClusterBlockLevel level : levels) {
            sb.append(delimiter).append(level.name());
            delimiter = ",";
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterBlock that = (ClusterBlock) o;
        return id == that.id && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return 31 * Integer.hashCode(id) + Objects.hashCode(uuid);
    }

    /**
     * Determines if resource release operations are allowed even when this block is active.
     * This is important for allowing cleanup operations to proceed during blocked states.
     *
     * @return {@code true} if resource release is allowed, {@code false} otherwise
     */
    public boolean isAllowReleaseResources() {
        return allowReleaseResources;
    }

    static EnumSet<ClusterBlockLevel> filterLevels(EnumSet<ClusterBlockLevel> levels, Predicate<ClusterBlockLevel> predicate) {
        assert levels != null;
        int size = levels.size();
        if (size == 0 || (size == 1 && predicate.test(levels.iterator().next()))) {
            return levels;
        }
        var filteredLevels = EnumSet.noneOf(ClusterBlockLevel.class);
        for (ClusterBlockLevel level : levels) {
            if (predicate.test(level)) {
                filteredLevels.add(level);
            }
        }
        return filteredLevels;
    }
}
