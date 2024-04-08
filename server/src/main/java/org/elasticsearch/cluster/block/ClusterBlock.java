/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.block;

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

    public int id() {
        return this.id;
    }

    @Nullable
    public String uuid() {
        return uuid;
    }

    public String description() {
        return this.description;
    }

    public RestStatus status() {
        return this.status;
    }

    public EnumSet<ClusterBlockLevel> levels() {
        return this.levels;
    }

    public boolean contains(ClusterBlockLevel level) {
        return levels.contains(level);
    }

    /**
     * Should operations get into retry state if this block is present.
     */
    public boolean retryable() {
        return this.retryable;
    }

    /**
     * Should global state persistence be disabled when this block is present. Note,
     * only relevant for global blocks.
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
        out.writeEnumSet(levels);
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

    public boolean isAllowReleaseResources() {
        return allowReleaseResources;
    }
}
