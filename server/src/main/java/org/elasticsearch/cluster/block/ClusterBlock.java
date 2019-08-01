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

package org.elasticsearch.cluster.block;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;

public class ClusterBlock implements Writeable, ToXContentFragment {

    private int id;
    private @Nullable String uuid;
    private String description;
    private EnumSet<ClusterBlockLevel> levels;
    private boolean retryable;
    private boolean disableStatePersistence = false;
    private boolean allowReleaseResources;
    private RestStatus status;

    public ClusterBlock(StreamInput in) throws IOException {
        id = in.readVInt();
        uuid = in.readOptionalString();
        description = in.readString();
        final int len = in.readVInt();
        ArrayList<ClusterBlockLevel> levels = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            levels.add(in.readEnum(ClusterBlockLevel.class));
        }
        this.levels = EnumSet.copyOf(levels);
        retryable = in.readBoolean();
        disableStatePersistence = in.readBoolean();
        status = RestStatus.readFrom(in);
        allowReleaseResources = in.readBoolean();
    }

    public ClusterBlock(int id, String description, boolean retryable, boolean disableStatePersistence,
                        boolean allowReleaseResources, RestStatus status, EnumSet<ClusterBlockLevel> levels) {
        this(id, null, description, retryable, disableStatePersistence, allowReleaseResources, status, levels);
    }

    public ClusterBlock(int id, String uuid, String description, boolean retryable, boolean disableStatePersistence,
                        boolean allowReleaseResources, RestStatus status, EnumSet<ClusterBlockLevel> levels) {
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
        for (ClusterBlockLevel testLevel : levels) {
            if (testLevel == level) {
                return true;
            }
        }
        return false;
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
        out.writeVInt(levels.size());
        for (ClusterBlockLevel level : levels) {
            out.writeEnum(level);
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
        return Objects.hash(id, uuid);
    }

    public boolean isAllowReleaseResources() {
        return allowReleaseResources;
    }
}
