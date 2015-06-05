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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Locale;

/**
 *
 */
public class ClusterBlock implements Serializable, Streamable, ToXContent {

    private int id;

    private String description;

    private EnumSet<ClusterBlockLevel> levels;

    private boolean retryable;

    private boolean disableStatePersistence = false;

    private RestStatus status;

    ClusterBlock() {
    }

    public ClusterBlock(int id, String description, boolean retryable, boolean disableStatePersistence, RestStatus status, EnumSet<ClusterBlockLevel> levels) {
        this.id = id;
        this.description = description;
        this.retryable = retryable;
        this.disableStatePersistence = disableStatePersistence;
        this.status = status;
        this.levels = levels;
    }

    public int id() {
        return this.id;
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

    public static ClusterBlock readClusterBlock(StreamInput in) throws IOException {
        ClusterBlock block = new ClusterBlock();
        block.readFrom(in);
        return block;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        description = in.readString();
        final int len = in.readVInt();
        ArrayList<ClusterBlockLevel> levels = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            levels.add(ClusterBlockLevel.fromId(in.readVInt()));
        }
        this.levels = EnumSet.copyOf(levels);
        retryable = in.readBoolean();
        disableStatePersistence = in.readBoolean();
        status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeString(description);
        out.writeVInt(levels.size());
        for (ClusterBlockLevel level : levels) {
            out.writeVInt(level.id());
        }
        out.writeBoolean(retryable);
        out.writeBoolean(disableStatePersistence);
        RestStatus.writeTo(out, status);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(",").append(description).append(", blocks ");
        for (ClusterBlockLevel level : levels) {
            sb.append(level.name()).append(",");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterBlock that = (ClusterBlock) o;

        if (id != that.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
