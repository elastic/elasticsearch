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

package org.elasticsearch.cluster.routing;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.Serializable;

/**
 * {@link ImmutableShardRouting} immutably encapsulates information about shard
 * routings like id, state, version, etc.
 */
public class ImmutableShardRouting implements Streamable, Serializable, ShardRouting {

    protected String index;

    protected int shardId;

    protected String currentNodeId;

    protected String relocatingNodeId;

    protected boolean primary;

    protected ShardRoutingState state;

    protected long version;

    private transient ShardId shardIdentifier;

    protected RestoreSource restoreSource;

    private final transient ImmutableList<ShardRouting> asList;

    ImmutableShardRouting() {
        this.asList = ImmutableList.of((ShardRouting) this);
    }

    public ImmutableShardRouting(ShardRouting copy) {
        this(copy.index(), copy.id(), copy.currentNodeId(), copy.primary(), copy.state(), copy.version());
        this.relocatingNodeId = copy.relocatingNodeId();
        this.restoreSource = copy.restoreSource();
        if (copy instanceof ImmutableShardRouting) {
            this.shardIdentifier = ((ImmutableShardRouting) copy).shardIdentifier;
        }
    }

    public ImmutableShardRouting(ShardRouting copy, long version) {
        this(copy.index(), copy.id(), copy.currentNodeId(), copy.primary(), copy.state(), copy.version());
        this.relocatingNodeId = copy.relocatingNodeId();
        this.restoreSource = copy.restoreSource();
        this.version = version;
        if (copy instanceof ImmutableShardRouting) {
            this.shardIdentifier = ((ImmutableShardRouting) copy).shardIdentifier;
        }
    }

    public ImmutableShardRouting(String index, int shardId, String currentNodeId,
                                 String relocatingNodeId, boolean primary, ShardRoutingState state, long version) {
        this(index, shardId, currentNodeId, primary, state, version);
        this.relocatingNodeId = relocatingNodeId;
    }

    public ImmutableShardRouting(String index, int shardId, String currentNodeId,
                                 String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version) {
        this(index, shardId, currentNodeId, relocatingNodeId, primary, state, version);
        this.restoreSource = restoreSource;
    }

    public ImmutableShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state, long version) {
        this.index = index;
        this.shardId = shardId;
        this.currentNodeId = currentNodeId;
        this.primary = primary;
        this.state = state;
        this.asList = ImmutableList.of((ShardRouting) this);
        this.version = version;
    }

    @Override
    public String index() {
        return this.index;
    }

    @Override
    public String getIndex() {
        return index();
    }

    @Override
    public int id() {
        return this.shardId;
    }

    @Override
    public int getId() {
        return id();
    }

    @Override
    public long version() {
        return this.version;
    }

    @Override
    public boolean unassigned() {
        return state == ShardRoutingState.UNASSIGNED;
    }

    @Override
    public boolean initializing() {
        return state == ShardRoutingState.INITIALIZING;
    }

    @Override
    public boolean active() {
        return started() || relocating();
    }

    @Override
    public boolean started() {
        return state == ShardRoutingState.STARTED;
    }

    @Override
    public boolean relocating() {
        return state == ShardRoutingState.RELOCATING;
    }

    @Override
    public boolean assignedToNode() {
        return currentNodeId != null;
    }

    @Override
    public String currentNodeId() {
        return this.currentNodeId;
    }

    @Override
    public String relocatingNodeId() {
        return this.relocatingNodeId;
    }

    @Override
    public ShardRouting targetRoutingIfRelocating() {
        if (!relocating()) {
            return null;
        }
        return new ImmutableShardRouting(index, shardId, relocatingNodeId, currentNodeId, primary, ShardRoutingState.INITIALIZING, version);
    }

    @Override
    public RestoreSource restoreSource() {
        return restoreSource;
    }

    @Override
    public boolean primary() {
        return this.primary;
    }

    @Override
    public ShardRoutingState state() {
        return this.state;
    }

    @Override
    public ShardId shardId() {
        if (shardIdentifier != null) {
            return shardIdentifier;
        }
        shardIdentifier = new ShardId(index, shardId);
        return shardIdentifier;
    }

    @Override
    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId(), asList);
    }

    public static ImmutableShardRouting readShardRoutingEntry(StreamInput in) throws IOException {
        ImmutableShardRouting entry = new ImmutableShardRouting();
        entry.readFrom(in);
        return entry;
    }

    public static ImmutableShardRouting readShardRoutingEntry(StreamInput in, String index, int shardId) throws IOException {
        ImmutableShardRouting entry = new ImmutableShardRouting();
        entry.readFrom(in, index, shardId);
        return entry;
    }

    public void readFrom(StreamInput in, String index, int shardId) throws IOException {
        this.index = index;
        this.shardId = shardId;
        readFromThin(in);
    }

    @Override
    public void readFromThin(StreamInput in) throws IOException {
        version = in.readLong();
        if (in.readBoolean()) {
            currentNodeId = in.readString();
        }

        if (in.readBoolean()) {
            relocatingNodeId = in.readString();
        }

        primary = in.readBoolean();
        state = ShardRoutingState.fromValue(in.readByte());

        restoreSource = RestoreSource.readOptionalRestoreSource(in);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        readFrom(in, in.readString(), in.readVInt());
    }

    /**
     * Writes shard information to {@link StreamOutput} without writing index name and shard id
     *
     * @param out {@link StreamOutput} to write shard information to
     * @throws IOException if something happens during write
     */
    @Override
    public void writeToThin(StreamOutput out) throws IOException {
        out.writeLong(version);
        if (currentNodeId != null) {
            out.writeBoolean(true);
            out.writeString(currentNodeId);
        } else {
            out.writeBoolean(false);
        }

        if (relocatingNodeId != null) {
            out.writeBoolean(true);
            out.writeString(relocatingNodeId);
        } else {
            out.writeBoolean(false);
        }

        out.writeBoolean(primary);
        out.writeByte(state.value());

        if (restoreSource != null) {
            out.writeBoolean(true);
            restoreSource.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        writeToThin(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // we check on instanceof so we also handle the MutableShardRouting case as well
        if (o == null || !(o instanceof ImmutableShardRouting)) {
            return false;
        }

        ImmutableShardRouting that = (ImmutableShardRouting) o;

        if (primary != that.primary) {
            return false;
        }
        if (shardId != that.shardId) {
            return false;
        }
        if (currentNodeId != null ? !currentNodeId.equals(that.currentNodeId) : that.currentNodeId != null) {
            return false;
        }
        if (index != null ? !index.equals(that.index) : that.index != null) {
            return false;
        }
        if (relocatingNodeId != null ? !relocatingNodeId.equals(that.relocatingNodeId) : that.relocatingNodeId != null) {
            return false;
        }
        if (state != that.state) {
            return false;
        }
        if (restoreSource != null ? !restoreSource.equals(that.restoreSource) : that.restoreSource != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + shardId;
        result = 31 * result + (currentNodeId != null ? currentNodeId.hashCode() : 0);
        result = 31 * result + (relocatingNodeId != null ? relocatingNodeId.hashCode() : 0);
        result = 31 * result + (primary ? 1 : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (restoreSource != null ? restoreSource.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return shortSummary();
    }

    @Override
    public String shortSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(index).append(']').append('[').append(shardId).append(']');
        sb.append(", node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("relocating [").append(relocatingNodeId).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            sb.append("[R]");
        }
        if (this.restoreSource != null) {
            sb.append(", restoring[" + restoreSource + "]");
        }
        sb.append(", s[").append(state).append("]");
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field("state", state())
                .field("primary", primary())
                .field("node", currentNodeId())
                .field("relocating_node", relocatingNodeId())
                .field("shard", shardId().id())
                .field("index", shardId().index().name());
        if (restoreSource() != null) {
            builder.field("restore_source");
            restoreSource().toXContent(builder, params);
        }
        return builder.endObject();
    }
}
