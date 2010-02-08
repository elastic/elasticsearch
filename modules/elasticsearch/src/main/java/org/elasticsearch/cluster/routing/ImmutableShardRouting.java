/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (Shay Banon)
 */
public class ImmutableShardRouting implements Streamable, Serializable, ShardRouting {

    protected String index;

    protected int shardId;

    protected String currentNodeId;

    protected String relocatingNodeId;

    protected boolean primary;

    protected ShardRoutingState state;

    private transient ShardId shardIdentifier;

    public ImmutableShardRouting() {
    }

    public ImmutableShardRouting(ShardRouting copy) {
        this(copy.index(), copy.id(), copy.currentNodeId(), copy.primary(), copy.state());
        this.relocatingNodeId = copy.relocatingNodeId();
    }

    public ImmutableShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        this.index = index;
        this.shardId = shardId;
        this.currentNodeId = currentNodeId;
        this.primary = primary;
        this.state = state;
    }

    public ImmutableShardRouting(String index, int shardId, String currentNodeId,
                                 String relocatingNodeId, boolean primary, ShardRoutingState state) {
        this(index, shardId, currentNodeId, primary, state);
        this.relocatingNodeId = relocatingNodeId;
    }

    @Override public String index() {
        return this.index;
    }

    @Override public int id() {
        return this.shardId;
    }

    @Override public boolean unassigned() {
        return state == ShardRoutingState.UNASSIGNED;
    }

    @Override public boolean initializing() {
        return state == ShardRoutingState.INITIALIZING;
    }

    @Override public boolean active() {
        return started() || relocating();
    }

    @Override public boolean started() {
        return state == ShardRoutingState.STARTED;
    }

    @Override public boolean relocating() {
        return state == ShardRoutingState.RELOCATING;
    }

    @Override public boolean assignedToNode() {
        return currentNodeId != null;
    }

    @Override public String currentNodeId() {
        return this.currentNodeId;
    }

    @Override public String relocatingNodeId() {
        return this.relocatingNodeId;
    }

    @Override public boolean primary() {
        return this.primary;
    }

    @Override public ShardRoutingState state() {
        return this.state;
    }

    @Override public ShardId shardId() {
        if (shardIdentifier != null) {
            return shardIdentifier;
        }
        shardIdentifier = new ShardId(index, shardId);
        return shardIdentifier;
    }

    public static ImmutableShardRouting readShardRoutingEntry(DataInput in) throws IOException, ClassNotFoundException {
        ImmutableShardRouting entry = new ImmutableShardRouting();
        entry.readFrom(in);
        return entry;
    }

    public static ImmutableShardRouting readShardRoutingEntry(DataInput in, String index, int shardId) throws IOException, ClassNotFoundException {
        ImmutableShardRouting entry = new ImmutableShardRouting();
        entry.readFrom(in, index, shardId);
        return entry;
    }

    public void readFrom(DataInput in, String index, int shardId) throws IOException, ClassNotFoundException {
        this.index = index;
        this.shardId = shardId;
        readFromThin(in);
    }

    @Override public void readFromThin(DataInput in) throws IOException {
        if (in.readBoolean()) {
            currentNodeId = in.readUTF();
        }

        if (in.readBoolean()) {
            relocatingNodeId = in.readUTF();
        }

        primary = in.readBoolean();
        state = ShardRoutingState.fromValue(in.readByte());
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        readFrom(in, in.readUTF(), in.readInt());
    }

    /**
     * Does not write index name and shard id
     */
    public void writeToThin(DataOutput out) throws IOException {
        if (currentNodeId != null) {
            out.writeBoolean(true);
            out.writeUTF(currentNodeId);
        } else {
            out.writeBoolean(false);
        }

        if (relocatingNodeId != null) {
            out.writeBoolean(true);
            out.writeUTF(relocatingNodeId);
        } else {
            out.writeBoolean(false);
        }

        out.writeBoolean(primary);
        out.writeByte(state.value());
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(index);
        out.writeInt(shardId);
        writeToThin(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImmutableShardRouting that = (ImmutableShardRouting) o;

        if (primary != that.primary) return false;
        if (shardId != that.shardId) return false;
        if (currentNodeId != null ? !currentNodeId.equals(that.currentNodeId) : that.currentNodeId != null)
            return false;
        if (index != null ? !index.equals(that.index) : that.index != null) return false;
        if (relocatingNodeId != null ? !relocatingNodeId.equals(that.relocatingNodeId) : that.relocatingNodeId != null)
            return false;
        if (state != that.state) return false;

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
        return result;
    }

    @Override public String toString() {
        return shortSummary();
    }

    @Override public String shortSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(index).append(']').append('[').append(shardId).append(']');
        sb.append(", Node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("Relocating [").append(relocatingNodeId).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            sb.append("[B]");
        }
        sb.append(", S[").append(state).append("]");
        return sb.toString();
    }

}
