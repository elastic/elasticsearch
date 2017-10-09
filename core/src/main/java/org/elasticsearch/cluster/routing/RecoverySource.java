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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents the recovery source of a shard. Available recovery types are:
 *
 * - {@link StoreRecoverySource} recovery from the local store (empty or with existing data)
 * - {@link PeerRecoverySource} recovery from a primary on another node
 * - {@link SnapshotRecoverySource} recovery from a snapshot
 * - {@link LocalShardsRecoverySource} recovery from other shards of another index on the same node
 */
public abstract class RecoverySource implements Writeable, ToXContentObject {

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("type", getType());
        addAdditionalFields(builder, params);
        return builder.endObject();
    }

    /**
     * to be overridden by subclasses
     */
    public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {

    }

    public static RecoverySource readFrom(StreamInput in) throws IOException {
        Type type = Type.values()[in.readByte()];
        switch (type) {
            case EMPTY_STORE: return StoreRecoverySource.EMPTY_STORE_INSTANCE;
            case EXISTING_STORE: return StoreRecoverySource.EXISTING_STORE_INSTANCE;
            case PEER: return PeerRecoverySource.INSTANCE;
            case SNAPSHOT: return new SnapshotRecoverySource(in);
            case LOCAL_SHARDS: return LocalShardsRecoverySource.INSTANCE;
            default: throw new IllegalArgumentException("unknown recovery type: " + type.name());
        }
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) getType().ordinal());
        writeAdditionalFields(out);
    }

    /**
     * to be overridden by subclasses
     */
    protected void writeAdditionalFields(StreamOutput out) throws IOException {

    }

    public enum Type {
        EMPTY_STORE,
        EXISTING_STORE,
        PEER,
        SNAPSHOT,
        LOCAL_SHARDS
    }

    public abstract Type getType();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecoverySource that = (RecoverySource) o;

        return getType() == that.getType();
    }

    @Override
    public int hashCode() {
        return getType().hashCode();
    }

    /**
     * recovery from an existing on-disk store or a fresh copy
     */
    public abstract static class StoreRecoverySource extends RecoverySource {
        public static final StoreRecoverySource EMPTY_STORE_INSTANCE = new StoreRecoverySource() {
            @Override
            public Type getType() {
                return Type.EMPTY_STORE;
            }
        };
        public static final StoreRecoverySource EXISTING_STORE_INSTANCE = new StoreRecoverySource() {
            @Override
            public Type getType() {
                return Type.EXISTING_STORE;
            }
        };

        @Override
        public String toString() {
            return getType() == Type.EMPTY_STORE ? "new shard recovery" : "existing recovery";
        }
    }

    /**
     * recovery from other shards on same node (shrink index action)
     */
    public static class LocalShardsRecoverySource extends RecoverySource {

        public static final LocalShardsRecoverySource INSTANCE = new LocalShardsRecoverySource();

        private LocalShardsRecoverySource() {
        }

        @Override
        public Type getType() {
            return Type.LOCAL_SHARDS;
        }

        @Override
        public String toString() {
            return "local shards recovery";
        }

    }

    /**
     * recovery from a snapshot
     */
    public static class SnapshotRecoverySource extends RecoverySource {
        private final Snapshot snapshot;
        private final String index;
        private final Version version;

        public SnapshotRecoverySource(Snapshot snapshot, Version version, String index) {
            this.snapshot = Objects.requireNonNull(snapshot);
            this.version = Objects.requireNonNull(version);
            this.index = Objects.requireNonNull(index);
        }

        SnapshotRecoverySource(StreamInput in) throws IOException {
            snapshot = new Snapshot(in);
            version = Version.readVersion(in);
            index = in.readString();
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public String index() {
            return index;
        }

        public Version version() {
            return version;
        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {
            snapshot.writeTo(out);
            Version.writeVersion(version, out);
            out.writeString(index);
        }

        @Override
        public Type getType() {
            return Type.SNAPSHOT;
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("repository", snapshot.getRepository())
                .field("snapshot", snapshot.getSnapshotId().getName())
                .field("version", version.toString())
                .field("index", index);
        }

        @Override
        public String toString() {
            return "snapshot recovery from " + snapshot.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            @SuppressWarnings("unchecked") SnapshotRecoverySource that = (SnapshotRecoverySource) o;
            return snapshot.equals(that.snapshot) && index.equals(that.index) && version.equals(that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, index, version);
        }

    }

    /**
     * peer recovery from a primary shard
     */
    public static class PeerRecoverySource extends RecoverySource {

        public static final PeerRecoverySource INSTANCE = new PeerRecoverySource();

        private PeerRecoverySource() {
        }

        @Override
        public Type getType() {
            return Type.PEER;
        }

        @Override
        public String toString() {
            return "peer recovery";
        }
    }
}
