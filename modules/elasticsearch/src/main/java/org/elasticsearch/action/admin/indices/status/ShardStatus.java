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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexShardState;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.ImmutableShardRouting.*;
import static org.elasticsearch.common.unit.ByteSizeValue.*;

/**
 * @author kimchy (Shay Banon)
 */
public class ShardStatus extends BroadcastShardOperationResponse {

    public static class Docs {
        public static final Docs UNKNOWN = new Docs();

        int numDocs = -1;
        int maxDoc = -1;
        int deletedDocs = -1;

        public int numDocs() {
            return numDocs;
        }

        public int getNumDocs() {
            return numDocs();
        }

        public int maxDoc() {
            return maxDoc;
        }

        public int getMaxDoc() {
            return maxDoc();
        }

        public int deletedDocs() {
            return deletedDocs;
        }

        public int getDeletedDocs() {
            return deletedDocs();
        }
    }

    public static class PeerRecoveryStatus {

        public enum Stage {
            INIT((byte) 0),
            RETRY((byte) 1),
            FILES((byte) 2),
            TRANSLOG((byte) 3),
            FINALIZE((byte) 4),
            DONE((byte) 5);

            private final byte value;

            Stage(byte value) {
                this.value = value;
            }

            public byte value() {
                return value;
            }

            public static Stage fromValue(byte value) {
                if (value == 0) {
                    return INIT;
                } else if (value == 1) {
                    return RETRY;
                } else if (value == 2) {
                    return FILES;
                } else if (value == 3) {
                    return TRANSLOG;
                } else if (value == 4) {
                    return FINALIZE;
                } else if (value == 5) {
                    return DONE;
                }
                throw new ElasticSearchIllegalArgumentException("No stage found for [" + value + ']');
            }
        }

        final Stage stage;

        final long startTime;

        final long took;

        final long retryTime;

        final long indexSize;

        final long reusedIndexSize;

        final long recoveredIndexSize;

        final long recoveredTranslogOperations;

        public PeerRecoveryStatus(Stage stage, long startTime, long took, long retryTime, long indexSize, long reusedIndexSize,
                                  long recoveredIndexSize, long recoveredTranslogOperations) {
            this.stage = stage;
            this.startTime = startTime;
            this.took = took;
            this.retryTime = retryTime;
            this.indexSize = indexSize;
            this.reusedIndexSize = reusedIndexSize;
            this.recoveredIndexSize = recoveredIndexSize;
            this.recoveredTranslogOperations = recoveredTranslogOperations;
        }

        public Stage stage() {
            return this.stage;
        }

        public long startTime() {
            return this.startTime;
        }

        public long getStartTime() {
            return this.startTime;
        }

        public TimeValue took() {
            return TimeValue.timeValueMillis(took);
        }

        public TimeValue getTook() {
            return took();
        }

        public TimeValue retryTime() {
            return TimeValue.timeValueMillis(retryTime);
        }

        public TimeValue getRetryTime() {
            return retryTime();
        }

        public ByteSizeValue indexSize() {
            return new ByteSizeValue(indexSize);
        }

        public ByteSizeValue getIndexSize() {
            return indexSize();
        }

        public ByteSizeValue reusedIndexSize() {
            return new ByteSizeValue(reusedIndexSize);
        }

        public ByteSizeValue getReusedIndexSize() {
            return reusedIndexSize();
        }

        public ByteSizeValue expectedRecoveredIndexSize() {
            return new ByteSizeValue(indexSize - reusedIndexSize);
        }

        public ByteSizeValue getExpectedRecoveredIndexSize() {
            return expectedRecoveredIndexSize();
        }

        /**
         * How much of the index has been recovered.
         */
        public ByteSizeValue recoveredIndexSize() {
            return new ByteSizeValue(recoveredIndexSize);
        }

        /**
         * How much of the index has been recovered.
         */
        public ByteSizeValue getRecoveredIndexSize() {
            return recoveredIndexSize();
        }

        public long recoveredTranslogOperations() {
            return recoveredTranslogOperations;
        }

        public long getRecoveredTranslogOperations() {
            return recoveredTranslogOperations();
        }
    }

    private ShardRouting shardRouting;

    IndexShardState state;

    ByteSizeValue storeSize;

    long translogId = -1;

    long translogOperations = -1;

    Docs docs = Docs.UNKNOWN;

    PeerRecoveryStatus peerRecoveryStatus;

    ShardStatus() {
    }

    ShardStatus(ShardRouting shardRouting) {
        super(shardRouting.index(), shardRouting.id());
        this.shardRouting = shardRouting;
    }

    public ShardRouting shardRouting() {
        return this.shardRouting;
    }

    public ShardRouting getShardRouting() {
        return shardRouting();
    }

    public IndexShardState state() {
        return state;
    }

    public IndexShardState getState() {
        return state();
    }

    public ByteSizeValue storeSize() {
        return storeSize;
    }

    public ByteSizeValue getStoreSize() {
        return storeSize();
    }

    public long translogId() {
        return translogId;
    }

    public long getTranslogId() {
        return translogId();
    }

    public long translogOperations() {
        return translogOperations;
    }

    public long getTranslogOperations() {
        return translogOperations();
    }

    public Docs docs() {
        return docs;
    }

    public Docs getDocs() {
        return docs();
    }

    public PeerRecoveryStatus peerRecoveryStatus() {
        return peerRecoveryStatus;
    }

    public PeerRecoveryStatus getPeerRecoveryStatus() {
        return peerRecoveryStatus();
    }

    public static ShardStatus readIndexShardStatus(StreamInput in) throws IOException {
        ShardStatus shardStatus = new ShardStatus();
        shardStatus.readFrom(in);
        return shardStatus;
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        out.writeByte(state.id());
        if (storeSize == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            storeSize.writeTo(out);
        }
        out.writeLong(translogId);
        out.writeLong(translogOperations);
        if (docs == Docs.UNKNOWN) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(docs.numDocs());
            out.writeInt(docs.maxDoc());
            out.writeInt(docs.deletedDocs());
        }
        if (peerRecoveryStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(peerRecoveryStatus.stage.value);
            out.writeVLong(peerRecoveryStatus.startTime);
            out.writeVLong(peerRecoveryStatus.took);
            out.writeVLong(peerRecoveryStatus.retryTime);
            out.writeVLong(peerRecoveryStatus.indexSize);
            out.writeVLong(peerRecoveryStatus.reusedIndexSize);
            out.writeVLong(peerRecoveryStatus.recoveredIndexSize);
            out.writeVLong(peerRecoveryStatus.recoveredTranslogOperations);
        }
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardRouting = readShardRoutingEntry(in);
        state = IndexShardState.fromId(in.readByte());
        if (in.readBoolean()) {
            storeSize = readBytesSizeValue(in);
        }
        translogId = in.readLong();
        translogOperations = in.readLong();
        if (in.readBoolean()) {
            docs = new Docs();
            docs.numDocs = in.readInt();
            docs.maxDoc = in.readInt();
            docs.deletedDocs = in.readInt();
        }
        if (in.readBoolean()) {
            peerRecoveryStatus = new PeerRecoveryStatus(PeerRecoveryStatus.Stage.fromValue(in.readByte()),
                    in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }
    }
}
