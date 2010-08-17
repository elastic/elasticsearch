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

package org.elasticsearch.index.gateway;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.component.CloseableIndexComponent;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.translog.Translog;

/**
 * @author kimchy (shay.banon)
 */
public interface IndexShardGateway extends IndexShardComponent, CloseableIndexComponent {

    String type();

    /**
     * The last / on going recovery status.
     */
    RecoveryStatus recoveryStatus();

    /**
     * The last snapshot status performed. Can be <tt>null</tt>.
     */
    SnapshotStatus lastSnapshotStatus();

    /**
     * The current snapshot status being performed. Can be <tt>null</tt> indicating that no snapshot
     * is being executed currently.
     */
    SnapshotStatus currentSnapshotStatus();

    /**
     * Recovers the state of the shard from the gateway.
     */
    RecoveryStatus recover() throws IndexShardGatewayRecoveryException;

    /**
     * Snapshots the given shard into the gateway.
     */
    SnapshotStatus snapshot(Snapshot snapshot) throws IndexShardGatewaySnapshotFailedException;

    /**
     * Returns <tt>true</tt> if this gateway requires scheduling management for snapshot
     * operations.
     */
    boolean requiresSnapshotScheduling();

    public static class Snapshot {
        private final SnapshotIndexCommit indexCommit;
        private final Translog.Snapshot translogSnapshot;

        private final long lastIndexVersion;
        private final long lastTranslogId;
        private final long lastTranslogPosition;
        private final long lastTranslogLength;

        public Snapshot(SnapshotIndexCommit indexCommit, Translog.Snapshot translogSnapshot, long lastIndexVersion, long lastTranslogId, long lastTranslogPosition, long lastTranslogLength) {
            this.indexCommit = indexCommit;
            this.translogSnapshot = translogSnapshot;
            this.lastIndexVersion = lastIndexVersion;
            this.lastTranslogId = lastTranslogId;
            this.lastTranslogPosition = lastTranslogPosition;
            this.lastTranslogLength = lastTranslogLength;
        }

        /**
         * Indicates that the index has changed from the latest snapshot.
         */
        public boolean indexChanged() {
            return lastIndexVersion != indexCommit.getVersion();
        }

        /**
         * Indicates that a new transaction log has been created. Note check this <b>before</b> you
         * check {@link #sameTranslogNewOperations()}.
         */
        public boolean newTranslogCreated() {
            return translogSnapshot.translogId() != lastTranslogId;
        }

        /**
         * Indicates that the same translog exists, but new operations have been appended to it. Throws
         * {@link ElasticSearchIllegalStateException} if {@link #newTranslogCreated()} is <tt>true</tt>, so
         * always check that first.
         */
        public boolean sameTranslogNewOperations() {
            if (newTranslogCreated()) {
                throw new ElasticSearchIllegalStateException("Should not be called when there is a new translog");
            }
            return translogSnapshot.length() > lastTranslogLength;
        }

        public SnapshotIndexCommit indexCommit() {
            return indexCommit;
        }

        public Translog.Snapshot translogSnapshot() {
            return translogSnapshot;
        }

        public long lastIndexVersion() {
            return lastIndexVersion;
        }

        public long lastTranslogId() {
            return lastTranslogId;
        }

        public long lastTranslogPosition() {
            return lastTranslogPosition;
        }

        public long lastTranslogLength() {
            return lastTranslogLength;
        }
    }
}
