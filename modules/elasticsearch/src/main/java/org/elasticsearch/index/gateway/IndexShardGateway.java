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

import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.translog.Translog;

/**
 * @author kimchy (Shay Banon)
 */
public interface IndexShardGateway extends IndexShardComponent {

    /**
     * Recovers the state of the shard from the gateway.
     */
    RecoveryStatus recover() throws IndexShardGatewayRecoveryException;

    /**
     * Snapshots the given shard into the gateway.
     */
    void snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot);

    /**
     * Returns <tt>true</tt> if this gateway requires scheduling management for snapshot
     * operations.
     */
    boolean requiresSnapshotScheduling();

    void close();
}
