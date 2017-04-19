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

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;

import java.io.IOException;

public abstract class RecoverySourceHandler {
    protected final Logger logger;
    // Shard that is going to be recovered (the "source")
    protected final IndexShard shard;
    protected final RecoveryResponse response;
    protected final CancellableThreads cancellableThreads = new CancellableThreads() {
        @Override
        protected void onCancel(String reason, @Nullable Exception suppressedException) {
            RuntimeException e;
            if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                e = new IndexShardClosedException(shard.shardId(),
                    "shard is closed and recovery was canceled reason [" + reason + "]");
            } else {
                e = new ExecutionCancelledException(
                    "recovery was canceled reason [" + reason + "]");
            }
            if (suppressedException != null) {
                e.addSuppressed(suppressedException);
            }
            throw e;
        }
    };

    public RecoverySourceHandler(final IndexShard shard, final Settings nodeSettings,
                                 final DiscoveryNode targetNode) {
        this.shard = shard;
        this.logger = Loggers.getLogger(getClass(), nodeSettings, shard.shardId(),
            "recover to " + targetNode.getName());
        this.response = new RecoveryResponse();
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public abstract RecoveryResponse recoverToTarget() throws IOException;

    /**
     * Cancels the recovery and interrupts all eligible threads.
     */
    public void cancel(String reason) {
        cancellableThreads.cancel(reason);
    }
}
