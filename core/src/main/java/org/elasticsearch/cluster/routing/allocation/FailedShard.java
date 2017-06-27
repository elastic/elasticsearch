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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;

/**
 * A class representing a failed shard.
 */
public class FailedShard {
    private final ShardRouting routingEntry;
    private final String message;
    private final Exception failure;

    public FailedShard(ShardRouting routingEntry, String message, Exception failure) {
        assert routingEntry.assignedToNode() : "only assigned shards can be failed " + routingEntry;
        this.routingEntry = routingEntry;
        this.message = message;
        this.failure = failure;
    }

    @Override
    public String toString() {
        return "failed shard, shard " + routingEntry + ", message [" + message + "], failure [" +
                   ExceptionsHelper.detailedMessage(failure) + "]";
    }

    /**
     * The shard routing entry for the failed shard.
     */
    public ShardRouting getRoutingEntry() {
        return routingEntry;
    }

    /**
     * The failure message, if available, explaining why the shard failed.
     */
    @Nullable
    public String getMessage() {
        return message;
    }

    /**
     * The exception, if present, causing the shard to fail.
     */
    @Nullable
    public Exception getFailure() {
        return failure;
    }
}
