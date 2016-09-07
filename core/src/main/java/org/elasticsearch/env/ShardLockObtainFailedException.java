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

package org.elasticsearch.env;

import org.elasticsearch.index.shard.ShardId;

/**
 * Exception used when the in-memory lock for a shard cannot be obtained
 */
public class ShardLockObtainFailedException extends Exception {
    private final ShardId shardId;

    public ShardLockObtainFailedException(ShardId shardId, String message) {
        super(message);
        this.shardId = shardId;
    }

    public ShardLockObtainFailedException(ShardId shardId, String message, Throwable cause) {
        super(message, cause);
        this.shardId = shardId;
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append(shardId.toString());
        sb.append(": ");
        sb.append(super.getMessage());
        return sb.toString();
    }
}
