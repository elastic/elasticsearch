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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Exception used when the in-memory lock for a shard cannot be obtained
 */
public class ShardLockObtainFailedException extends ElasticsearchException {

    public ShardLockObtainFailedException(ShardId shardId, String message) {
        super(buildMessage(shardId, message));
        this.setShard(shardId);
    }

    public ShardLockObtainFailedException(ShardId shardId, String message, Throwable cause) {
        super(buildMessage(shardId, message), cause);
        this.setShard(shardId);
    }

    public ShardLockObtainFailedException(StreamInput in) throws IOException {
        super(in);
    }

    private static String buildMessage(ShardId shardId, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append(shardId.toString());
        sb.append(": ");
        sb.append(message);
        return sb.toString();
    }
}
