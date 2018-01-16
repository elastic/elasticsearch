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
package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class VersionConflictEngineException extends EngineException {

    public VersionConflictEngineException(ShardId shardId, Engine.Operation op, long currentVersion, boolean deleted) {
        this(shardId, op.type(), op.id(), op.versionType().explainConflictForWrites(currentVersion, op.version(), deleted));
    }

    public VersionConflictEngineException(ShardId shardId, String type, String id, String explanation) {
        this(shardId, null, type, id, explanation);
    }

    public VersionConflictEngineException(ShardId shardId, Throwable cause, String type, String id, String explanation) {
        this(shardId, "[{}][{}]: version conflict, {}", cause, type, id, explanation);
    }

    public VersionConflictEngineException(ShardId shardId, String msg, Throwable cause, Object... params) {
        super(shardId, msg, cause, params);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

    public VersionConflictEngineException(StreamInput in) throws IOException {
        super(in);
    }
}
