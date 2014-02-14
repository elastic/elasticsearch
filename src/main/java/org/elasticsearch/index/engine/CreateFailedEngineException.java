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

import org.elasticsearch.index.shard.ShardId;

/**
 *
 */
public class CreateFailedEngineException extends EngineException {

    private final String type;

    private final String id;

    public CreateFailedEngineException(ShardId shardId, Engine.Create create, Throwable cause) {
        super(shardId, "Create failed for [" + create.type() + "#" + create.id() + "]", cause);
        this.type = create.type();
        this.id = create.id();
    }

    public String type() {
        return this.type;
    }

    public String id() {
        return this.id;
    }
}
