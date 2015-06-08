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

package org.elasticsearch.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.engine.IgnoreOnRecoveryEngineException;

public class AlreadyExpiredException extends ElasticsearchException implements IgnoreOnRecoveryEngineException {
    private String index;
    private String type;
    private String id;
    private final long timestamp;
    private final long ttl;
    private final long now;

    public AlreadyExpiredException(String index, String type, String id, long timestamp, long ttl, long now) {
        super("already expired [" + index + "]/[" + type + "]/[" + id + "] due to expire at [" + (timestamp + ttl) + "] and was processed at [" + now + "]");
        this.index = index;
        this.type = type;
        this.id = id;
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.now = now;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public long timestamp() {
        return timestamp;
    }

    public long ttl() {
        return ttl;
    }

    public long now() {
        return now;
    }
}