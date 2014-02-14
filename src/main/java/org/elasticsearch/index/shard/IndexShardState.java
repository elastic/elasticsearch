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

package org.elasticsearch.index.shard;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

/**
 *
 */
public enum IndexShardState {
    CREATED((byte) 0),
    RECOVERING((byte) 1),
    POST_RECOVERY((byte) 2),
    STARTED((byte) 3),
    RELOCATED((byte) 4),
    CLOSED((byte) 5);

    private static final IndexShardState[] IDS = new IndexShardState[IndexShardState.values().length];

    static {
        for (IndexShardState state : IndexShardState.values()) {
            assert state.id() < IDS.length && state.id() >= 0;
            IDS[state.id()] = state;
        }
    }

    private final byte id;

    IndexShardState(byte id) {
        this.id = id;
    }

    public byte id() {
        return this.id;
    }

    public static IndexShardState fromId(byte id) throws ElasticsearchIllegalArgumentException {
        if (id < 0 || id >= IDS.length) {
            throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
        }
        return IDS[id];
    }
}
