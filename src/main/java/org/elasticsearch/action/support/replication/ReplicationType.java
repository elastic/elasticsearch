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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

/**
 * The type of replication to perform.
 */
public enum ReplicationType {
    /**
     * Sync replication, wait till all replicas have performed the operation.
     */
    SYNC((byte) 0),
    /**
     * Async replication. Will send the request to replicas, but will not wait for it
     */
    ASYNC((byte) 1),
    /**
     * Use the default replication type configured for this node.
     */
    DEFAULT((byte) 2);

    private byte id;

    ReplicationType(byte id) {
        this.id = id;
    }

    /**
     * The internal representation of the operation type.
     */
    public byte id() {
        return id;
    }

    /**
     * Constructs the operation type from its internal representation.
     */
    public static ReplicationType fromId(byte id) {
        if (id == 0) {
            return SYNC;
        } else if (id == 1) {
            return ASYNC;
        } else if (id == 2) {
            return DEFAULT;
        } else {
            throw new ElasticsearchIllegalArgumentException("No type match for [" + id + "]");
        }
    }

    /**
     * Parse the replication type from string.
     */
    public static ReplicationType fromString(String type) {
        if ("async".equals(type)) {
            return ASYNC;
        } else if ("sync".equals(type)) {
            return SYNC;
        } else if ("default".equals(type)) {
            return DEFAULT;
        }
        throw new ElasticsearchIllegalArgumentException("No replication type match for [" + type + "], should be either `async`, or `sync`");
    }
}
