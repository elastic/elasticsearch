/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;

/**
 * Controls the operation threading model for search operation that are performed
 * locally on the executing node.
 *
 *
 */
public enum SearchOperationThreading {
    /**
     * No threads are used, all the local shards operations will be performed on the calling
     * thread.
     */
    NO_THREADS((byte) 0),
    /**
     * The local shards operations will be performed in serial manner on a single forked thread.
     */
    SINGLE_THREAD((byte) 1),
    /**
     * Each local shard operation will execute on its own thread.
     */
    THREAD_PER_SHARD((byte) 2);

    private final byte id;

    SearchOperationThreading(byte id) {
        this.id = id;
    }

    public byte id() {
        return this.id;
    }

    public static SearchOperationThreading fromId(byte id) {
        if (id == 0) {
            return NO_THREADS;
        }
        if (id == 1) {
            return SINGLE_THREAD;
        }
        if (id == 2) {
            return THREAD_PER_SHARD;
        }
        throw new ElasticSearchIllegalArgumentException("No type matching id [" + id + "]");
    }

    public static SearchOperationThreading fromString(String value, @Nullable SearchOperationThreading defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if ("no_threads".equals(value) || "noThreads".equals(value)) {
            return NO_THREADS;
        } else if ("single_thread".equals(value) || "singleThread".equals(value)) {
            return SINGLE_THREAD;
        } else if ("thread_per_shard".equals(value) || "threadPerShard".equals(value)) {
            return THREAD_PER_SHARD;
        }
        throw new ElasticSearchIllegalArgumentException("No value for search operation threading matching [" + value + "]");
    }
}