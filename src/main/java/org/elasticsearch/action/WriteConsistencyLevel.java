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

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

/**
 * Write Consistency Level control how many replicas should be active for a write operation to occur (a write operation
 * can be index, or delete).
 *
 *
 */
public enum WriteConsistencyLevel {
    DEFAULT((byte) 0),
    ONE((byte) 1),
    QUORUM((byte) 2),
    ALL((byte) 3);

    private final byte id;

    WriteConsistencyLevel(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static WriteConsistencyLevel fromId(byte value) {
        if (value == 0) {
            return DEFAULT;
        } else if (value == 1) {
            return ONE;
        } else if (value == 2) {
            return QUORUM;
        } else if (value == 3) {
            return ALL;
        }
        throw new ElasticsearchIllegalArgumentException("No write consistency match [" + value + "]");
    }

    public static WriteConsistencyLevel fromString(String value) {
        if (value.equals("default")) {
            return DEFAULT;
        } else if (value.equals("one")) {
            return ONE;
        } else if (value.equals("quorum")) {
            return QUORUM;
        } else if (value.equals("all")) {
            return ALL;
        }
        throw new ElasticsearchIllegalArgumentException("No write consistency match [" + value + "]");
    }
}
