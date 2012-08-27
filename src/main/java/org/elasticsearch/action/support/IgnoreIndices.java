/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

/**
 * Specifies what type of requested indices to exclude.
 */
public enum IgnoreIndices {

    DEFAULT((byte) 0),
    NONE((byte) 1),
    MISSING((byte) 2);

    private final byte id;

    private IgnoreIndices(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static IgnoreIndices fromId(byte id) {
        if (id == 0) {
            return DEFAULT;
        } else if (id == 1) {
            return NONE;
        } else if(id == 2) {
            return MISSING;
        } else {
            throw new ElasticSearchIllegalArgumentException("No valid missing index type id: " + id);
        }
    }

    public static IgnoreIndices fromString(String type) {
        if ("none".equals(type)) {
            return NONE;
        } else if ("missing".equals(type)) {
            return MISSING;
        } else {
            throw new ElasticSearchIllegalArgumentException("No valid missing index type: " + type);
        }
    }

}
