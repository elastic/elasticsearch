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

package org.elasticsearch.action.updatebyquery;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

/**
 * Specifies how bulk responses that have been created based on the documents that have matched with the update query
 * are returned in the response.
 */
public enum BulkResponseOption {

    /**
     * Bulk responses aren't included in the response.
     */
    NONE((byte) 0),

    /**
     * Only failed bulk responses are included in the response.
     */
    FAILED((byte) 1),

    /**
     * All bulk responses are included in the response.
     */
    ALL((byte) 2);

    private byte id;

    private BulkResponseOption(byte id) {
        this.id = id;
    }

    /**
     * The internal representation of the operation type.
     */
    public byte id() {
        return id;
    }

    /**
     * Constructs the bulk response option from its internal representation.
     */
    public static BulkResponseOption fromId(byte id) {
        if (id == 0) {
            return NONE;
        } else if (id == 1) {
            return FAILED;
        } else if (id == 2) {
            return ALL;
        } else {
            throw new ElasticsearchIllegalArgumentException("No type match for [" + id + "]");
        }
    }

    /**
     * Parse the bulk response option from string.
     */
    public static BulkResponseOption fromString(String type) {
        if ("none".equals(type)) {
            return NONE;
        } else if ("failed".equals(type)) {
            return FAILED;
        } else if ("all".equals(type)) {
            return ALL;
        }
        throw new ElasticsearchIllegalArgumentException("no response type match for [" + type + "], should be either `none`, `failed` or `all`");
    }

}
