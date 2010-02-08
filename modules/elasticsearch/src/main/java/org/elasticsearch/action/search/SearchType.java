/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

/**
 * @author kimchy (Shay Banon)
 */
public enum SearchType {
    DFS_QUERY_THEN_FETCH((byte) 0),
    QUERY_THEN_FETCH((byte) 1),
    DFS_QUERY_AND_FETCH((byte) 2),
    QUERY_AND_FETCH((byte) 3);

    private byte id;

    SearchType(byte id) {
        this.id = id;
    }

    public byte id() {
        return this.id;
    }

    public static SearchType fromId(byte id) {
        if (id == 0) {
            return DFS_QUERY_THEN_FETCH;
        } else if (id == 1) {
            return QUERY_THEN_FETCH;
        } else if (id == 2) {
            return DFS_QUERY_AND_FETCH;
        } else if (id == 3) {
            return QUERY_AND_FETCH;
        } else {
            throw new ElasticSearchIllegalArgumentException("No search type for [" + id + "]");
        }
    }
}
