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

package org.elasticsearch.cluster.block;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

/**
 *
 */
public enum ClusterBlockLevel {
    READ(0),
    WRITE(1),
    METADATA(2);

    public static final ClusterBlockLevel[] ALL = new ClusterBlockLevel[]{READ, WRITE, METADATA};
    public static final ClusterBlockLevel[] READ_WRITE = new ClusterBlockLevel[]{READ, WRITE};

    private final int id;

    ClusterBlockLevel(int id) {
        this.id = id;
    }

    public int id() {
        return this.id;
    }

    public static ClusterBlockLevel fromId(int id) {
        if (id == 0) {
            return READ;
        } else if (id == 1) {
            return WRITE;
        } else if (id == 2) {
            return METADATA;
        }
        throw new ElasticSearchIllegalArgumentException("No cluster block level matching [" + id + "]");
    }
}
