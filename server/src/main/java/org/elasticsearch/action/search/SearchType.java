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

package org.elasticsearch.action.search;

/**
 * Search type represent the manner at which the search operation is executed.
 *
 *
 */
public enum SearchType {
    /**
     * Same as {@link #QUERY_THEN_FETCH}, except for an initial scatter phase which goes and computes the distributed
     * term frequencies for more accurate scoring.
     */
    DFS_QUERY_THEN_FETCH((byte) 0),
    /**
     * The query is executed against all shards, but only enough information is returned (not the document content).
     * The results are then sorted and ranked, and based on it, only the relevant shards are asked for the actual
     * document content. The return number of hits is exactly as specified in size, since they are the only ones that
     * are fetched. This is very handy when the index has a lot of shards (not replicas, shard id groups).
     */
    QUERY_THEN_FETCH((byte) 1);
    // 2 used to be DFS_QUERY_AND_FETCH
    // 3 used to be QUERY_AND_FETCH

    /**
     * The default search type ({@link #QUERY_THEN_FETCH}.
     */
    public static final SearchType DEFAULT = QUERY_THEN_FETCH;

    /**
     * Non-deprecated types
     */
    public static final SearchType [] CURRENTLY_SUPPORTED = {QUERY_THEN_FETCH, DFS_QUERY_THEN_FETCH};

    private byte id;

    SearchType(byte id) {
        this.id = id;
    }

    /**
     * The internal id of the type.
     */
    public byte id() {
        return this.id;
    }

    /**
     * Constructs search type based on the internal id.
     */
    public static SearchType fromId(byte id) {
        if (id == 0) {
            return DFS_QUERY_THEN_FETCH;
        } else if (id == 1
            || id == 3) { // TODO this bwc layer can be removed once this is back-ported to 5.3 QUERY_AND_FETCH is removed now
            return QUERY_THEN_FETCH;
        } else {
            throw new IllegalArgumentException("No search type for [" + id + "]");
        }
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch" and "query_and_fetch"/"queryAndFetch".
     */
    public static SearchType fromString(String searchType) {
        if (searchType == null) {
            return SearchType.DEFAULT;
        }
        if ("dfs_query_then_fetch".equals(searchType)) {
            return SearchType.DFS_QUERY_THEN_FETCH;
        } else if ("query_then_fetch".equals(searchType)) {
            return SearchType.QUERY_THEN_FETCH;
        } else {
            throw new IllegalArgumentException("No search type for [" + searchType + "]");
        }
    }

}
