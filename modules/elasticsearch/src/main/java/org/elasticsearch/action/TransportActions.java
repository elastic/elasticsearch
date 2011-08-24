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

package org.elasticsearch.action;

/**
 * @author kimchy (shay.banon)
 */
public class TransportActions {

    public static final String BULK = "indices/bulk";

    public static final String INDEX = "indices/index/shard/index";

    public static final String COUNT = "indices/count";

    public static final String DELETE = "indices/index/shard/delete";

    public static final String DELETE_BY_QUERY = "indices/deleteByQuery";

    public static final String GET = "indices/get";

    public static final String MULTI_GET = "indices/mget";

    public static final String SEARCH = "indices/search";

    public static final String SEARCH_SCROLL = "indices/searchScroll";

    public static final String TERMS = "indices/terms";

    public static final String MORE_LIKE_THIS = "indices/moreLikeThis";

    public static final String PERCOLATE = "indices/percolate";

    public static class Admin {

        public static class Indices {
            public static final String CREATE = "indices/createIndex";
            public static final String DELETE = "indices/deleteIndex";
            public static final String OPEN = "indices/openIndex";
            public static final String CLOSE = "indices/closeIndex";
            public static final String FLUSH = "indices/flush";
            public static final String REFRESH = "indices/refresh";
            public static final String OPTIMIZE = "indices/optimize";
            public static final String STATUS = "indices/status";
            public static final String STATS = "indices/stats";
            public static final String SEGMENTS = "indices/segments";
            public static final String EXISTS = "indices/exists";
            public static final String ALIASES = "indices/aliases";
            public static final String UPDATE_SETTINGS = "indices/updateSettings";
            public static final String ANALYZE = "indices/analyze";
            public static final String PUT_INDEX_TEMPLATE = "indices/putIndexTemplate";
            public static final String DELETE_INDEX_TEMPLATE = "indices/deleteIndexTemplate";

            public static class Gateway {
                public static final String SNAPSHOT = "indices/gateway/snapshot";
            }

            public static class Mapping {
                public static final String PUT = "indices/mapping/put";
                public static final String DELETE = "indices/mapping/delete";
            }

            public static class Cache {
                public static final String CLEAR = "indices/cache/clear";
            }
        }

        public static class Cluster {

            public static final String STATE = "/cluster/state";
            public static final String HEALTH = "/cluster/health";
            public static final String UPDATE_SETTINGS = "/cluster/updateSettings";

            public static class Node {
                public static final String INFO = "/cluster/nodes/info";
                public static final String STATS = "/cluster/nodes/stats";
                public static final String SHUTDOWN = "/cluster/nodes/shutdown";
                public static final String RESTART = "/cluster/nodes/restart";
            }

            public static class Ping {
                public static final String SINGLE = "/cluster/ping/single";
                public static final String REPLICATION = "/cluster/ping/replication";
                public static final String BROADCAST = "/cluster/ping/broadcast";
            }
        }
    }
}
