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

import org.elasticsearch.common.Nullable;

class ScrollIdForNode {
    private final String node;
    private final long scrollId;
    private final String clusterAlias;

    ScrollIdForNode(@Nullable String clusterAlias, String node, long scrollId) {
        this.node = node;
        this.clusterAlias = clusterAlias;
        this.scrollId = scrollId;
    }

    public String getNode() {
        return node;
    }

    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    public long getScrollId() {
        return scrollId;
    }

    @Override
    public String toString() {
        return "ScrollIdForNode{" +
            "node='" + node + '\'' +
            ", scrollId=" + scrollId +
            ", clusterAlias='" + clusterAlias + '\'' +
            '}';
    }
}
