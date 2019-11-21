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

import org.elasticsearch.index.shard.ShardId;

import java.util.Comparator;
import java.util.Objects;

public class SearchShard implements Comparable<SearchShard> {
    private final String clusterAlias;
    private final ShardId shardId;

    SearchShard(String clusterAlias, ShardId shardId) {
        this.clusterAlias = clusterAlias;
        this.shardId = shardId;
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public int compareTo(SearchShard o) {
        int cmp = Objects.compare(clusterAlias, o.clusterAlias,
            Comparator.nullsFirst(Comparator.naturalOrder()));
        return cmp != 0 ? cmp : shardId.compareTo(o.shardId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShard that = (SearchShard) o;
        return Objects.equals(clusterAlias, that.clusterAlias) &&
            shardId.equals(that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAlias, shardId);
    }

    @Override
    public String toString() {
        return "SearchShard{" +
            "clusterAlias='" + clusterAlias + '\'' +
            ", shardId=" + shardId +
            '}';
    }
}
