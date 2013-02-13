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

package org.elasticsearch.cluster.routing.operation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.indices.IndexMissingException;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public interface OperationRouting {

    ShardIterator indexShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing) throws IndexMissingException, IndexShardMissingException;

    ShardIterator deleteShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing) throws IndexMissingException, IndexShardMissingException;

    GroupShardsIterator broadcastDeleteShards(ClusterState clusterState, String index) throws IndexMissingException, IndexShardMissingException;

    ShardIterator getShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing, @Nullable String preference) throws IndexMissingException, IndexShardMissingException;

    ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) throws IndexMissingException, IndexShardMissingException;

    GroupShardsIterator deleteByQueryShards(ClusterState clusterState, String index, @Nullable Set<String> routing) throws IndexMissingException;

    int searchShardsCount(ClusterState clusterState, String[] indices, String[] concreteIndices, @Nullable Map<String, Set<String>> routing, @Nullable String preference) throws IndexMissingException;

    GroupShardsIterator searchShards(ClusterState clusterState, String[] indices, String[] concreteIndices, @Nullable Map<String, Set<String>> routing, @Nullable String preference) throws IndexMissingException;
}
