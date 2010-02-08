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

package org.elasticsearch.indices;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.settings.Settings;

import java.util.Set;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public interface IndicesService extends Iterable<IndexService>, LifecycleComponent<IndicesService> {

    /**
     * Returns <tt>true</tt> if changes (adding / removing) indices, shards and so on are allowed.
     */
    public boolean changesAllowed();

    boolean hasIndex(String index);

    Set<String> indices();

    IndexService indexService(String index);

    IndexService indexServiceSafe(String index) throws IndexMissingException;

    /**
     * Gets all the "searchable" shards on all the given indices.
     *
     * @see org.elasticsearch.index.routing.OperationRouting#searchShards(org.elasticsearch.cluster.ClusterState, String)
     */
    GroupShardsIterator searchShards(ClusterState clusterState, String[] indices, String queryHint) throws ElasticSearchException;

    IndexService createIndex(String index, Settings settings, String localNodeId) throws ElasticSearchException;

    void deleteIndex(String index) throws ElasticSearchException;
}
