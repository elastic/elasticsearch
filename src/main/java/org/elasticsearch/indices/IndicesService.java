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

package org.elasticsearch.indices;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;

import java.util.Set;

/**
 *
 */
public interface IndicesService extends Iterable<IndexService>, LifecycleComponent<IndicesService> {

    /**
     * Returns <tt>true</tt> if changes (adding / removing) indices, shards and so on are allowed.
     */
    public boolean changesAllowed();

    /**
     * Returns the node stats indices stats. The <tt>includePrevious</tt> flag controls
     * if old shards stats will be aggregated as well (only for relevant stats, such as
     * refresh and indexing, not for docs/store).
     */
    NodeIndicesStats stats(boolean includePrevious);

    NodeIndicesStats stats(boolean includePrevious, CommonStatsFlags flags);

    boolean hasIndex(String index);

    IndicesLifecycle indicesLifecycle();

    Set<String> indices();

    IndexService indexService(String index);

    IndexService indexServiceSafe(String index) throws IndexMissingException;

    IndexService createIndex(String index, Settings settings, String localNodeId) throws ElasticSearchException;

    void removeIndex(String index, String reason) throws ElasticSearchException;
}
