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

package org.elasticsearch.index.cache.filter.node;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.support.AbstractWeightedFilterCache;
import org.elasticsearch.index.cache.filter.support.FilterCacheValue;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.cache.filter.IndicesNodeFilterCache;

import java.util.concurrent.ConcurrentMap;

public class NodeFilterCache extends AbstractWeightedFilterCache {

    private final IndicesNodeFilterCache indicesNodeFilterCache;

    @Inject
    public NodeFilterCache(Index index, @IndexSettings Settings indexSettings, IndicesNodeFilterCache indicesNodeFilterCache) {
        super(index, indexSettings);
        this.indicesNodeFilterCache = indicesNodeFilterCache;

        indicesNodeFilterCache.addEvictionListener(this);
    }

    @Override
    public void close() throws ElasticSearchException {
        indicesNodeFilterCache.removeEvictionListener(this);
        super.close();
    }

    @Override
    protected ConcurrentMap<FilterCacheKey, FilterCacheValue<DocSet>> cache() {
        return indicesNodeFilterCache.cache();
    }

    @Override
    public String type() {
        return "node";
    }
}