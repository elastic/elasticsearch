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

package org.elasticsearch.index.cache.filter;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexService;

import java.io.Closeable;

/**
 *
 */
public interface FilterCache extends IndexComponent, Closeable {

    static class EntriesStats {
        public final long sizeInBytes;
        public final long count;

        public EntriesStats(long sizeInBytes, long count) {
            this.sizeInBytes = sizeInBytes;
            this.count = count;
        }
    }

    // we need to "inject" the index service to not create cyclic dep
    void setIndexService(IndexService indexService);

    String type();

    Filter cache(Filter filterToCache, @Nullable HashedBytesRef cacheKey, QueryCachingPolicy policy);

    void clear(Object reader);

    void clear(String reason);

    void clear(String reason, String[] keys);
}
