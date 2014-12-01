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

package org.elasticsearch.indices.cache.termvectors;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;

import java.util.concurrent.Callable;

/**
 * Mostly wraps on {@link org.elasticsearch.indices.cache.query.IndicesQueryCache} to provide shard level caching
 * for term vectors coherent with NRT semantics.
 */
public class IndicesTermVectorsCache extends AbstractComponent implements RemovalListener<IndicesQueryCache.Key, BytesReference> {

    private final IndicesQueryCache indicesQueryCache;
    private final ClusterService clusterService;

    @Inject
    public IndicesTermVectorsCache(Settings settings, ClusterService clusterService, IndicesQueryCache indicesQueryCache) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesQueryCache = indicesQueryCache;
    }

    public void close() {
        indicesQueryCache.close();
    }

    public void clear(org.elasticsearch.index.shard.service.IndexShard shard) {
        indicesQueryCache.clear(shard);
    }

    @Override
    public void onRemoval(RemovalNotification<IndicesQueryCache.Key, BytesReference> notification) {
        indicesQueryCache.onRemoval(notification);
    }

    /**
     * Can the shard request be cached at all?
     */
    public boolean canCache(TermVectorRequest request, IndexReader reader) {
        String index = clusterService.state().getMetaData().concreteSingleIndex(request.index(), request.indicesOptions());
        if (index == null) { // in case we didn't yet have the cluster state, or it just got deleted
            return false;
        }
        // must be explicitly set in the request
        if (request.cache() == null || !request.cache()) {
            return false;
        }
        // if the reader is not a directory reader, we can't get the version from it
        if (!(reader instanceof DirectoryReader)) {
            return false;
        }
        return true;
    }

    /**
     * Loads the cache result, computing it if needed by executing the term vector request. The combination of load + compute allows
     * to have a single load operation that will cause other requests with the same key to wait till its loaded an reuse
     * the same cache.
     */
    public TermVectorResponse load(final TermVectorRequest request, IndexShard indexShard, Engine.Searcher searcher) throws Exception {
        final IndexReader reader = searcher.reader();

        assert canCache(request, reader);
        TermVectorResponse response = new TermVectorResponse();

        IndicesQueryCache.Key key = buildKey(request, indexShard, reader);
        Loader loader = new Loader(indexShard, request, searcher, key);
        BytesReference value = indicesQueryCache.getCache().get(key, loader);
        if (loader.isLoaded()) {
            key.shard.queryCache().onMiss();
            // see if its the first time we see this reader, and make sure to register a cleanup key
            indicesQueryCache.registerCleanupKey(indexShard, reader);
        } else {
            key.shard.queryCache().onHit();
        }
        response.readFrom(value.streamInput());
        return response;
    }

    private static class Loader implements Callable<BytesReference> {
        private final IndexShard indexShard;
        private final TermVectorRequest request;
        private final Engine.Searcher searcher;
        private final IndicesQueryCache.Key key;
        private boolean loaded;

        Loader(IndexShard indexShard, TermVectorRequest request, Engine.Searcher searcher, IndicesQueryCache.Key key) {
            this.indexShard = indexShard;
            this.request = request;
            this.searcher = searcher;
            this.key = key;
        }

        public boolean isLoaded() {
            return this.loaded;
        }

        @Override
        public BytesReference call() throws Exception {
            TermVectorResponse termVector = indexShard.termVectorService().getTermVector(request, searcher);
            BytesStreamOutput out = new BytesStreamOutput();
            termVector.writeTo(out);
            // for now, keep the paged data structure, which might have unused bytes to fill a page, but better to keep
            // the memory properly paged instead of having varied sized bytes
            BytesReference value = out.bytes();
            loaded = true;
            key.shard.queryCache().onCached(key, value);
            return value;
        }
    }

    private IndicesQueryCache.Key buildKey(TermVectorRequest request, IndexShard indexShard, IndexReader reader) throws Exception {
        long version = ((DirectoryReader) reader).getVersion();
        IndicesQueryCache.Key key = new IndicesQueryCache.Key(indexShard, version, request.cacheKey());
        key.keyType = "_termvectors";
        return key;
    }

}
