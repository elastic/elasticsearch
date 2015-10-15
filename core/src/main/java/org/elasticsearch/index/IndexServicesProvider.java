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

package org.elasticsearch.index;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Simple provider class that holds the Index and Node level services used by
 * a shard.
 * This is just a temporary solution until we cleaned up index creation and removed injectors on that level as well.
 */
public final class IndexServicesProvider {

    private final IndicesLifecycle indicesLifecycle;
    private final ThreadPool threadPool;
    private final MapperService mapperService;
    private final IndexQueryParserService queryParserService;
    private final IndexCache indexCache;
    private final IndicesQueryCache indicesQueryCache;
    private final CodecService codecService;
    private final TermVectorsService termVectorsService;
    private final IndexFieldDataService indexFieldDataService;
    private final IndicesWarmer warmer;
    private final SimilarityService similarityService;
    private final EngineFactory factory;
    private final BigArrays bigArrays;
    private final IndexSearcherWrapper indexSearcherWrapper;
    private final IndexingMemoryController indexingMemoryController;

    @Inject
    public IndexServicesProvider(IndicesLifecycle indicesLifecycle, ThreadPool threadPool, MapperService mapperService, IndexQueryParserService queryParserService, IndexCache indexCache, IndicesQueryCache indicesQueryCache, CodecService codecService, TermVectorsService termVectorsService, IndexFieldDataService indexFieldDataService, @Nullable IndicesWarmer warmer, SimilarityService similarityService, EngineFactory factory, BigArrays bigArrays, @Nullable IndexSearcherWrapper indexSearcherWrapper, IndexingMemoryController indexingMemoryController) {
        this.indicesLifecycle = indicesLifecycle;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.indexCache = indexCache;
        this.indicesQueryCache = indicesQueryCache;
        this.codecService = codecService;
        this.termVectorsService = termVectorsService;
        this.indexFieldDataService = indexFieldDataService;
        this.warmer = warmer;
        this.similarityService = similarityService;
        this.factory = factory;
        this.bigArrays = bigArrays;
        this.indexSearcherWrapper = indexSearcherWrapper;
        this.indexingMemoryController = indexingMemoryController;
    }

    public IndicesLifecycle getIndicesLifecycle() {
        return indicesLifecycle;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public MapperService getMapperService() {
        return mapperService;
    }

    public IndexQueryParserService getQueryParserService() {
        return queryParserService;
    }

    public IndexCache getIndexCache() {
        return indexCache;
    }

    public IndicesQueryCache getIndicesQueryCache() {
        return indicesQueryCache;
    }

    public CodecService getCodecService() {
        return codecService;
    }

    public TermVectorsService getTermVectorsService() {
        return termVectorsService;
    }

    public IndexFieldDataService getIndexFieldDataService() {
        return indexFieldDataService;
    }

    public IndicesWarmer getWarmer() {
        return warmer;
    }

    public SimilarityService getSimilarityService() {
        return similarityService;
    }

    public EngineFactory getFactory() {
        return factory;
    }

    public BigArrays getBigArrays() {
        return bigArrays;
    }

    public IndexSearcherWrapper getIndexSearcherWrapper() {
        return indexSearcherWrapper;
    }

    public IndexingMemoryController getIndexingMemoryController() {
        return indexingMemoryController;
    }
}
