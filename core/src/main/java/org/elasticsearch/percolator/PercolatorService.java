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
package org.elasticsearch.percolator;

import com.carrotsearch.hppc.IntObjectHashMap;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PercolatorService extends AbstractComponent {

    public final static float NO_SCORE = Float.NEGATIVE_INFINITY;
    public final static String TYPE_NAME = ".percolator";

    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final AggregationPhase aggregationPhase;
    private final PageCacheRecycler pageCacheRecycler;
    private final IntObjectHashMap<PercolatorType> percolatorTypes;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final PercolatorIndex single;
    private final PercolatorIndex multi;

    private final CountPercolatorType countPercolatorType;
    private final TopMatchingPercolatorType topMatchingPercolator;
    private final PercolateDocumentParser percolateDocumentParser;

    private final CloseableThreadLocal<MemoryIndex> cache;

    private final ParseFieldMatcher parseFieldMatcher;

    @Inject
    public PercolatorService(Settings settings, IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService,
                             PageCacheRecycler pageCacheRecycler, BigArrays bigArrays,
                             HighlightPhase highlightPhase, ClusterService clusterService,
                             AggregationPhase aggregationPhase, ScriptService scriptService,
                             PercolateDocumentParser percolateDocumentParser) {
        super(settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.percolateDocumentParser = percolateDocumentParser;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.indicesService = indicesService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.aggregationPhase = aggregationPhase;

        final long maxReuseBytes = settings.getAsBytesSize("indices.memory.memory_index.size_per_thread", new ByteSizeValue(1, ByteSizeUnit.MB)).bytes();
        cache = new CloseableThreadLocal<MemoryIndex>() {
            @Override
            protected MemoryIndex initialValue() {
                // TODO: should we expose payloads as an option? should offsets be turned on always?
                return new ExtendedMemoryIndex(true, false, maxReuseBytes);
            }
        };
        single = new SingleDocumentPercolatorIndex(cache);
        multi = new MultiDocumentPercolatorIndex(cache);

        percolatorTypes = new IntObjectHashMap<>(4);
        countPercolatorType = new CountPercolatorType(bigArrays, scriptService);
        percolatorTypes.put(countPercolatorType.id(), countPercolatorType);
        topMatchingPercolator = new TopMatchingPercolatorType(bigArrays, scriptService, highlightPhase);
        percolatorTypes.put(topMatchingPercolator.id(), topMatchingPercolator);
    }


    public ReduceResult reduce(byte percolatorTypeId, List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
        PercolatorType percolatorType = percolatorTypes.get(percolatorTypeId);
        try {
            return percolatorType.reduce(shardResults, headersContext);
        } catch (IOException e) {
            throw new ElasticsearchException("exception during reduce", e);
        }
    }

    public PercolateShardResponse percolate(PercolateShardRequest request) throws IOException {
        IndexService percolateIndexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = percolateIndexService.getShard(request.shardId().id());
        indexShard.readAllowed(); // check if we can read the shard...
        PercolatorQueriesRegistry percolateQueryRegistry = indexShard.percolateRegistry();
        percolateQueryRegistry.prePercolate();
        long startTime = System.nanoTime();

        // TODO: The filteringAliases should be looked up at the coordinating node and serialized with all shard request,
        // just like is done in other apis.
        String[] filteringAliases = indexNameExpressionResolver.filteringAliases(
                clusterService.state(),
                indexShard.shardId().index().name(),
                request.indices()
        );
        Query aliasFilter = percolateIndexService.aliasFilter(indexShard.getQueryShardContext(), filteringAliases);

        SearchShardTarget searchShardTarget = new SearchShardTarget(clusterService.localNode().id(), request.shardId().getIndex(), request.shardId().id());
        final PercolateContext context = new PercolateContext(
                request, searchShardTarget, indexShard, percolateIndexService, pageCacheRecycler, bigArrays, scriptService, aliasFilter, parseFieldMatcher
        );
        SearchContext.setCurrent(context);
        try {
            ParsedDocument parsedDocument = percolateDocumentParser.parse(request, context, percolateIndexService.mapperService(), percolateIndexService.getQueryShardContext());

            PercolatorQueriesRegistry queriesRegistry = indexShard.percolateRegistry();
            if (queriesRegistry.getPercolateQueries().isEmpty()) {
                return new PercolateShardResponse(Lucene.EMPTY_TOP_DOCS, Collections.emptyMap(), Collections.emptyMap(), context);
            }
            if (context.size() < 0) {
                context.size(0);
            }

            // parse the source either into one MemoryIndex, if it is a single document or index multiple docs if nested
            PercolatorIndex percolatorIndex;
            boolean isNested = indexShard.mapperService().documentMapper(request.documentType()).hasNestedObjects();
            if (parsedDocument.docs().size() > 1) {
                assert isNested;
                percolatorIndex = multi;
            } else {
                percolatorIndex = single;
            }

            final PercolatorType action;
            if (request.onlyCount()) {
                action = countPercolatorType;
            } else {
                action = topMatchingPercolator;
            }
            context.percolatorTypeId = action.id();

            percolatorIndex.prepare(context, parsedDocument);

            BucketCollector aggregatorCollector = null;
            if (context.aggregations() != null) {
                AggregationContext aggregationContext = new AggregationContext(context);
                context.aggregations().aggregationContext(aggregationContext);

                Aggregator[] aggregators = context.aggregations().factories().createTopLevelAggregators(aggregationContext);
                List<Aggregator> aggregatorCollectors = new ArrayList<>(aggregators.length);
                for (int i = 0; i < aggregators.length; i++) {
                    if (!(aggregators[i] instanceof GlobalAggregator)) {
                        Aggregator aggregator = aggregators[i];
                        aggregatorCollectors.add(aggregator);
                    }
                }
                context.aggregations().aggregators(aggregators);
                aggregatorCollector = BucketCollector.wrap(aggregatorCollectors);
                aggregatorCollector.preCollection();
            }
            Query percolatorTypeFilter = context.indexService().mapperService().documentMapper(TYPE_NAME).typeFilter();
            Collector collector = action.doPercolate(context.percolateQuery(), aliasFilter, percolatorTypeFilter, queriesRegistry, context.searcher(), context.docSearcher(), context.size(), aggregatorCollector);
            if (aggregatorCollector != null) {
                aggregatorCollector.postCollection();
                aggregationPhase.execute(context);
            }
            return action.processResults(context, queriesRegistry, collector);
        } finally {
            SearchContext.removeCurrent();
            context.close();
            percolateQueryRegistry.postPercolate(System.nanoTime() - startTime);
        }
    }

    public void close() {
        cache.close();
    }

    public final static class ReduceResult {

        private final long count;
        private final PercolateResponse.Match[] matches;
        private final InternalAggregations reducedAggregations;

        ReduceResult(long count, PercolateResponse.Match[] matches, InternalAggregations reducedAggregations) {
            this.count = count;
            this.matches = matches;
            this.reducedAggregations = reducedAggregations;
        }

        public ReduceResult(long count, InternalAggregations reducedAggregations) {
            this.count = count;
            this.matches = null;
            this.reducedAggregations = reducedAggregations;
        }

        public long count() {
            return count;
        }

        public PercolateResponse.Match[] matches() {
            return matches;
        }

        public InternalAggregations reducedAggregations() {
            return reducedAggregations;
        }
    }


}
