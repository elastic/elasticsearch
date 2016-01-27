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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.Version;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.PercolatorFieldMapper;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;

public class PercolatorService extends AbstractComponent {

    public final static float NO_SCORE = Float.NEGATIVE_INFINITY;
    public final static String TYPE_NAME = ".percolator";

    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final HighlightPhase highlightPhase;
    private final AggregationPhase aggregationPhase;
    private final PageCacheRecycler pageCacheRecycler;
    private final ParseFieldMatcher parseFieldMatcher;
    private final CloseableThreadLocal<MemoryIndex> cache;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final PercolateDocumentParser percolateDocumentParser;

    private final PercolatorIndex single;
    private final PercolatorIndex multi;

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
        this.highlightPhase = highlightPhase;

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
    }

    public ReduceResult reduce(boolean onlyCount, List<PercolateShardResponse> shardResponses) throws IOException {
        if (onlyCount) {
            long finalCount = 0;
            for (PercolateShardResponse shardResponse : shardResponses) {
                finalCount += shardResponse.topDocs().totalHits;
            }

            InternalAggregations reducedAggregations = reduceAggregations(shardResponses);
            return new PercolatorService.ReduceResult(finalCount, reducedAggregations);
        } else {
            int requestedSize = shardResponses.get(0).requestedSize();
            TopDocs[] shardResults = new TopDocs[shardResponses.size()];
            long foundMatches = 0;
            for (int i = 0; i < shardResults.length; i++) {
                TopDocs shardResult = shardResponses.get(i).topDocs();
                foundMatches += shardResult.totalHits;
                shardResults[i] = shardResult;
            }
            TopDocs merged = TopDocs.merge(requestedSize, shardResults);
            PercolateResponse.Match[] matches = new PercolateResponse.Match[merged.scoreDocs.length];
            for (int i = 0; i < merged.scoreDocs.length; i++) {
                ScoreDoc doc = merged.scoreDocs[i];
                PercolateShardResponse shardResponse = shardResponses.get(doc.shardIndex);
                String id = shardResponse.ids().get(doc.doc);
                Map<String, HighlightField> hl = shardResponse.hls().get(doc.doc);
                matches[i] = new PercolateResponse.Match(new Text(shardResponse.getIndex()), new Text(id), doc.score, hl);
            }
            InternalAggregations reducedAggregations = reduceAggregations(shardResponses);
            return new PercolatorService.ReduceResult(foundMatches, matches, reducedAggregations);
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

            if (context.searcher().getIndexReader().maxDoc() == 0) {
                return new PercolateShardResponse(Lucene.EMPTY_TOP_DOCS, Collections.emptyMap(), Collections.emptyMap(), context);
            }
            if (context.size() < 0) {
                context.size(0);
            }

            // parse the source either into one MemoryIndex, if it is a single document or index multiple docs if nested
            PercolatorIndex percolatorIndex;
            DocumentMapper documentMapper = indexShard.mapperService().documentMapper(request.documentType());
            boolean isNested = documentMapper != null && documentMapper.hasNestedObjects();
            if (parsedDocument.docs().size() > 1) {
                assert isNested;
                percolatorIndex = multi;
            } else {
                percolatorIndex = single;
            }
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
            PercolatorQueriesRegistry queriesRegistry = indexShard.percolateRegistry();
            return doPercolate(context, queriesRegistry, aggregationPhase, aggregatorCollector, highlightPhase);
        } finally {
            SearchContext.removeCurrent();
            context.close();
            percolateQueryRegistry.postPercolate(System.nanoTime() - startTime);
        }
    }

    // moved the core percolation logic to a pck protected method to make testing easier:
    static PercolateShardResponse doPercolate(PercolateContext context, PercolatorQueriesRegistry queriesRegistry, AggregationPhase aggregationPhase, @Nullable BucketCollector aggregatorCollector, HighlightPhase highlightPhase) throws IOException {
        PercolatorQuery.Builder builder = new PercolatorQuery.Builder(context.docSearcher(), queriesRegistry.getPercolateQueries(), context.percolatorTypeFilter());
        if (queriesRegistry.indexSettings().getSettings().getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null).onOrAfter(Version.V_3_0_0)) {
            builder.extractQueryTermsQuery(PercolatorFieldMapper.EXTRACTED_TERMS_FULL_FIELD_NAME, PercolatorFieldMapper.UNKNOWN_QUERY_FULL_FIELD_NAME);
        }
        if (context.percolateQuery() != null || context.aliasFilter() != null) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            if (context.percolateQuery() != null) {
                bq.add(context.percolateQuery(), MUST);
            }
            if (context.aliasFilter() != null) {
                bq.add(context.aliasFilter(), FILTER);
            }
            builder.setPercolateQuery(bq.build());
        }
        PercolatorQuery percolatorQuery = builder.build();

        if (context.isOnlyCount() || context.size() == 0) {
            TotalHitCountCollector collector = new TotalHitCountCollector();
            context.searcher().search(percolatorQuery, MultiCollector.wrap(collector, aggregatorCollector));
            if (aggregatorCollector != null) {
                aggregatorCollector.postCollection();
                aggregationPhase.execute(context);
            }
            return new PercolateShardResponse(new TopDocs(collector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0f), Collections.emptyMap(), Collections.emptyMap(), context);
        } else {
            int size = context.size();
            if (size > context.searcher().getIndexReader().maxDoc()) {
                // prevent easy OOM if more than the total number of docs that exist is requested...
                size = context.searcher().getIndexReader().maxDoc();
            }
            TopScoreDocCollector collector = TopScoreDocCollector.create(size);
            context.searcher().search(percolatorQuery, MultiCollector.wrap(collector, aggregatorCollector));
            if (aggregatorCollector != null) {
                aggregatorCollector.postCollection();
                aggregationPhase.execute(context);
            }

            TopDocs topDocs = collector.topDocs();
            Map<Integer, String> ids = new HashMap<>(topDocs.scoreDocs.length);
            Map<Integer, Map<String, HighlightField>> hls = new HashMap<>(topDocs.scoreDocs.length);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                if (context.trackScores() == false) {
                    // No sort or tracking scores was provided, so use special value to indicate to not show the scores:
                    scoreDoc.score = NO_SCORE;
                }

                int segmentIdx = ReaderUtil.subIndex(scoreDoc.doc, context.searcher().getIndexReader().leaves());
                LeafReaderContext atomicReaderContext = context.searcher().getIndexReader().leaves().get(segmentIdx);
                final int segmentDocId = scoreDoc.doc - atomicReaderContext.docBase;
                SingleFieldsVisitor fieldsVisitor = new SingleFieldsVisitor(UidFieldMapper.NAME);
                atomicReaderContext.reader().document(segmentDocId, fieldsVisitor);
                String id = fieldsVisitor.uid().id();
                ids.put(scoreDoc.doc, id);
                if (context.highlight() != null) {
                    Query query = queriesRegistry.getPercolateQueries().get(new BytesRef(id));
                    context.parsedQuery(new ParsedQuery(query));
                    context.hitContext().cache().clear();
                    highlightPhase.hitExecute(context, context.hitContext());
                    hls.put(scoreDoc.doc, context.hitContext().hit().getHighlightFields());
                }
            }
            return new PercolateShardResponse(topDocs, ids, hls, context);
        }
    }

    public void close() {
        cache.close();
    }

    private InternalAggregations reduceAggregations(List<PercolateShardResponse> shardResults) {
        if (shardResults.get(0).aggregations() == null) {
            return null;
        }

        List<InternalAggregations> aggregationsList = new ArrayList<>(shardResults.size());
        for (PercolateShardResponse shardResult : shardResults) {
            aggregationsList.add(shardResult.aggregations());
        }
        InternalAggregations aggregations = InternalAggregations.reduce(aggregationsList, new InternalAggregation.ReduceContext(bigArrays, scriptService));
        if (aggregations != null) {
            List<SiblingPipelineAggregator> pipelineAggregators = shardResults.get(0).pipelineAggregators();
            if (pipelineAggregators != null) {
                List<InternalAggregation> newAggs = StreamSupport.stream(aggregations.spliterator(), false).map((p) -> {
                    return (InternalAggregation) p;
                }).collect(Collectors.toList());
                for (SiblingPipelineAggregator pipelineAggregator : pipelineAggregators) {
                    InternalAggregation newAgg = pipelineAggregator.doReduce(new InternalAggregations(newAggs), new InternalAggregation.ReduceContext(bigArrays, scriptService));
                    newAggs.add(newAgg);
                }
                aggregations = new InternalAggregations(newAggs);
            }
        }
        return aggregations;
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
