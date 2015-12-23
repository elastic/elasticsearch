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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.percolator.QueryCollector.Count;
import org.elasticsearch.percolator.QueryCollector.Match;
import org.elasticsearch.percolator.QueryCollector.MatchAndScore;
import org.elasticsearch.percolator.QueryCollector.MatchAndSort;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.mapper.SourceToParse.source;
import static org.elasticsearch.percolator.QueryCollector.count;
import static org.elasticsearch.percolator.QueryCollector.match;
import static org.elasticsearch.percolator.QueryCollector.matchAndScore;

public class PercolatorService extends AbstractComponent {

    public final static float NO_SCORE = Float.NEGATIVE_INFINITY;
    public final static String TYPE_NAME = ".percolator";

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndicesService indicesService;
    private final IntObjectHashMap<PercolatorType> percolatorTypes;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final ClusterService clusterService;

    private final PercolatorIndex single;
    private final PercolatorIndex multi;

    private final HighlightPhase highlightPhase;
    private final AggregationPhase aggregationPhase;
    private final SortParseElement sortParseElement;
    private final ScriptService scriptService;
    private final MappingUpdatedAction mappingUpdatedAction;

    private final CloseableThreadLocal<MemoryIndex> cache;

    private final ParseFieldMatcher parseFieldMatcher;

    @Inject
    public PercolatorService(Settings settings, IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService,
                             PageCacheRecycler pageCacheRecycler, BigArrays bigArrays,
                             HighlightPhase highlightPhase, ClusterService clusterService,
                             AggregationPhase aggregationPhase, ScriptService scriptService,
                             MappingUpdatedAction mappingUpdatedAction) {
        super(settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.indicesService = indicesService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.clusterService = clusterService;
        this.highlightPhase = highlightPhase;
        this.aggregationPhase = aggregationPhase;
        this.scriptService = scriptService;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.sortParseElement = new SortParseElement();

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

        percolatorTypes = new IntObjectHashMap<>(6);
        percolatorTypes.put(countPercolator.id(), countPercolator);
        percolatorTypes.put(queryCountPercolator.id(), queryCountPercolator);
        percolatorTypes.put(matchPercolator.id(), matchPercolator);
        percolatorTypes.put(queryPercolator.id(), queryPercolator);
        percolatorTypes.put(scoringPercolator.id(), scoringPercolator);
        percolatorTypes.put(topMatchingPercolator.id(), topMatchingPercolator);
    }


    public ReduceResult reduce(byte percolatorTypeId, List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
        PercolatorType percolatorType = percolatorTypes.get(percolatorTypeId);
        return percolatorType.reduce(shardResults, headersContext);
    }

    public PercolateShardResponse percolate(PercolateShardRequest request) {
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
            ParsedDocument parsedDocument = parseRequest(indexShard, request, context, request.shardId().getIndex());
            if (context.percolateQueries().isEmpty()) {
                return new PercolateShardResponse(context, request.shardId());
            }

            if (request.docSource() != null && request.docSource().length() != 0) {
                parsedDocument = parseFetchedDoc(context, request.docSource(), percolateIndexService, request.shardId().getIndex(), request.documentType());
            } else if (parsedDocument == null) {
                throw new IllegalArgumentException("Nothing to percolate");
            }

            if (context.percolateQuery() == null && (context.trackScores() || context.doSort || context.aggregations() != null) || context.aliasFilter() != null) {
                context.percolateQuery(new MatchAllDocsQuery());
            }

            if (context.doSort && !context.limit) {
                throw new IllegalArgumentException("Can't sort if size isn't specified");
            }

            if (context.highlight() != null && !context.limit) {
                throw new IllegalArgumentException("Can't highlight if size isn't specified");
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

            PercolatorType action;
            if (request.onlyCount()) {
                action = context.percolateQuery() != null ? queryCountPercolator : countPercolator;
            } else {
                if (context.doSort) {
                    action = topMatchingPercolator;
                } else if (context.percolateQuery() != null) {
                    action = context.trackScores() ? scoringPercolator : queryPercolator;
                } else {
                    action = matchPercolator;
                }
            }
            context.percolatorTypeId = action.id();

            percolatorIndex.prepare(context, parsedDocument);
            return action.doPercolate(request, context, isNested);
        } finally {
            SearchContext.removeCurrent();
            context.close();
            percolateQueryRegistry.postPercolate(System.nanoTime() - startTime);
        }
    }

    private ParsedDocument parseRequest(IndexShard shard, PercolateShardRequest request, PercolateContext context, String index) {
        BytesReference source = request.source();
        if (source == null || source.length() == 0) {
            return null;
        }

        // TODO: combine all feature parse elements into one map
        Map<String, ? extends SearchParseElement> hlElements = highlightPhase.parseElements();
        Map<String, ? extends SearchParseElement> aggregationElements = aggregationPhase.parseElements();

        ParsedDocument doc = null;
        XContentParser parser = null;

        // Some queries (function_score query when for decay functions) rely on a SearchContext being set:
        // We switch types because this context needs to be in the context of the percolate queries in the shard and
        // not the in memory percolate doc
        String[] previousTypes = context.types();
        context.types(new String[]{TYPE_NAME});
        QueryShardContext queryShardContext = shard.getQueryShardContext();
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    // we need to check the "doc" here, so the next token will be START_OBJECT which is
                    // the actual document starting
                    if ("doc".equals(currentFieldName)) {
                        if (doc != null) {
                            throw new ElasticsearchParseException("Either specify doc or get, not both");
                        }

                        MapperService mapperService = shard.mapperService();
                        DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate(request.documentType());
                        doc = docMapper.getDocumentMapper().parse(source(parser).index(index).type(request.documentType()).flyweight(true));
                        if (docMapper.getMapping() != null) {
                            doc.addDynamicMappingsUpdate(docMapper.getMapping());
                        }
                        if (doc.dynamicMappingsUpdate() != null) {
                            mappingUpdatedAction.updateMappingOnMasterSynchronously(request.shardId().getIndex(), request.documentType(), doc.dynamicMappingsUpdate());
                        }
                        // the document parsing exists the "doc" object, so we need to set the new current field.
                        currentFieldName = parser.currentName();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    SearchParseElement element = hlElements.get(currentFieldName);
                    if (element == null) {
                        element = aggregationElements.get(currentFieldName);
                    }

                    if ("query".equals(currentFieldName)) {
                        if (context.percolateQuery() != null) {
                            throw new ElasticsearchParseException("Either specify query or filter, not both");
                        }
                        context.percolateQuery(queryShardContext.parse(parser).query());
                    } else if ("filter".equals(currentFieldName)) {
                        if (context.percolateQuery() != null) {
                            throw new ElasticsearchParseException("Either specify query or filter, not both");
                        }
                        Query filter = queryShardContext.parseInnerFilter(parser).query();
                        context.percolateQuery(new ConstantScoreQuery(filter));
                    } else if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    } else if (element != null) {
                        element.parse(parser, context);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    }
                } else if (token == null) {
                    break;
                } else if (token.isValue()) {
                    if ("size".equals(currentFieldName)) {
                        context.size(parser.intValue());
                        if (context.size() < 0) {
                            throw new ElasticsearchParseException("size is set to [{}] and is expected to be higher or equal to 0", context.size());
                        }
                    } else if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    } else if ("track_scores".equals(currentFieldName) || "trackScores".equals(currentFieldName)) {
                        context.trackScores(parser.booleanValue());
                    }
                }
            }

            // We need to get the actual source from the request body for highlighting, so parse the request body again
            // and only get the doc source.
            if (context.highlight() != null) {
                parser.close();
                currentFieldName = null;
                parser = XContentFactory.xContent(source).createParser(source);
                token = parser.nextToken();
                assert token == XContentParser.Token.START_OBJECT;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("doc".equals(currentFieldName)) {
                            BytesStreamOutput bStream = new BytesStreamOutput();
                            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE, bStream);
                            builder.copyCurrentStructure(parser);
                            builder.close();
                            doc.setSource(bStream.bytes());
                            break;
                        } else {
                            parser.skipChildren();
                        }
                    } else if (token == null) {
                        break;
                    }
                }
            }

        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse request", e);
        } finally {
            context.types(previousTypes);
            if (parser != null) {
                parser.close();
            }
        }

        return doc;
    }

    private void parseSort(XContentParser parser, PercolateContext context) throws Exception {
        sortParseElement.parse(parser, context);
        // null, means default sorting by relevancy
        if (context.sort() == null) {
            context.doSort = true;
        } else {
            throw new ElasticsearchParseException("Only _score desc is supported");
        }
    }

    private ParsedDocument parseFetchedDoc(PercolateContext context, BytesReference fetchedDoc, IndexService documentIndexService, String index, String type) {
        ParsedDocument doc = null;
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(fetchedDoc).createParser(fetchedDoc);
            MapperService mapperService = documentIndexService.mapperService();
            DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate(type);
            doc = docMapper.getDocumentMapper().parse(source(parser).index(index).type(type).flyweight(true));

            if (context.highlight() != null) {
                doc.setSource(fetchedDoc);
            }
        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse request", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        if (doc == null) {
            throw new ElasticsearchParseException("No doc to percolate in the request");
        }

        return doc;
    }

    public void close() {
        cache.close();
    }

    interface PercolatorType {

        // 0x00 is reserved for empty type.
        byte id();

        ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext);

        PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context, boolean isNested);

    }

    private final PercolatorType countPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x01;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
            long finalCount = 0;
            for (PercolateShardResponse shardResponse : shardResults) {
                finalCount += shardResponse.count();
            }

            assert !shardResults.isEmpty();
            InternalAggregations reducedAggregations = reduceAggregations(shardResults, headersContext);
            return new ReduceResult(finalCount, reducedAggregations);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context, boolean isNested) {
            long count = 0;
            for (Map.Entry<BytesRef, Query> entry : context.percolateQueries().entrySet()) {
                try {
                    Query existsQuery = entry.getValue();
                    if (isNested) {
                        existsQuery = new BooleanQuery.Builder()
                            .add(existsQuery, Occur.MUST)
                            .add(Queries.newNonNestedFilter(), Occur.FILTER)
                            .build();
                    }
                    if (Lucene.exists(context.docSearcher(), existsQuery)) {
                        count ++;
                    }
                } catch (Throwable e) {
                    logger.debug("[" + entry.getKey() + "] failed to execute query", e);
                    throw new PercolateException(context.indexShard().shardId(), "failed to execute", e);
                }
            }
            return new PercolateShardResponse(count, context, request.shardId());
        }

    };

    private final PercolatorType queryCountPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x02;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
            return countPercolator.reduce(shardResults, headersContext);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context, boolean isNested) {
            long count = 0;
            Engine.Searcher percolatorSearcher = context.indexShard().acquireSearcher("percolate");
            try {
                Count countCollector = count(logger, context, isNested);
                queryBasedPercolating(percolatorSearcher, context, countCollector);
                count = countCollector.counter();
            } catch (Throwable e) {
                logger.warn("failed to execute", e);
            } finally {
                percolatorSearcher.close();
            }
            return new PercolateShardResponse(count, context, request.shardId());
        }

    };

    private final PercolatorType matchPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x03;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
            long foundMatches = 0;
            int numMatches = 0;
            for (PercolateShardResponse response : shardResults) {
                foundMatches += response.count();
                numMatches += response.matches().length;
            }
            int requestedSize = shardResults.get(0).requestedSize();

            // Use a custom impl of AbstractBigArray for Object[]?
            List<PercolateResponse.Match> finalMatches = new ArrayList<>(requestedSize == 0 ? numMatches : requestedSize);
            outer:
            for (PercolateShardResponse response : shardResults) {
                Text index = new Text(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? NO_SCORE : response.scores()[i];
                    Text match = new Text(new BytesArray(response.matches()[i]));
                    Map<String, HighlightField> hl = response.hls().isEmpty() ? null : response.hls().get(i);
                    finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                    if (requestedSize != 0 && finalMatches.size() == requestedSize) {
                        break outer;
                    }
                }
            }

            assert !shardResults.isEmpty();
            InternalAggregations reducedAggregations = reduceAggregations(shardResults, headersContext);
            return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]), reducedAggregations);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context, boolean isNested) {
            long count = 0;
            List<BytesRef> matches = new ArrayList<>();
            List<Map<String, HighlightField>> hls = new ArrayList<>();

            for (Map.Entry<BytesRef, Query> entry : context.percolateQueries().entrySet()) {
                if (context.highlight() != null) {
                    context.parsedQuery(new ParsedQuery(entry.getValue()));
                    context.hitContext().cache().clear();
                }
                try {
                    Query existsQuery = entry.getValue();
                    if (isNested) {
                        existsQuery = new BooleanQuery.Builder()
                            .add(existsQuery, Occur.MUST)
                            .add(Queries.newNonNestedFilter(), Occur.FILTER)
                            .build();
                    }
                    if (Lucene.exists(context.docSearcher(), existsQuery)) {
                        if (!context.limit || count < context.size()) {
                            matches.add(entry.getKey());
                            if (context.highlight() != null) {
                                highlightPhase.hitExecute(context, context.hitContext());
                                hls.add(context.hitContext().hit().getHighlightFields());
                            }
                        }
                        count++;
                    }
                } catch (Throwable e) {
                    logger.debug("[" + entry.getKey() + "] failed to execute query", e);
                    throw new PercolateException(context.indexShard().shardId(), "failed to execute", e);
                }
            }

            BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
            return new PercolateShardResponse(finalMatches, hls, count, context, request.shardId());
        }
    };

    private final PercolatorType queryPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x04;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
            return matchPercolator.reduce(shardResults, headersContext);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context, boolean isNested) {
            Engine.Searcher percolatorSearcher = context.indexShard().acquireSearcher("percolate");
            try {
                Match match = match(logger, context, highlightPhase, isNested);
                queryBasedPercolating(percolatorSearcher, context, match);
                List<BytesRef> matches = match.matches();
                List<Map<String, HighlightField>> hls = match.hls();
                long count = match.counter();

                BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
                return new PercolateShardResponse(finalMatches, hls, count, context, request.shardId());
            } catch (Throwable e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard().shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.close();
            }
        }
    };

    private final PercolatorType scoringPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x05;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
            return matchPercolator.reduce(shardResults, headersContext);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context, boolean isNested) {
            Engine.Searcher percolatorSearcher = context.indexShard().acquireSearcher("percolate");
            try {
                MatchAndScore matchAndScore = matchAndScore(logger, context, highlightPhase, isNested);
                queryBasedPercolating(percolatorSearcher, context, matchAndScore);
                List<BytesRef> matches = matchAndScore.matches();
                List<Map<String, HighlightField>> hls = matchAndScore.hls();
                float[] scores = matchAndScore.scores().toArray();
                long count = matchAndScore.counter();

                BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
                return new PercolateShardResponse(finalMatches, hls, count, scores, context, request.shardId());
            } catch (Throwable e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard().shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.close();
            }
        }
    };

    private final PercolatorType topMatchingPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x06;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
            long foundMatches = 0;
            int nonEmptyResponses = 0;
            int firstNonEmptyIndex = 0;
            for (int i = 0; i < shardResults.size(); i++) {
                PercolateShardResponse response = shardResults.get(i);
                foundMatches += response.count();
                if (response.matches().length != 0) {
                    if (firstNonEmptyIndex == 0) {
                        firstNonEmptyIndex = i;
                    }
                    nonEmptyResponses++;
                }
            }

            int requestedSize = shardResults.get(0).requestedSize();

            // Use a custom impl of AbstractBigArray for Object[]?
            List<PercolateResponse.Match> finalMatches = new ArrayList<>(requestedSize);
            if (nonEmptyResponses == 1) {
                PercolateShardResponse response = shardResults.get(firstNonEmptyIndex);
                Text index = new Text(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? Float.NaN : response.scores()[i];
                    Text match = new Text(new BytesArray(response.matches()[i]));
                    if (!response.hls().isEmpty()) {
                        Map<String, HighlightField> hl = response.hls().get(i);
                        finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                    } else {
                        finalMatches.add(new PercolateResponse.Match(index, match, score));
                    }
                }
            } else {
                int[] slots = new int[shardResults.size()];
                while (true) {
                    float lowestScore = Float.NEGATIVE_INFINITY;
                    int requestIndex = -1;
                    int itemIndex = -1;
                    for (int i = 0; i < shardResults.size(); i++) {
                        int scoreIndex = slots[i];
                        float[] scores = shardResults.get(i).scores();
                        if (scoreIndex >= scores.length) {
                            continue;
                        }

                        float score = scores[scoreIndex];
                        int cmp = Float.compare(lowestScore, score);
                        // TODO: Maybe add a tie?
                        if (cmp < 0) {
                            requestIndex = i;
                            itemIndex = scoreIndex;
                            lowestScore = score;
                        }
                    }

                    // This means the shard matches have been exhausted and we should bail
                    if (requestIndex == -1) {
                        break;
                    }

                    slots[requestIndex]++;

                    PercolateShardResponse shardResponse = shardResults.get(requestIndex);
                    Text index = new Text(shardResponse.getIndex());
                    Text match = new Text(new BytesArray(shardResponse.matches()[itemIndex]));
                    float score = shardResponse.scores()[itemIndex];
                    if (!shardResponse.hls().isEmpty()) {
                        Map<String, HighlightField> hl = shardResponse.hls().get(itemIndex);
                        finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                    } else {
                        finalMatches.add(new PercolateResponse.Match(index, match, score));
                    }
                    if (finalMatches.size() == requestedSize) {
                        break;
                    }
                }
            }

            assert !shardResults.isEmpty();
            InternalAggregations reducedAggregations = reduceAggregations(shardResults, headersContext);
            return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]), reducedAggregations);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context, boolean isNested) {
            Engine.Searcher percolatorSearcher = context.indexShard().acquireSearcher("percolate");
            try {
                MatchAndSort matchAndSort = QueryCollector.matchAndSort(logger, context, isNested);
                queryBasedPercolating(percolatorSearcher, context, matchAndSort);
                TopDocs topDocs = matchAndSort.topDocs();
                long count = topDocs.totalHits;
                List<BytesRef> matches = new ArrayList<>(topDocs.scoreDocs.length);
                float[] scores = new float[topDocs.scoreDocs.length];
                List<Map<String, HighlightField>> hls = null;
                if (context.highlight() != null) {
                    hls = new ArrayList<>(topDocs.scoreDocs.length);
                }

                final MappedFieldType uidMapper = context.mapperService().fullName(UidFieldMapper.NAME);
                final IndexFieldData<?> uidFieldData = context.fieldData().getForField(uidMapper);
                int i = 0;
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    int segmentIdx = ReaderUtil.subIndex(scoreDoc.doc, percolatorSearcher.reader().leaves());
                    LeafReaderContext atomicReaderContext = percolatorSearcher.reader().leaves().get(segmentIdx);
                    SortedBinaryDocValues values = uidFieldData.load(atomicReaderContext).getBytesValues();
                    final int localDocId = scoreDoc.doc - atomicReaderContext.docBase;
                    values.setDocument(localDocId);
                    final int numValues = values.count();
                    assert numValues == 1;
                    BytesRef bytes = Uid.splitUidIntoTypeAndId(values.valueAt(0))[1];
                    matches.add(BytesRef.deepCopyOf(bytes));
                    if (hls != null) {
                        Query query = context.percolateQueries().get(bytes);
                        context.parsedQuery(new ParsedQuery(query));
                        context.hitContext().cache().clear();
                        highlightPhase.hitExecute(context, context.hitContext());
                        hls.add(i, context.hitContext().hit().getHighlightFields());
                    }
                    scores[i++] = scoreDoc.score;
                }
                if (hls != null) {
                    return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), hls, count, scores, context, request.shardId());
                } else {
                    return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), count, scores, context, request.shardId());
                }
            } catch (Throwable e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard().shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.close();
            }
        }

    };

    private void queryBasedPercolating(Engine.Searcher percolatorSearcher, PercolateContext context, QueryCollector percolateCollector) throws IOException {
        Query percolatorTypeFilter = context.indexService().mapperService().documentMapper(TYPE_NAME).typeFilter();

        final Query filter;
        if (context.aliasFilter() != null) {
            BooleanQuery.Builder booleanFilter = new BooleanQuery.Builder();
            booleanFilter.add(context.aliasFilter(), BooleanClause.Occur.MUST);
            booleanFilter.add(percolatorTypeFilter, BooleanClause.Occur.MUST);
            filter = booleanFilter.build();
        } else {
            filter = percolatorTypeFilter;
        }

        Query query = Queries.filtered(context.percolateQuery(), filter);
        percolatorSearcher.searcher().search(query, percolateCollector);
        percolateCollector.aggregatorCollector.postCollection();
        if (context.aggregations() != null) {
            aggregationPhase.execute(context);
        }
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

    private InternalAggregations reduceAggregations(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
        if (shardResults.get(0).aggregations() == null) {
            return null;
        }

        List<InternalAggregations> aggregationsList = new ArrayList<>(shardResults.size());
        for (PercolateShardResponse shardResult : shardResults) {
            aggregationsList.add(shardResult.aggregations());
        }
        InternalAggregations aggregations = InternalAggregations.reduce(aggregationsList, new ReduceContext(bigArrays, scriptService,
                headersContext));
        if (aggregations != null) {
            List<SiblingPipelineAggregator> pipelineAggregators = shardResults.get(0).pipelineAggregators();
            if (pipelineAggregators != null) {
                List<InternalAggregation> newAggs = StreamSupport.stream(aggregations.spliterator(), false).map((p) -> {
                    return (InternalAggregation) p;
                }).collect(Collectors.toList());
                for (SiblingPipelineAggregator pipelineAggregator : pipelineAggregators) {
                    InternalAggregation newAgg = pipelineAggregator.doReduce(new InternalAggregations(newAggs), new ReduceContext(
                            bigArrays, scriptService, headersContext));
                    newAggs.add(newAgg);
                }
                aggregations = new InternalAggregations(newAggs);
            }
        }
        return aggregations;
    }

}
