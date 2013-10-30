/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.percolator;

import com.carrotsearch.hppc.ByteObjectOpenHashMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.XCollector;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.stats.ShardPercolateService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.percolator.QueryCollector.Count;
import org.elasticsearch.percolator.QueryCollector.Match;
import org.elasticsearch.percolator.QueryCollector.MatchAndScore;
import org.elasticsearch.percolator.QueryCollector.MatchAndSort;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhase;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.SourceToParse.source;
import static org.elasticsearch.percolator.QueryCollector.*;

/**
 */
public class PercolatorService extends AbstractComponent {

    public final static float NO_SCORE = Float.NEGATIVE_INFINITY;

    private final CloseableThreadLocal<MemoryIndex> cache;
    private final IndicesService indicesService;
    private final ByteObjectOpenHashMap<PercolatorType> percolatorTypes;
    private final CacheRecycler cacheRecycler;
    private final ClusterService clusterService;

    private final FacetPhase facetPhase;
    private final HighlightPhase highlightPhase;

    @Inject
    public PercolatorService(Settings settings, IndicesService indicesService, CacheRecycler cacheRecycler, HighlightPhase highlightPhase, ClusterService clusterService, FacetPhase facetPhase) {
        super(settings);
        this.indicesService = indicesService;
        this.cacheRecycler = cacheRecycler;
        this.clusterService = clusterService;
        this.highlightPhase = highlightPhase;
        this.facetPhase = facetPhase;

        final long maxReuseBytes = settings.getAsBytesSize("indices.memory.memory_index.size_per_thread", new ByteSizeValue(1, ByteSizeUnit.MB)).bytes();
        cache = new CloseableThreadLocal<MemoryIndex>() {
            @Override
            protected MemoryIndex initialValue() {
                return new ExtendedMemoryIndex(true, maxReuseBytes);
            }
        };

        percolatorTypes = new ByteObjectOpenHashMap<PercolatorType>(6);
        percolatorTypes.put(countPercolator.id(), countPercolator);
        percolatorTypes.put(queryCountPercolator.id(), queryCountPercolator);
        percolatorTypes.put(matchPercolator.id(), matchPercolator);
        percolatorTypes.put(queryPercolator.id(), queryPercolator);
        percolatorTypes.put(scoringPercolator.id(), scoringPercolator);
        percolatorTypes.put(topMatchingPercolator.id(), topMatchingPercolator);
    }


    public ReduceResult reduce(byte percolatorTypeId, List<PercolateShardResponse> shardResults) {
        PercolatorType percolatorType = percolatorTypes.get(percolatorTypeId);
        return percolatorType.reduce(shardResults);
    }

    public PercolateShardResponse percolate(PercolateShardRequest request) {
        IndexService percolateIndexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = percolateIndexService.shardSafe(request.shardId());

        ShardPercolateService shardPercolateService = indexShard.shardPercolateService();
        shardPercolateService.prePercolate();
        long startTime = System.nanoTime();

        SearchShardTarget searchShardTarget = new SearchShardTarget(clusterService.localNode().id(), request.index(), request.shardId());
        final PercolateContext context = new PercolateContext(
                request, searchShardTarget, indexShard, percolateIndexService, cacheRecycler
        );
        try {

            ParsedDocument parsedDocument = parseRequest(percolateIndexService, request, context);
            if (context.percolateQueries().isEmpty()) {
                return new PercolateShardResponse(context, request.index(), request.shardId());
            }

            if (request.docSource() != null && request.docSource().length() != 0) {
                parsedDocument = parseFetchedDoc(request.docSource(), percolateIndexService, request.documentType());
            } else if (parsedDocument == null) {
                throw new ElasticSearchIllegalArgumentException("Nothing to percolate");
            }

            if (context.percolateQuery() == null && (context.score || context.sort || context.facets() != null)) {
                context.percolateQuery(new MatchAllDocsQuery());
            }

            if (context.sort && !context.limit) {
                throw new ElasticSearchIllegalArgumentException("Can't sort if size isn't specified");
            }

            if (context.highlight() != null && !context.limit) {
                throw new ElasticSearchIllegalArgumentException("Can't highlight if size isn't specified");
            }

            if (context.size < 0) {
                context.size = 0;
            }

            // first, parse the source doc into a MemoryIndex
            final MemoryIndex memoryIndex = cache.get();
            // TODO: This means percolation does not support nested docs...
            // So look into: ByteBufferDirectory
            for (IndexableField field : parsedDocument.rootDoc().getFields()) {
                if (!field.fieldType().indexed() && field.name().equals(UidFieldMapper.NAME)) {
                    continue;
                }
                try {
                    TokenStream tokenStream = field.tokenStream(parsedDocument.analyzer());
                    if (tokenStream != null) {
                        memoryIndex.addField(field.name(), tokenStream, field.boost());
                    }
                } catch (IOException e) {
                    throw new ElasticSearchException("Failed to create token stream", e);
                }
            }

            PercolatorType action;
            if (request.onlyCount()) {
                action = context.percolateQuery() != null ? queryCountPercolator : countPercolator;
            } else {
                if (context.sort) {
                    action = topMatchingPercolator;
                } else if (context.percolateQuery() != null) {
                    action = context.score ? scoringPercolator : queryPercolator;
                } else {
                    action = matchPercolator;
                }
            }
            context.percolatorTypeId = action.id();

            context.initialize(memoryIndex, parsedDocument);
            return action.doPercolate(request, context);
        } finally {
            context.release();
            shardPercolateService.postPercolate(System.nanoTime() - startTime);
        }
    }

    private ParsedDocument parseRequest(IndexService documentIndexService, PercolateShardRequest request, PercolateContext context) throws ElasticSearchException {
        BytesReference source = request.source();
        if (source == null || source.length() == 0) {
            return null;
        }

        Map<String, ? extends SearchParseElement> hlElements = highlightPhase.parseElements();
        Map<String, ? extends SearchParseElement> facetElements = facetPhase.parseElements();

        ParsedDocument doc = null;
        XContentParser parser = null;

        // Some queries (function_score query when for decay functions) rely on a SearchContext being set:
        // We switch types because this context needs to be in the context of the percolate queries in the shard and
        // not the in memory percolate doc
        String[] previousTypes = context.types();
        context.types(new String[]{Constants.TYPE_NAME});
        SearchContext.setCurrent(context);
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
                            throw new ElasticSearchParseException("Either specify doc or get, not both");
                        }

                        MapperService mapperService = documentIndexService.mapperService();
                        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(request.documentType());
                        doc = docMapper.parse(source(parser).type(request.documentType()).flyweight(true));
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    SearchParseElement element = hlElements.get(currentFieldName);
                    if (element == null) {
                        element = facetElements.get(currentFieldName);
                    }

                    if ("query".equals(currentFieldName)) {
                        if (context.percolateQuery() != null) {
                            throw new ElasticSearchParseException("Either specify query or filter, not both");
                        }
                        context.percolateQuery(documentIndexService.queryParserService().parse(parser).query());
                    } else if ("filter".equals(currentFieldName)) {
                        if (context.percolateQuery() != null) {
                            throw new ElasticSearchParseException("Either specify query or filter, not both");
                        }
                        Filter filter = documentIndexService.queryParserService().parseInnerFilter(parser).filter();
                        context.percolateQuery(new XConstantScoreQuery(filter));
                    } else if (element != null) {
                        element.parse(parser, context);
                    }
                } else if (token == null) {
                    break;
                } else if (token.isValue()) {
                    if ("size".equals(currentFieldName)) {
                        context.limit = true;
                        context.size = parser.intValue();
                        if (context.size < 0) {
                            throw new ElasticSearchParseException("size is set to [" + context.size + "] and is expected to be higher or equal to 0");
                        }
                    } else if ("sort".equals(currentFieldName)) {
                        context.sort = parser.booleanValue();
                    } else if ("score".equals(currentFieldName)) {
                        context.score = parser.booleanValue();
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
            throw new ElasticSearchParseException("failed to parse request", e);
        } finally {
            context.types(previousTypes);
            SearchContext.removeCurrent();
            if (parser != null) {
                parser.close();
            }
        }

        return doc;
    }

    private ParsedDocument parseFetchedDoc(BytesReference fetchedDoc, IndexService documentIndexService, String type) {
        ParsedDocument doc = null;
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(fetchedDoc).createParser(fetchedDoc);
            MapperService mapperService = documentIndexService.mapperService();
            DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(type);
            doc = docMapper.parse(source(parser).type(type).flyweight(true));
        } catch (Throwable e) {
            throw new ElasticSearchParseException("failed to parse request", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        if (doc == null) {
            throw new ElasticSearchParseException("No doc to percolate in the request");
        }

        return doc;
    }

    public void close() {
        cache.close();
    }

    interface PercolatorType {

        // 0x00 is reserved for empty type.
        byte id();

        ReduceResult reduce(List<PercolateShardResponse> shardResults);

        PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context);

    }

    private final PercolatorType countPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x01;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            long finalCount = 0;
            for (PercolateShardResponse shardResponse : shardResults) {
                finalCount += shardResponse.count();
            }

            assert !shardResults.isEmpty();
            if (shardResults.get(0).facets() != null) {
                InternalFacets reducedFacets = reduceFacets(shardResults);
                return new ReduceResult(finalCount, reducedFacets);
            } else {
                return new ReduceResult(finalCount);
            }
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            long count = 0;
            Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
            for (Map.Entry<HashedBytesRef, Query> entry : context.percolateQueries().entrySet()) {
                collector.reset();
                try {
                    context.docSearcher().search(entry.getValue(), collector);
                } catch (IOException e) {
                    logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                }

                if (collector.exists()) {
                    count++;
                }
            }
            return new PercolateShardResponse(count, context, request.index(), request.shardId());
        }

    };

    private final PercolatorType queryCountPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x02;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            return countPercolator.reduce(shardResults);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            long count = 0;
            Engine.Searcher percolatorSearcher = context.indexShard().acquireSearcher("percolate");
            try {
                Count countCollector = count(logger, context);
                queryBasedPercolating(percolatorSearcher, context, countCollector);
                count = countCollector.counter();
            } catch (Throwable e) {
                logger.warn("failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
            return new PercolateShardResponse(count, context, request.index(), request.shardId());
        }

    };

    private final PercolatorType matchPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x03;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            long foundMatches = 0;
            int numMatches = 0;
            for (PercolateShardResponse response : shardResults) {
                foundMatches += response.count();
                numMatches += response.matches().length;
            }
            int requestedSize = shardResults.get(0).requestedSize();

            // Use a custom impl of AbstractBigArray for Object[]?
            List<PercolateResponse.Match> finalMatches = new ArrayList<PercolateResponse.Match>(requestedSize == 0 ? numMatches : requestedSize);
            outer:
            for (PercolateShardResponse response : shardResults) {
                Text index = new StringText(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? NO_SCORE : response.scores()[i];
                    Text match = new BytesText(new BytesArray(response.matches()[i]));
                    Map<String, HighlightField> hl = response.hls().isEmpty() ? null : response.hls().get(i);
                    finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                    if (requestedSize != 0 && finalMatches.size() == requestedSize) {
                        break outer;
                    }
                }
            }

            assert !shardResults.isEmpty();
            if (shardResults.get(0).facets() != null) {
                InternalFacets reducedFacets = reduceFacets(shardResults);
                return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]), reducedFacets);
            } else {
                return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]));
            }
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            long count = 0;
            List<BytesRef> matches = new ArrayList<BytesRef>();
            List<Map<String, HighlightField>> hls = new ArrayList<Map<String, HighlightField>>();
            Lucene.ExistsCollector collector = new Lucene.ExistsCollector();

            for (Map.Entry<HashedBytesRef, Query> entry : context.percolateQueries().entrySet()) {
                collector.reset();
                if (context.highlight() != null) {
                    context.parsedQuery(new ParsedQuery(entry.getValue(), ImmutableMap.<String, Filter>of()));
                    context.hitContext().cache().clear();
                }
                try {
                    context.docSearcher().search(entry.getValue(), collector);
                } catch (Throwable e) {
                    logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                }

                if (collector.exists()) {
                    if (!context.limit || count < context.size) {
                        matches.add(entry.getKey().bytes);
                        if (context.highlight() != null) {
                            highlightPhase.hitExecute(context, context.hitContext());
                            hls.add(context.hitContext().hit().getHighlightFields());
                        }
                    }
                    count++;
                }
            }

            BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
            return new PercolateShardResponse(finalMatches, hls, count, context, request.index(), request.shardId());
        }
    };

    private final PercolatorType queryPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x04;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            return matchPercolator.reduce(shardResults);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            Engine.Searcher percolatorSearcher = context.indexShard().acquireSearcher("percolate");
            try {
                Match match = match(logger, context, highlightPhase);
                queryBasedPercolating(percolatorSearcher, context, match);
                List<BytesRef> matches = match.matches();
                List<Map<String, HighlightField>> hls = match.hls();
                long count = match.counter();

                BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
                return new PercolateShardResponse(finalMatches, hls, count, context, request.index(), request.shardId());
            } catch (Throwable e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard().shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
        }
    };

    private final PercolatorType scoringPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x05;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            return matchPercolator.reduce(shardResults);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            Engine.Searcher percolatorSearcher = context.indexShard().acquireSearcher("percolate");
            try {
                MatchAndScore matchAndScore = matchAndScore(logger, context, highlightPhase);
                queryBasedPercolating(percolatorSearcher, context, matchAndScore);
                List<BytesRef> matches = matchAndScore.matches();
                List<Map<String, HighlightField>> hls = matchAndScore.hls();
                float[] scores = matchAndScore.scores().toArray();
                long count = matchAndScore.counter();

                BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
                return new PercolateShardResponse(finalMatches, hls, count, scores, context, request.index(), request.shardId());
            } catch (Throwable e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard().shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
        }
    };

    private final PercolatorType topMatchingPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x06;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
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
            List<PercolateResponse.Match> finalMatches = new ArrayList<PercolateResponse.Match>(requestedSize);
            if (nonEmptyResponses == 1) {
                PercolateShardResponse response = shardResults.get(firstNonEmptyIndex);
                Text index = new StringText(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? Float.NaN : response.scores()[i];
                    Text match = new BytesText(new BytesArray(response.matches()[i]));
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
                    Text index = new StringText(shardResponse.getIndex());
                    Text match = new BytesText(new BytesArray(shardResponse.matches()[itemIndex]));
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
            if (shardResults.get(0).facets() != null) {
                InternalFacets reducedFacets = reduceFacets(shardResults);
                return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]), reducedFacets);
            } else {
                return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]));
            }
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            Engine.Searcher percolatorSearcher = context.indexShard().acquireSearcher("percolate");
            try {
                MatchAndSort matchAndSort = QueryCollector.matchAndSort(logger, context);
                queryBasedPercolating(percolatorSearcher, context, matchAndSort);
                TopDocs topDocs = matchAndSort.topDocs();
                long count = topDocs.totalHits;
                List<BytesRef> matches = new ArrayList<BytesRef>(topDocs.scoreDocs.length);
                float[] scores = new float[topDocs.scoreDocs.length];
                List<Map<String, HighlightField>> hls = null;
                if (context.highlight() != null) {
                    hls = new ArrayList<Map<String, HighlightField>>(topDocs.scoreDocs.length);
                }

                IndexFieldData idFieldData = context.fieldData().getForField(
                        new FieldMapper.Names(IdFieldMapper.NAME),
                        new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")),
                        false
                );
                int i = 0;
                final HashedBytesRef spare = new HashedBytesRef(new BytesRef());
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    int segmentIdx = ReaderUtil.subIndex(scoreDoc.doc, percolatorSearcher.reader().leaves());
                    AtomicReaderContext atomicReaderContext = percolatorSearcher.reader().leaves().get(segmentIdx);
                    BytesValues values = idFieldData.load(atomicReaderContext).getBytesValues(true);
                    final int localDocId = scoreDoc.doc - atomicReaderContext.docBase;
                    final int numValues = values.setDocument(localDocId);
                    assert numValues == 1;
                    spare.bytes = values.nextValue();
                    spare.hash = values.currentValueHash();
                    matches.add(values.copyShared());
                    if (hls != null) {
                        Query query = context.percolateQueries().get(spare);
                        context.parsedQuery(new ParsedQuery(query, ImmutableMap.<String, Filter>of()));
                        context.hitContext().cache().clear();
                        highlightPhase.hitExecute(context, context.hitContext());
                        hls.add(i, context.hitContext().hit().getHighlightFields());
                    }
                    scores[i++] = scoreDoc.score;
                }
                if (hls != null) {
                    return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), hls, count, scores, context, request.index(), request.shardId());
                } else {
                    return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), count, scores, context, request.index(), request.shardId());
                }
            } catch (Throwable e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard().shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
        }

    };

    private void queryBasedPercolating(Engine.Searcher percolatorSearcher, PercolateContext context, QueryCollector percolateCollector) throws IOException {
        Filter percolatorTypeFilter = context.indexService().mapperService().documentMapper(Constants.TYPE_NAME).typeFilter();
        percolatorTypeFilter = context.indexService().cache().filter().cache(percolatorTypeFilter);
        FilteredQuery query = new FilteredQuery(context.percolateQuery(), percolatorTypeFilter);
        percolatorSearcher.searcher().search(query, percolateCollector);

        for (Collector queryCollector : percolateCollector.facetCollectors) {
            if (queryCollector instanceof XCollector) {
                ((XCollector) queryCollector).postCollection();
            }
        }
        if (context.facets() != null) {
            facetPhase.execute(context);
        }
    }

    public final static class ReduceResult {

        private static PercolateResponse.Match[] EMPTY = new PercolateResponse.Match[0];

        private final long count;
        private final PercolateResponse.Match[] matches;
        private final InternalFacets reducedFacets;

        ReduceResult(long count, PercolateResponse.Match[] matches, InternalFacets reducedFacets) {
            this.count = count;
            this.matches = matches;
            this.reducedFacets = reducedFacets;
        }

        ReduceResult(long count, PercolateResponse.Match[] matches) {
            this.count = count;
            this.matches = matches;
            this.reducedFacets = null;
        }

        public ReduceResult(long count, InternalFacets reducedFacets) {
            this.count = count;
            this.matches = EMPTY;
            this.reducedFacets = reducedFacets;
        }

        public ReduceResult(long count) {
            this.count = count;
            this.matches = EMPTY;
            this.reducedFacets = null;
        }

        public long count() {
            return count;
        }

        public PercolateResponse.Match[] matches() {
            return matches;
        }

        public InternalFacets reducedFacets() {
            return reducedFacets;
        }
    }

    public static final class Constants {

        public static final String TYPE_NAME = "_percolator";

    }

    private InternalFacets reduceFacets(List<PercolateShardResponse> shardResults) {
        if (shardResults.size() == 1) {
            return shardResults.get(0).facets();
        }

        PercolateShardResponse firstShardResponse = shardResults.get(0);
        List<Facet> aggregatedFacets = Lists.newArrayList();
        List<Facet> namedFacets = Lists.newArrayList();
        for (Facet facet : firstShardResponse.facets()) {
            // aggregate each facet name into a single list, and aggregate it
            namedFacets.clear();
            for (PercolateShardResponse entry : shardResults) {
                for (Facet facet1 : entry.facets()) {
                    if (facet.getName().equals(facet1.getName())) {
                        namedFacets.add(facet1);
                    }
                }
            }
            if (!namedFacets.isEmpty()) {
                Facet aggregatedFacet = ((InternalFacet) namedFacets.get(0)).reduce(new InternalFacet.ReduceContext(cacheRecycler, namedFacets));
                aggregatedFacets.add(aggregatedFacet);
            }
        }
        return new InternalFacets(aggregatedFacets);
    }

}
