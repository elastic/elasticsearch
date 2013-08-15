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

import gnu.trove.map.hash.TByteObjectHashMap;
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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.stats.ShardPercolateService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.index.mapper.SourceToParse.source;
import static org.elasticsearch.percolator.QueryCollector.*;

/**
 */
public class PercolatorService extends AbstractComponent {

    public final static float NO_SCORE = Float.NEGATIVE_INFINITY;

    private final CloseableThreadLocal<MemoryIndex> cache;
    private final IndicesService indicesService;
    private final TByteObjectHashMap<PercolatorType> percolatorTypes;

    @Inject
    public PercolatorService(Settings settings, IndicesService indicesService) {
        super(settings);
        this.indicesService = indicesService;
        final long maxReuseBytes = settings.getAsBytesSize("indices.memory.memory_index.size_per_thread", new ByteSizeValue(1, ByteSizeUnit.MB)).bytes();
        cache = new CloseableThreadLocal<MemoryIndex>() {
            @Override
            protected MemoryIndex initialValue() {
                return new ExtendedMemoryIndex(false, maxReuseBytes);
            }
        };

        percolatorTypes = new TByteObjectHashMap<PercolatorType>(6);
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

        try {
            final PercolateContext context = new PercolateContext();
            context.percolateQueries = indexShard.percolateRegistry().percolateQueries();
            context.indexShard = indexShard;
            context.percolateIndexService = percolateIndexService;
            ParsedDocument parsedDocument = parsePercolate(percolateIndexService, request, context);
            if (context.percolateQueries.isEmpty()) {
                return new PercolateShardResponse(context, request.index(), request.shardId());
            }

            if (request.docSource() != null && request.docSource().length() != 0) {
                parsedDocument = parseFetchedDoc(request.docSource(), percolateIndexService, request.documentType());
            } else if (parsedDocument == null) {
                throw new ElasticSearchIllegalArgumentException("Nothing to percolate");
            }

            if (context.query == null && (context.score || context.sort)) {
                throw new ElasticSearchIllegalArgumentException("Can't sort or score if no query is specified");
            }

            if (context.size < 0) {
                context.size = 0;
            }

            // first, parse the source doc into a MemoryIndex
            final MemoryIndex memoryIndex = cache.get();
            try {
                // TODO: This means percolation does not support nested docs...
                // So look into: ByteBufferDirectory
                for (IndexableField field : parsedDocument.rootDoc().getFields()) {
                    if (!field.fieldType().indexed()) {
                        continue;
                    }
                    // no need to index the UID field
                    if (field.name().equals(UidFieldMapper.NAME)) {
                        continue;
                    }
                    TokenStream tokenStream;
                    try {
                        tokenStream = field.tokenStream(parsedDocument.analyzer());
                        if (tokenStream != null) {
                            memoryIndex.addField(field.name(), tokenStream, field.boost());
                        }
                    } catch (IOException e) {
                        throw new ElasticSearchException("Failed to create token stream", e);
                    }
                }

                PercolatorType action;
                if (request.onlyCount()) {
                    action = context.query != null ? queryCountPercolator : countPercolator;
                } else {
                    if (context.sort) {
                        action = topMatchingPercolator;
                    } else if (context.query != null) {
                        action = context.score ? scoringPercolator : queryPercolator;
                    } else {
                        action = matchPercolator;
                    }
                }
                context.percolatorTypeId = action.id();

                context.docSearcher = memoryIndex.createSearcher();
                context.fieldData = percolateIndexService.fieldData();
                IndexCache indexCache = percolateIndexService.cache();
                try {
                    return action.doPercolate(request, context);
                } finally {
                    // explicitly clear the reader, since we can only register on callback on SegmentReader
                    indexCache.clear(context.docSearcher.getIndexReader());
                    context.fieldData.clear(context.docSearcher.getIndexReader());
                }
            } finally {
                memoryIndex.reset();
            }
        } finally {
            shardPercolateService.postPercolate(System.nanoTime() - startTime);
        }
    }

    private ParsedDocument parsePercolate(IndexService documentIndexService, PercolateShardRequest request, PercolateContext context) throws ElasticSearchException {
        BytesReference source = request.source();
        if (source == null || source.length() == 0) {
            return null;
        }

        ParsedDocument doc = null;
        XContentParser parser = null;

        // Some queries (function_score query when for decay functions) rely on SearchContext being set:
        SearchContext.setCurrent(new SearchContext(0,
                new ShardSearchRequest().types(new String[0]),
                null, context.indexShard.searcher(), context.percolateIndexService, context.indexShard,
                null, null));
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
                    if ("query".equals(currentFieldName)) {
                        if (context.query != null) {
                            throw new ElasticSearchParseException("Either specify query or filter, not both");
                        }
                        context.query = documentIndexService.queryParserService().parse(parser).query();
                    } else if ("filter".equals(currentFieldName)) {
                        if (context.query != null) {
                            throw new ElasticSearchParseException("Either specify query or filter, not both");
                        }
                        Filter filter = documentIndexService.queryParserService().parseInnerFilter(parser).filter();
                        context.query = new XConstantScoreQuery(filter);
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
        } catch (IOException e) {
            throw new ElasticSearchParseException("failed to parse request", e);
        } finally {
            SearchContext.current().release();
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
        } catch (IOException e) {
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
            return new ReduceResult(finalCount);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            long count = 0;
            Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
            for (Map.Entry<HashedBytesRef, Query> entry : context.percolateQueries.entrySet()) {
                collector.reset();
                try {
                    context.docSearcher.search(entry.getValue(), collector);
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
            Engine.Searcher percolatorSearcher = context.indexShard.searcher();
            try {
                Count countCollector = count(logger, context);
                queryBasedPercolating(percolatorSearcher, context, countCollector);
                count = countCollector.counter();
            } catch (IOException e) {
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
            outer: for (PercolateShardResponse response : shardResults) {
                Text index = new StringText(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? NO_SCORE : response.scores()[i];
                    Text match = new BytesText(new BytesArray(response.matches()[i]));
                    finalMatches.add(new PercolateResponse.Match(index, match, score));
                    if (requestedSize != 0 && finalMatches.size() == requestedSize) {
                        break outer;
                    }
                }
            }
            return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]));
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            long count = 0;
            List<BytesRef> matches = new ArrayList<BytesRef>();
            Lucene.ExistsCollector collector = new Lucene.ExistsCollector();

            for (Map.Entry<HashedBytesRef, Query> entry : context.percolateQueries.entrySet()) {
                collector.reset();
                try {
                    context.docSearcher.search(entry.getValue(), collector);
                } catch (IOException e) {
                    logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                }

                if (collector.exists()) {
                    if (!context.limit || count < context.size) {
                        matches.add(entry.getKey().bytes);
                    }
                    count++;
                }
            }
            return new PercolateShardResponse(matches.toArray(new BytesRef[0]), count, context, request.index(), request.shardId());
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
            Engine.Searcher percolatorSearcher = context.indexShard.searcher();
            try {
                Match match = match(logger, context);
                queryBasedPercolating(percolatorSearcher, context, match);
                List<BytesRef> matches = match.matches();
                long count = match.counter();
                return new PercolateShardResponse(matches.toArray(new BytesRef[0]), count, context, request.index(), request.shardId());
            } catch (IOException e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard.shardId(), "failed to execute", e);
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
            Engine.Searcher percolatorSearcher = context.indexShard.searcher();
            try {
                MatchAndScore matchAndScore = matchAndScore(logger, context);
                queryBasedPercolating(percolatorSearcher, context, matchAndScore);
                BytesRef[] matches = matchAndScore.matches().toArray(new BytesRef[0]);
                float[] scores = matchAndScore.scores().toArray();
                long count = matchAndScore.counter();
                return new PercolateShardResponse(matches, count, scores, context, request.index(), request.shardId());
            } catch (IOException e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard.shardId(), "failed to execute", e);
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
            for (PercolateShardResponse response : shardResults) {
                foundMatches += response.count();
            }
            int requestedSize = shardResults.get(0).requestedSize();

            // Use a custom impl of AbstractBigArray for Object[]?
            List<PercolateResponse.Match> finalMatches = new ArrayList<PercolateResponse.Match>(requestedSize);
            if (shardResults.size() == 1) {
                PercolateShardResponse response = shardResults.get(0);
                Text index = new StringText(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? Float.NaN : response.scores()[i];
                    Text match = new BytesText(new BytesArray(response.matches()[i]));
                    finalMatches.add(new PercolateResponse.Match(index, match, score));
                }
            } else {
                int[] slots = new int[shardResults.size()];
                while (true) {
                    float lowestScore = Float.NEGATIVE_INFINITY;
                    int requestIndex = 0;
                    int itemIndex = 0;
                    for (int i = 0; i < shardResults.size(); i++) {
                        int scoreIndex = slots[i];
                        float[] scores = shardResults.get(i).scores();
                        if (scoreIndex >= scores.length) {
                            continue;
                        }

                        float score = scores[scoreIndex];
                        int cmp = Float.compare(lowestScore, score);
                        if (cmp < 0) {
                            requestIndex = i;
                            itemIndex = scoreIndex;
                            lowestScore = score;
                        }
                    }
                    slots[requestIndex]++;

                    PercolateShardResponse shardResponse = shardResults.get(requestIndex);
                    Text index = new StringText(shardResponse.getIndex());
                    Text match = new BytesText(new BytesArray(shardResponse.matches()[itemIndex]));
                    float score = shardResponse.scores()[itemIndex];
                    finalMatches.add(new PercolateResponse.Match(index, match, score));
                    if (finalMatches.size() == requestedSize) {
                        break;
                    }
                }
            }
            return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]));
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            Engine.Searcher percolatorSearcher = context.indexShard.searcher();
            try {
                MatchAndSort matchAndSort = QueryCollector.matchAndSort(logger, context);
                queryBasedPercolating(percolatorSearcher, context, matchAndSort);
                TopDocs topDocs = matchAndSort.topDocs();
                long count = topDocs.totalHits;
                List<BytesRef> matches = new ArrayList<BytesRef>(topDocs.scoreDocs.length);
                float[] scores = new float[topDocs.scoreDocs.length];

                IndexFieldData idFieldData = context.fieldData.getForField(
                        new FieldMapper.Names(IdFieldMapper.NAME),
                        new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes"))
                );
                int i = 0;
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    int segmentIdx = ReaderUtil.subIndex(scoreDoc.doc, percolatorSearcher.reader().leaves());
                    AtomicReaderContext atomicReaderContext = percolatorSearcher.reader().leaves().get(segmentIdx);
                    BytesValues values = idFieldData.load(atomicReaderContext).getBytesValues();
                    BytesRef id = values.getValue(scoreDoc.doc - atomicReaderContext.docBase);
                    matches.add(values.makeSafe(id));
                    scores[i++] = scoreDoc.score;
                }
                return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), count, scores, context, request.index(), request.shardId());
            } catch (Exception e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard.shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
        }

    };

    private static void queryBasedPercolating(Engine.Searcher percolatorSearcher, PercolateContext context, Collector collector) throws IOException {
        Filter percolatorTypeFilter = context.percolateIndexService.mapperService().documentMapper(Constants.TYPE_NAME).typeFilter();
        percolatorTypeFilter = context.percolateIndexService.cache().filter().cache(percolatorTypeFilter);
        FilteredQuery query = new FilteredQuery(context.query, percolatorTypeFilter);
        percolatorSearcher.searcher().search(query, collector);
    }

    public class PercolateContext {

        public boolean limit;
        public int size;
        public boolean score;
        public boolean sort;
        public byte percolatorTypeId;

        Query query;
        ConcurrentMap<HashedBytesRef, Query> percolateQueries;
        IndexSearcher docSearcher;
        IndexShard indexShard;
        IndexFieldDataService fieldData;
        IndexService percolateIndexService;

    }

    public final static class ReduceResult {

        private final long count;
        private final PercolateResponse.Match[] matches;

        ReduceResult(long count, PercolateResponse.Match[] matches) {
            this.count = count;
            this.matches = matches;
        }

        public ReduceResult(long count) {
            this.count = count;
            this.matches = new PercolateResponse.Match[0];
        }

        public long count() {
            return count;
        }

        public PercolateResponse.Match[] matches() {
            return matches;
        }
    }

    public static final class Constants {

        public static final String TYPE_NAME = "_percolator";

    }

}
