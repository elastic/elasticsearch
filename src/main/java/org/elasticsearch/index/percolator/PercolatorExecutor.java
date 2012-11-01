/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.percolator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.memory.CustomMemoryIndex;
import org.apache.lucene.search.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.InternalSearchHit;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.SourceToParse.source;

/**
 *
 */
public class PercolatorExecutor extends AbstractIndexComponent {

    public static class SourceRequest {
        private final String type;
        private final BytesReference source;

        public SourceRequest(String type, BytesReference source) {
            this.type = type;
            this.source = source;
        }

        public String type() {
            return this.type;
        }

        public BytesReference source() {
            return source;
        }
    }

    public static class DocAndSourceQueryRequest {
        private final ParsedDocument doc;
        @Nullable
        private final String query;

        public DocAndSourceQueryRequest(ParsedDocument doc, @Nullable String query) {
            this.doc = doc;
            this.query = query;
        }

        public ParsedDocument doc() {
            return this.doc;
        }

        @Nullable
        String query() {
            return this.query;
        }
    }


    public static class DocAndQueryRequest {
        private final ParsedDocument doc;
        @Nullable
        private final Query query;

        public DocAndQueryRequest(ParsedDocument doc, @Nullable Query query) {
            this.doc = doc;
            this.query = query;
        }

        public ParsedDocument doc() {
            return this.doc;
        }

        @Nullable
        Query query() {
            return this.query;
        }
    }

    public static final class PercolationMatch {
        private final String match;
        private final Map<String, HighlightField> highlightFields;

        public PercolationMatch(String match) {
            this(match,  null);
        }

        public PercolationMatch(String match, @Nullable Map<String, HighlightField> highlightFields) {
            this.match = match;
            this.highlightFields = highlightFields;
        }
    }

    public static final class Response {
        private final List<PercolationMatch> matches;
        private final boolean mappersAdded;

        public Response(List<PercolationMatch> matches, boolean mappersAdded) {
            this.matches = matches;
            this.mappersAdded = mappersAdded;
        }

        public boolean mappersAdded() {
            return this.mappersAdded;
        }

        public List<PercolationMatch> matches() {
            return matches;
        }
    }

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private final IndexCache indexCache;

    private final Map<String, Query> queries = ConcurrentCollections.newConcurrentMap();


    private IndicesService indicesService;

    @Inject
    public PercolatorExecutor(Index index, @IndexSettings Settings indexSettings,
                              MapperService mapperService, IndexQueryParserService queryParserService,
                              IndexCache indexCache) {
        super(index, indexSettings);
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.indexCache = indexCache;
    }

    public void setIndicesService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    public void close() {
        this.queries.clear();
    }

    public void addQuery(String name, QueryBuilder queryBuilder) throws ElasticSearchException {
        try {
            XContentBuilder builder = XContentFactory.smileBuilder()
                    .startObject().field("query", queryBuilder).endObject();
            addQuery(name, builder.bytes());
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to add query [" + name + "]", e);
        }
    }

    public void addQuery(String name, BytesReference source) throws ElasticSearchException {
        addQuery(name, parseQuery(name, source));
    }

    public Query parseQuery(String name, BytesReference source) throws ElasticSearchException {
        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(source);
            Query query = null;
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken(); // move the START_OBJECT
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticSearchException("failed to parse query [" + name + "], not starting with OBJECT");
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        query = queryParserService.parse(parser).query();
                        break;
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
            return query;
        } catch (Exception e) {
            throw new ElasticSearchException("failed to parse query [" + name + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private void addQuery(String name, Query query) {
        Preconditions.checkArgument(query != null, "query must be provided for percolate request");
        this.queries.put(name, query);
    }

    public void removeQuery(String name) {
        this.queries.remove(name);
    }

    public void addQueries(Map<String, Query> queries) {
        this.queries.putAll(queries);
    }

    public Response percolate(final SourceRequest request) throws ElasticSearchException {
        Query query = null;
        ParsedDocument doc = null;
        XContentParser parser = null;
        try {

            parser = XContentFactory.xContent(request.source()).createParser(request.source());
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    // we need to check the "doc" here, so the next token will be START_OBJECT which is
                    // the actual document starting
                    if ("doc".equals(currentFieldName)) {
                        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(request.type());
                        doc = docMapper.parse(source(parser).type(request.type()).flyweight(true));
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        query = percolatorIndexServiceSafe().queryParserService().parse(parser).query();
                    }
                } else if (token == null) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new PercolatorException(index, "failed to parse request", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        if (doc == null) {
            throw new PercolatorException(index, "No doc to percolate in the request");
        }

        return percolate(new DocAndQueryRequest(doc, query));
    }

    public Response percolate(DocAndSourceQueryRequest request) throws ElasticSearchException {
        Query query = null;
        if (Strings.hasLength(request.query()) && !request.query().equals("*")) {
            query = percolatorIndexServiceSafe().queryParserService().parse(QueryBuilders.queryString(request.query())).query();
        }
        return percolate(new DocAndQueryRequest(request.doc(), query));
    }

    private Response percolate(DocAndQueryRequest request) throws ElasticSearchException {
        // first, parse the source doc into a MemoryIndex
        final CustomMemoryIndex memoryIndex = new CustomMemoryIndex();

        // TODO: This means percolation does not support nested docs...
        for (Fieldable field : request.doc().rootDoc().getFields()) {
            if (!field.isIndexed()) {
                continue;
            }
            // no need to index the UID field
            if (field.name().equals(UidFieldMapper.NAME)) {
                continue;
            }
            TokenStream tokenStream = field.tokenStreamValue();
            if (tokenStream != null) {
                memoryIndex.addField(field.name(), tokenStream, field.getBoost());
            } else {
                Reader reader = field.readerValue();
                if (reader != null) {
                    try {
                        memoryIndex.addField(field.name(), request.doc().analyzer().reusableTokenStream(field.name(), reader), field.getBoost() * request.doc().rootDoc().getBoost());
                    } catch (IOException e) {
                        throw new MapperParsingException("Failed to analyze field [" + field.name() + "]", e);
                    }
                } else {
                    String value = field.stringValue();
                    if (value != null) {
                        try {
                            memoryIndex.addField(field.name(), request.doc().analyzer().reusableTokenStream(field.name(), new FastStringReader(value)), field.getBoost() * request.doc().rootDoc().getBoost());
                        } catch (IOException e) {
                            throw new MapperParsingException("Failed to analyze field [" + field.name() + "]", e);
                        }
                    }
                }
            }
        }

        final IndexSearcher searcher = memoryIndex.createSearcher();
        List<PercolationMatch> matches = new ArrayList<PercolationMatch>();

        try {
            if (request.query() == null) {
                final boolean doHighlight = false;
                if (doHighlight) {
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
                    for (Map.Entry<String, Query> entry : queries.entrySet()) {
                        TopDocs hits = null;
                        try {
                            hits = searcher.search(entry.getValue(), 100);
                        } catch (IOException e) {
                            logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                        }

                        for (ScoreDoc scoreDoc : hits.scoreDocs) {
                            InternalSearchHit searchHit = new InternalSearchHit(scoreDoc.doc, uid.id(), uid.type(), sourceRequested ? source : null, null);
                            hitContext.reset(searchHit, searcher.getIndexReader(), scoreDoc.doc, searcher.getIndexReader(), scoreDoc.doc, null);
                            HighlightPhase.doHighlighting(null, null, entry.getValue(), mapperService, hitContext, false);
                        }
                    }
                } else {

                Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
                for (Map.Entry<String, Query> entry : queries.entrySet()) {
                    collector.reset();
                    try {
                        searcher.search(entry.getValue(), collector);
                    } catch (IOException e) {
                        logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                    }

                    if (collector.exists()) {
                        matches.add(new PercolationMatch(entry.getKey()));
                    }
                }
                }
            } else {
                IndexService percolatorIndex = percolatorIndexServiceSafe();
                if (percolatorIndex.numberOfShards() == 0) {
                    throw new PercolateIndexUnavailable(new Index(PercolatorService.INDEX_NAME));
                }
                IndexShard percolatorShard = percolatorIndex.shard(0);
                Engine.Searcher percolatorSearcher = percolatorShard.searcher();
                try {
                    percolatorSearcher.searcher().search(request.query(), new QueryCollector(logger, queries, searcher, percolatorIndex, mapperService, matches));
                } catch (IOException e) {
                    logger.warn("failed to execute", e);
                } finally {
                    percolatorSearcher.release();
                }
            }
        } finally {
            // explicitly clear the reader, since we can only register on callback on SegmentReader
            indexCache.clear(searcher.getIndexReader());
        }

        return new Response(matches, request.doc().mappingsModified());
    }

    private IndexService percolatorIndexServiceSafe() {
        IndexService indexService = indicesService.indexService(PercolatorService.INDEX_NAME);
        if (indexService == null) {
            throw new PercolateIndexUnavailable(new Index(PercolatorService.INDEX_NAME));
        }
        return indexService;
    }

    static class QueryCollector extends Collector {
        private final IndexSearcher searcher;
        private final IndexService percolatorIndex;
        private final MapperService mapperService;
        private final List<PercolationMatch> matches;
        private final Map<String, Query> queries;
        private final ESLogger logger;
        private final boolean doHighlight = false;

        private final Lucene.ExistsCollector collector = new Lucene.ExistsCollector();

        private FieldData fieldData;

        QueryCollector(ESLogger logger, Map<String, Query> queries, IndexSearcher searcher, IndexService percolatorIndex, MapperService mapperService, List<PercolationMatch> matches) {
            this.logger = logger;
            this.queries = queries;
            this.searcher = searcher;
            this.percolatorIndex = percolatorIndex;
            this.mapperService = mapperService;
            this.matches = matches;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
        }

        @Override
        public void collect(int doc) throws IOException {
            String uid = fieldData.stringValue(doc);
            if (uid == null) {
                return;
            }
            String id = Uid.idFromUid(uid);
            Query query = queries.get(id);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                if (doHighlight) {
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
                    TopDocs hits = searcher.search(query, 100);
                    for (ScoreDoc scoreDoc : hits.scoreDocs) {
                        InternalSearchHit searchHit = new InternalSearchHit(scoreDoc.doc, uid.id(), uid.type(), sourceRequested ? source : null, null);
                        hitContext.reset(searchHit, searcher.getIndexReader(), scoreDoc.doc, searcher.getIndexReader(), scoreDoc.doc, null);
                        HighlightPhase.doHighlighting(null, null, query, mapperService, null, false);
                    }
                } else {
                    collector.reset();
                    searcher.search(query, collector);
                    if (collector.exists()) {
                        matches.add(new PercolationMatch(id)); // TODO
                    }
                }
            } catch (IOException e) {
                logger.warn("[" + id + "] failed to execute query", e);
            }
        }

        @Override
        public void setNextReader(IndexReader reader, int docBase) throws IOException {
            // we use the UID because id might not be indexed
            fieldData = percolatorIndex.cache().fieldData().cache(FieldDataType.DefaultTypes.STRING, reader, UidFieldMapper.NAME);
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }
    }
}
