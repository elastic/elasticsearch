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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
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
import org.elasticsearch.search.highlight.*;
import org.elasticsearch.search.internal.InternalSearchHit;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.SourceToParse.source;
import static org.elasticsearch.index.percolator.PercolatorService.*;

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

        @Nullable
        private final SearchContextHighlight searchContextHighlight;

        public DocAndQueryRequest(ParsedDocument doc, @Nullable Query query, @Nullable SearchContextHighlight searchContextHighlight) {
            this.doc = doc;
            this.query = query;
            this.searchContextHighlight = searchContextHighlight;
        }

        public DocAndQueryRequest(ParsedDocument doc, @Nullable Query query) {
            this(doc, query,  null);
        }

        public ParsedDocument doc() {
            return this.doc;
        }

        @Nullable
        Query query() {
            return this.query;
        }

        @Nullable
        public SearchContextHighlight getSearchContextHighlight() {
            return searchContextHighlight;
        }
    }

    public static final class PercolationMatch implements Streamable {
        private String match;
        private Map<String, HighlightField> highlightFields;

        public PercolationMatch(String match) {
            this(match, null);
        }

        public PercolationMatch(String match, @Nullable Map<String, HighlightField> highlightFields) {
            if (match == null)
                throw new IllegalArgumentException("match cannot be null");

            this.match = match;
            this.highlightFields = highlightFields;
        }

        protected PercolationMatch() {
        }

        public Map<String, HighlightField> getHighlightFields() {
            return highlightFields;
        }

        public String getMatch() {
            return match;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            match = in.readString();
            int count = in.readVInt();
            if (count > 0) {
                highlightFields = new HashMap<String, HighlightField>(count);
                for (int j = 0; j < count; j++){
                    highlightFields.put(in.readString(), HighlightField.readHighlightField(in));
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(match);
            if (highlightFields == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(highlightFields.size());
                for (Map.Entry<String, HighlightField> entry : highlightFields.entrySet()) {
                    out.writeString(entry.getKey());
                    entry.getValue().writeTo(out);
                }
            }
        }

        public static PercolationMatch readPercolationMatch(StreamInput in) throws IOException {
            PercolationMatch match = new PercolationMatch();
            match.readFrom(in);
            return match;
        }

        @Override
        public boolean equals(Object o){
            if (o instanceof PercolationMatch){
                PercolationMatch other = (PercolationMatch)o;
                if (!this.match.equals(other.match)) return false;
                if (other.highlightFields == null || this.highlightFields == null) {
                    return other.highlightFields == this.highlightFields;
                }
                return this.highlightFields.equals(other.highlightFields);
            }
            return false;
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

    private final Map<String, QueryAndHighlightContext> queries = ConcurrentCollections.newConcurrentMap();


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
        addQuery(name, queryBuilder, null);
    }

    public void addQuery(String name, QueryBuilder queryBuilder, HighlightBuilder highlightBuilder) throws ElasticSearchException {
        try {
            XContentBuilder builder = XContentFactory.smileBuilder()
                    .startObject()
                    .field("query", queryBuilder)
                    ;

            if (highlightBuilder != null)
                highlightBuilder.toXContent(builder, HighlightBuilder.EMPTY_PARAMS);

            builder.endObject();

            addQuery(name, builder.bytes());
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to add query [" + name + "]", e);
        }
    }

    public void addQuery(String name, BytesReference source) throws ElasticSearchException {
        addQuery(name, parseQuery(name, source));
    }

    public PercolatorService.QueryAndHighlightContext parseQuery(String name, BytesReference source) throws ElasticSearchException {
        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(source);
            Query query = null;
            SearchContextHighlight searchContextHighlight = null;
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
                    } else if ("highlight".equals(currentFieldName)) {
                        searchContextHighlight = HighlighterParseElement.parseImpl(parser);
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
            return new PercolatorService.QueryAndHighlightContext(query, searchContextHighlight);
        } catch (Exception e) {
            throw new ElasticSearchException("failed to parse query [" + name + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private void addQuery(String name, QueryAndHighlightContext queryAndHighlightContext) {
        Preconditions.checkArgument(queryAndHighlightContext.getQuery() != null, "query must be provided for percolate request");
        this.queries.put(name, queryAndHighlightContext);
    }

    public void removeQuery(String name) {
        this.queries.remove(name);
    }

    public void addQueries(Map<String, QueryAndHighlightContext> queries) {
        this.queries.putAll(queries);
    }

    public Response percolate(final SourceRequest request) throws ElasticSearchException {
        Query query = null;
        SearchContextHighlight searchContextHighlight = null;
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
                    } else if ("highlight".equals(currentFieldName)) {
                        try {
                            searchContextHighlight = HighlighterParseElement.parseImpl(parser);
                        } catch (Exception e) {
                            // TODO log??
                        }
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

        return percolate(new DocAndQueryRequest(doc, query, searchContextHighlight));
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
        final List<PercolationMatch> matches = new ArrayList<PercolationMatch>();
        final ParsedDocument parsedDocument = request.doc();

        try {
            if (request.query() == null) {
                FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();

                Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
                for (Map.Entry<String, QueryAndHighlightContext> entry : queries.entrySet()) {
                    collector.reset();
                    try {
                        searcher.search(entry.getValue().getQuery(), collector);
                    } catch (IOException e) {
                        logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                    }

                    if (collector.exists()) {
                        if (entry.getValue().getHighlightContext() == null) {
                            matches.add(new PercolationMatch(entry.getKey()));
                        } else {
                            // TODO: we are assuming there is only one document in the index, whose docid is 0
                            InternalSearchHit searchHit = new InternalSearchHit(0/* scoreDoc.doc */, null, parsedDocument.type(), null, null);
                            hitContext.reset(searchHit, searcher.getIndexReader(), 0 /* scoreDoc.doc */, searcher.getIndexReader(), 0 /* scoreDoc.doc */, null);
                            Map<String, HighlightField> highlightFields =
                                    HighlightPhase.doHighlighting(null, entry.getValue().getHighlightContext(), entry.getValue().getQuery(),
                                            mapperService, hitContext, true /* TODO */, parsedDocument);

                            matches.add(new PercolationMatch(entry.getKey(), highlightFields));
                        }
                    }
                }
            } else {
                IndexService percolatorIndex = percolatorIndexServiceSafe();
                if (percolatorIndex.numberOfShards() == 0) {
                    throw new PercolateIndexUnavailable(new Index(INDEX_NAME));
                }
                IndexShard percolatorShard = percolatorIndex.shard(0);
                Engine.Searcher percolatorSearcher = percolatorShard.searcher();
                try {
                    percolatorSearcher.searcher().search(request.query(), new QueryCollector(logger, queries, searcher, percolatorIndex, mapperService, matches, parsedDocument));
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
        IndexService indexService = indicesService.indexService(INDEX_NAME);
        if (indexService == null) {
            throw new PercolateIndexUnavailable(new Index(INDEX_NAME));
        }
        return indexService;
    }

    static class QueryCollector extends Collector {
        private final IndexSearcher searcher;
        private final IndexService percolatorIndex;
        private final MapperService mapperService;
        private final List<PercolationMatch> matches;
        private final ParsedDocument parsedDocument;
        private final Map<String, QueryAndHighlightContext> queries;
        private final ESLogger logger;
        private final FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();

        private final Lucene.ExistsCollector collector = new Lucene.ExistsCollector();

        private FieldData fieldData;

        QueryCollector(ESLogger logger, Map<String, QueryAndHighlightContext> queries, IndexSearcher searcher, IndexService percolatorIndex,
                       MapperService mapperService, List<PercolationMatch> matches, ParsedDocument parsedDocument) {
            this.logger = logger;
            this.queries = queries;
            this.searcher = searcher;
            this.percolatorIndex = percolatorIndex;
            this.mapperService = mapperService;
            this.matches = matches;
            this.parsedDocument = parsedDocument;
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
            QueryAndHighlightContext queryAndHighlightContext = queries.get(id);
            if (queryAndHighlightContext == null || queryAndHighlightContext.getQuery() == null) {
                // log???
                return;
            }

            Query query = queryAndHighlightContext.getQuery();

            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    if (queryAndHighlightContext.getHighlightContext() == null) {
                        matches.add(new PercolationMatch(id));
                    } else {
                        // TODO: we are assuming there is only one document in the index, whose docid is 0
                        InternalSearchHit searchHit = new InternalSearchHit(0/* scoreDoc.doc */, null, parsedDocument.type(), null, null);
                        hitContext.reset(searchHit, searcher.getIndexReader(), 0 /* scoreDoc.doc */, searcher.getIndexReader(), 0 /* scoreDoc.doc */, null);
                        Map<String, HighlightField> highlightFields =
                                HighlightPhase.doHighlighting(null, queryAndHighlightContext.getHighlightContext(), query,
                                        mapperService, hitContext, true /* TODO */, parsedDocument);

                        matches.add(new PercolationMatch(id, highlightFields));
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
