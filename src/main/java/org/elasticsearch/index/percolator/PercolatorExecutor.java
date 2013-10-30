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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.index.memory.ReusableMemoryIndex;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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

    public static final class Response {
        private final List<String> matches;
        private final boolean mappersAdded;

        public Response(List<String> matches, boolean mappersAdded) {
            this.matches = matches;
            this.mappersAdded = mappersAdded;
        }

        public boolean mappersAdded() {
            return this.mappersAdded;
        }

        public List<String> matches() {
            return matches;
        }
    }

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private final IndexCache indexCache;
    private final IndexFieldDataService fieldDataService;

    private final Map<String, Query> queries = ConcurrentCollections.newConcurrentMap();

    /**
     * Realtime index setting to control the number of MemoryIndex instances used to handle
     * Percolate requests. The default is <tt>10</tt>
     */
    public static final String PERCOLATE_POOL_SIZE = "index.percolate.pool.size";

    /**
     * Realtime index setting to control the upper memory reuse limit across all {@link MemoryIndex} instances
     * pooled to handle Percolate requests. This is NOT a peak upper bound, percolate requests can use more memory than this upper
     * bound. Yet, if all pooled {@link MemoryIndex} instances are returned to the pool this marks the upper memory bound use
     * buy this idle instances. If more memory was allocated by a {@link MemoryIndex} the additinal memory is freed before it
     * returns to the pool. The default is <tt>1 MB</tt>
     */
    public static final String PERCOLATE_POOL_MAX_MEMORY = "index.percolate.pool.reuse_memory_size";

    /**
     * Realtime index setting to control the timeout or the maximum waiting time
     * for an pooled memory index until an extra memory index is created. The default is <tt>100 ms</tt>
     */
    public static final String PERCOLATE_TIMEOUT = "index.percolate.pool.timeout";

    /**
     * Simple {@link MemoryIndex} Pool that reuses MemoryIndex instance across threads and allows each of the
     * MemoryIndex instance to reuse its internal memory based on a user configured realtime value.
     */
    static final class MemoryIndexPool {
        private volatile BlockingQueue<ReusableMemoryIndex> memoryIndexQueue;

        // used to track the in-flight memoryIdx instances so we don't overallocate
        private int poolMaxSize;
        private int poolCurrentSize;
        private volatile long bytesPerMemoryIndex;
        private ByteSizeValue maxMemorySize; // only accessed in sync block
        private volatile TimeValue timeout;

        public MemoryIndexPool(Settings settings) {
            poolMaxSize = settings.getAsInt(PERCOLATE_POOL_SIZE, 10);
            if (poolMaxSize <= 0) {
                throw new ElasticSearchIllegalArgumentException(PERCOLATE_POOL_SIZE + " size must be > 0 but was [" + poolMaxSize + "]");
            }
            memoryIndexQueue = new ArrayBlockingQueue<ReusableMemoryIndex>(poolMaxSize);
            maxMemorySize = settings.getAsBytesSize(PERCOLATE_POOL_MAX_MEMORY, new ByteSizeValue(1, ByteSizeUnit.MB));
            if (maxMemorySize.bytes() < 0) {
                throw new ElasticSearchIllegalArgumentException(PERCOLATE_POOL_MAX_MEMORY + " must be positive but was [" + maxMemorySize.bytes() + "]");
            }
            timeout = settings.getAsTime(PERCOLATE_TIMEOUT, new TimeValue(100));
            if (timeout.millis() < 0) {
                throw new ElasticSearchIllegalArgumentException(PERCOLATE_TIMEOUT + " must be positive but was [" + timeout + "]");
            }
            bytesPerMemoryIndex = maxMemorySize.bytes() / poolMaxSize;
        }

        public synchronized void updateSettings(Settings settings) {

            final int newPoolSize = settings.getAsInt(PERCOLATE_POOL_SIZE, poolMaxSize);
            if (newPoolSize <= 0) {
                throw new ElasticSearchIllegalArgumentException(PERCOLATE_POOL_SIZE + " size must be > 0 but was [" + newPoolSize + "]");
            }
            final ByteSizeValue byteSize = settings.getAsBytesSize(PERCOLATE_POOL_MAX_MEMORY, maxMemorySize);
            if (byteSize.bytes() < 0) {
                throw new ElasticSearchIllegalArgumentException(PERCOLATE_POOL_MAX_MEMORY + " must be positive but was [" + byteSize.bytes() + "]");
            }
            timeout = settings.getAsTime(PERCOLATE_TIMEOUT, timeout); // always set this!
            if (timeout.millis() < 0) {
                throw new ElasticSearchIllegalArgumentException(PERCOLATE_TIMEOUT + " must be positive but was [" + timeout + "]");
            }
            if (maxMemorySize.equals(byteSize) && newPoolSize == poolMaxSize) {
                // nothing changed - return
                return;
            }
            maxMemorySize = byteSize;
            poolMaxSize = newPoolSize;
            poolCurrentSize = Integer.MAX_VALUE; // prevent new creations until we have the new index in place
            /*
             * if this has changed we simply change the blocking queue instance with a new pool 
             * size and reset the 
             */
            bytesPerMemoryIndex = byteSize.bytes() / newPoolSize;
            memoryIndexQueue = new ArrayBlockingQueue<ReusableMemoryIndex>(newPoolSize);
            poolCurrentSize = 0; // lets refill the queue
        }

        public ReusableMemoryIndex acquire() {
            final BlockingQueue<ReusableMemoryIndex> queue = memoryIndexQueue;
            final ReusableMemoryIndex poll = queue.poll();
            return poll == null ? waitOrCreate(queue) : poll;
        }

        private ReusableMemoryIndex waitOrCreate(BlockingQueue<ReusableMemoryIndex> queue) {
            synchronized (this) {
                if (poolCurrentSize < poolMaxSize) {
                    poolCurrentSize++;
                    return new ReusableMemoryIndex(false, bytesPerMemoryIndex);

                }
            }
            ReusableMemoryIndex poll = null;
            try {
                final TimeValue timeout = this.timeout; // only read the volatile var once
                poll = queue.poll(timeout.getMillis(), TimeUnit.MILLISECONDS); // delay this by 100ms by default
            } catch (InterruptedException ie) {
                // don't swallow the interrupt
                Thread.currentThread().interrupt();
            }
            return poll == null ? new ReusableMemoryIndex(false, bytesPerMemoryIndex) : poll;
        }

        public void release(ReusableMemoryIndex index) {
            assert index != null : "can't release null reference";
            if (bytesPerMemoryIndex == index.getMaxReuseBytes()) {
                index.reset();
                // only put is back into the queue if the size fits - prune old settings on the fly
                memoryIndexQueue.offer(index);
            }
        }

    }


    private IndicesService indicesService;
    private final MemoryIndexPool memIndexPool;

    @Inject
    public PercolatorExecutor(Index index, @IndexSettings Settings indexSettings,
                              MapperService mapperService, IndexQueryParserService queryParserService,
                              IndexCache indexCache, IndexFieldDataService fieldDataService, IndexSettingsService indexSettingsService) {
        super(index, indexSettings);
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.indexCache = indexCache;
        this.fieldDataService = fieldDataService;
        memIndexPool = new MemoryIndexPool(indexSettings);
        ApplySettings applySettings = new ApplySettings();
        indexSettingsService.addListener(applySettings);
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            memIndexPool.updateSettings(settings);
        }
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
        final ReusableMemoryIndex memoryIndex = memIndexPool.acquire();
        try {
            // TODO: This means percolation does not support nested docs...
            for (IndexableField field : request.doc().rootDoc().getFields()) {
                if (!field.fieldType().indexed()) {
                    continue;
                }
                // no need to index the UID field
                if (field.name().equals(UidFieldMapper.NAME)) {
                    continue;
                }
                TokenStream tokenStream;
                try {
                    tokenStream = field.tokenStream(request.doc().analyzer());
                    if (tokenStream != null) {
                        memoryIndex.addField(field.name(), tokenStream, field.boost());
                    }
                } catch (IOException e) {
                    throw new ElasticSearchException("Failed to create token stream", e);
                }
            }

            final IndexSearcher searcher = memoryIndex.createSearcher();
            List<String> matches = new ArrayList<String>();

            try {
                if (request.query() == null) {
                    Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
                    for (Map.Entry<String, Query> entry : queries.entrySet()) {
                        collector.reset();
                        try {
                            searcher.search(entry.getValue(), collector);
                        } catch (IOException e) {
                            logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                        }

                        if (collector.exists()) {
                            matches.add(entry.getKey());
                        }
                    }
                } else {
                    IndexService percolatorIndex = percolatorIndexServiceSafe();
                    if (percolatorIndex.numberOfShards() == 0) {
                        throw new PercolateIndexUnavailable(new Index(PercolatorService.INDEX_NAME));
                    }
                    IndexShard percolatorShard = percolatorIndex.shard(0);
                    Engine.Searcher percolatorSearcher = percolatorShard.acquireSearcher("percolate");
                    try {
                        percolatorSearcher.searcher().search(request.query(), new QueryCollector(logger, queries, searcher, percolatorIndex, matches));
                    } catch (IOException e) {
                        logger.warn("failed to execute", e);
                    } finally {
                        percolatorSearcher.release();
                    }
                }
            } finally {
                // explicitly clear the reader, since we can only register on callback on SegmentReader
                indexCache.clear(searcher.getIndexReader());
                fieldDataService.clear(searcher.getIndexReader());
            }
            return new Response(matches, request.doc().mappingsModified());
        } finally {
            memIndexPool.release(memoryIndex);
        }

    }

    private IndexService percolatorIndexServiceSafe() {
        IndexService indexService = indicesService.indexService(PercolatorService.INDEX_NAME);
        if (indexService == null) {
            throw new PercolateIndexUnavailable(new Index(PercolatorService.INDEX_NAME));
        }
        return indexService;
    }

    static class QueryCollector extends Collector {
        private final IndexFieldData uidFieldData;
        private final IndexSearcher searcher;
        private final IndexService percolatorIndex;
        private final List<String> matches;
        private final Map<String, Query> queries;
        private final ESLogger logger;

        private final Lucene.ExistsCollector collector = new Lucene.ExistsCollector();

        private BytesValues values;

        QueryCollector(ESLogger logger, Map<String, Query> queries, IndexSearcher searcher, IndexService percolatorIndex, List<String> matches) {
            this.logger = logger;
            this.queries = queries;
            this.searcher = searcher;
            this.percolatorIndex = percolatorIndex;
            this.matches = matches;
            // TODO: when we move to a UID level mapping def on the index level, we can use that one, now, its per type, and we can't easily choose one
            this.uidFieldData = percolatorIndex.fieldData().getForField(new FieldMapper.Names(UidFieldMapper.NAME), new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")));
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
        }

        @Override
        public void collect(int doc) throws IOException {
            if (values.setDocument(doc) == 0) {
                return;
            }
            String id = Uid.idFromUid(values.nextValue()).toUtf8();
            Query query = queries.get(id);
            if (query == null) {
                // log???
                return;
            }
            // run the query
            try {
                collector.reset();
                searcher.search(query, collector);
                if (collector.exists()) {
                    matches.add(id);
                }
            } catch (IOException e) {
                logger.warn("[" + id + "] failed to execute query", e);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            // we use the UID because id might not be indexed
            values = uidFieldData.load(context).getBytesValues(false);
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }
    }

    public void clearQueries() {
        this.queries.clear();
    }
}
