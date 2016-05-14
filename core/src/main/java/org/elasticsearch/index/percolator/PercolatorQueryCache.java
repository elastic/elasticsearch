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

package org.elasticsearch.index.percolator;

import com.carrotsearch.hppc.IntObjectHashMap;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexWarmer;
import org.elasticsearch.index.IndexWarmer.TerminationHandle;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.query.PercolateQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static org.elasticsearch.index.percolator.PercolatorFieldMapper.LEGACY_TYPE_NAME;
import static org.elasticsearch.index.percolator.PercolatorFieldMapper.PercolatorFieldType;
import static org.elasticsearch.index.percolator.PercolatorFieldMapper.parseQuery;

public final class PercolatorQueryCache extends AbstractIndexComponent
        implements Closeable, LeafReader.CoreClosedListener, PercolateQuery.QueryRegistry {

    public final static Setting<Boolean> INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING =
            Setting.boolSetting("index.percolator.map_unmapped_fields_as_string", false, Setting.Property.IndexScope);

    public final static XContentType QUERY_BUILDER_CONTENT_TYPE = XContentType.SMILE;

    private final Supplier<QueryShardContext> queryShardContextSupplier;
    private final Cache<Object, QueriesLeaf> cache;
    private final boolean mapUnmappedFieldsAsString;

    public PercolatorQueryCache(IndexSettings indexSettings, Supplier<QueryShardContext> queryShardContextSupplier) {
        super(indexSettings);
        this.queryShardContextSupplier = queryShardContextSupplier;
        cache = CacheBuilder.<Object, QueriesLeaf>builder().build();
        this.mapUnmappedFieldsAsString = indexSettings.getValue(INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING);
    }

    @Override
    public Leaf getQueries(LeafReaderContext ctx) {
        QueriesLeaf percolatorQueries = cache.get(ctx.reader().getCoreCacheKey());
        if (percolatorQueries == null) {
            throw new IllegalStateException("queries not loaded, queries should be have been preloaded during index warming...");
        }
        return percolatorQueries;
    }

    public IndexWarmer.Listener createListener(ThreadPool threadPool) {
        return new IndexWarmer.Listener() {

            final Executor executor = threadPool.executor(ThreadPool.Names.WARMER);

            @Override
            public TerminationHandle warmReader(IndexShard indexShard, Searcher searcher) {
                final CountDownLatch latch = new CountDownLatch(searcher.reader().leaves().size());
                for (final LeafReaderContext ctx : searcher.reader().leaves()) {
                    if (cache.get(ctx.reader().getCoreCacheKey()) != null) {
                        latch.countDown();
                        continue;
                    }
                    executor.execute(() -> {
                        try {
                            final long start = System.nanoTime();
                            QueriesLeaf queries = loadQueries(ctx, indexShard);
                            cache.put(ctx.reader().getCoreCacheKey(), queries);
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace(
                                        "loading percolator queries took [{}]",
                                        TimeValue.timeValueNanos(System.nanoTime() - start)
                                );
                            }
                        } catch (Throwable t) {
                            indexShard.warmerService().logger().warn("failed to load percolator queries", t);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                return () -> latch.await();
            }
        };
    }

    QueriesLeaf loadQueries(LeafReaderContext context, IndexShard indexShard) throws IOException {
        Version indexVersionCreated = indexShard.indexSettings().getIndexVersionCreated();
        MapperService mapperService = indexShard.mapperService();
        LeafReader leafReader = context.reader();
        ShardId shardId = ShardUtils.extractShardId(leafReader);
        if (shardId == null) {
            throw new IllegalStateException("can't resolve shard id");
        }
        if (indexSettings.getIndex().equals(shardId.getIndex()) == false) {
            // percolator cache insanity
            String message = "Trying to load queries for index " + shardId.getIndex() + " with cache of index " +
                    indexSettings.getIndex();
            throw new IllegalStateException(message);
        }

        IntObjectHashMap<Query> queries = new IntObjectHashMap<>();
        boolean legacyLoading = indexVersionCreated.before(Version.V_5_0_0_alpha1);
        if (legacyLoading) {
            PostingsEnum postings = leafReader.postings(new Term(TypeFieldMapper.NAME, LEGACY_TYPE_NAME), PostingsEnum.NONE);
            if (postings != null) {
                LegacyQueryFieldVisitor visitor = new LegacyQueryFieldVisitor();
                for (int docId = postings.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = postings.nextDoc()) {
                    leafReader.document(docId, visitor);
                    queries.put(docId, parseLegacyPercolatorDocument(docId, visitor.source));
                    visitor.source = null; // reset
                }
            }
        } else {
            // Each type can have one percolator field mapper,
            // So for each type we check if there is a percolator field mapper
            // and parse all the queries for the documents of that type.
            IndexSearcher indexSearcher = new IndexSearcher(leafReader);
            for (DocumentMapper documentMapper : mapperService.docMappers(false)) {
                Weight queryWeight = indexSearcher.createNormalizedWeight(documentMapper.typeFilter(), false);
                for (FieldMapper fieldMapper : documentMapper.mappers()) {
                    if (fieldMapper instanceof PercolatorFieldMapper) {
                        PercolatorFieldType fieldType = (PercolatorFieldType) fieldMapper.fieldType();
                        BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(fieldType.getQueryBuilderFieldName());
                        if (binaryDocValues != null) {
                            // use the same leaf reader context the indexSearcher is using too:
                            Scorer scorer = queryWeight.scorer(leafReader.getContext());
                            if (scorer != null) {
                                DocIdSetIterator iterator = scorer.iterator();
                                for (int docId = iterator.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
                                    BytesRef qbSource = binaryDocValues.get(docId);
                                    if (qbSource.length > 0) {
                                        queries.put(docId, parseQueryBuilder(docId, qbSource));
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
        leafReader.addCoreClosedListener(this);
        return new QueriesLeaf(shardId, queries);
    }

    private Query parseQueryBuilder(int docId, BytesRef qbSource) {
        XContent xContent = QUERY_BUILDER_CONTENT_TYPE.xContent();
        try (XContentParser sourceParser = xContent.createParser(qbSource.bytes, qbSource.offset, qbSource.length)) {
            QueryShardContext context = queryShardContextSupplier.get();
            return parseQuery(context, mapUnmappedFieldsAsString, sourceParser);
        } catch (IOException e) {
            throw new PercolatorException(index(), "failed to parse query builder for document  [" + docId   + "]", e);
        }
    }

    private Query parseLegacyPercolatorDocument(int docId, BytesReference source) {
        try (XContentParser sourceParser = XContentHelper.createParser(source)) {
            String currentFieldName = null;
            XContentParser.Token token = sourceParser.nextToken(); // move the START_OBJECT
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchException("failed to parse query [" + docId + "], not starting with OBJECT");
            }
            while ((token = sourceParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = sourceParser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        QueryShardContext context = queryShardContextSupplier.get();
                        return parseQuery(context, mapUnmappedFieldsAsString, sourceParser);
                    } else {
                        sourceParser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    sourceParser.skipChildren();
                }
            }
        } catch (Exception e) {
            throw new PercolatorException(index(), "failed to parse query [" + docId + "]", e);
        }
        return null;
    }

    public PercolatorQueryCacheStats getStats(ShardId shardId) {
        int numberOfQueries = 0;
        for (QueriesLeaf queries : cache.values()) {
            if (shardId.equals(queries.shardId)) {
                numberOfQueries += queries.queries.size();
            }
        }
        return new PercolatorQueryCacheStats(numberOfQueries);
    }

    @Override
    public void onClose(Object cacheKey) throws IOException {
        cache.invalidate(cacheKey);
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();
    }

    final static class LegacyQueryFieldVisitor extends StoredFieldVisitor {

        private BytesArray source;

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] bytes) throws IOException {
            source = new BytesArray(bytes);
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (source != null)  {
                return Status.STOP;
            }
            if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.YES;
            } else {
                return Status.NO;
            }
        }

    }

    final static class QueriesLeaf implements Leaf {

        final ShardId shardId;
        final IntObjectHashMap<Query> queries;

        QueriesLeaf(ShardId shardId, IntObjectHashMap<Query> queries) {
            this.shardId = shardId;
            this.queries = queries;
        }

        @Override
        public Query getQuery(int docId) {
            return queries.get(docId);
        }
    }
}
