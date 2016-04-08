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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexWarmer;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.PercolatorQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PercolatorQueryCacheTests extends ESTestCase {

    private QueryShardContext queryShardContext;
    private PercolatorQueryCache cache;

    void initialize(Object... fields) throws IOException {
        Settings settings = Settings.builder()
                .put("node.name", PercolatorQueryCacheTests.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();

        Map<String, Tuple<ParseField, QueryParser<?>>> queryParsers = new HashMap<>();
        QueryParser<TermQueryBuilder> termParser = TermQueryBuilder::fromXContent;
        queryParsers.put(TermQueryBuilder.NAME, new Tuple<>(TermQueryBuilder.QUERY_NAME_FIELD, termParser));
        QueryParser<WildcardQueryBuilder> wildcardParser = WildcardQueryBuilder::fromXContent; 
        queryParsers.put(WildcardQueryBuilder.NAME, new Tuple<>(WildcardQueryBuilder.QUERY_NAME_FIELD, wildcardParser));
        QueryParser<BoolQueryBuilder> boolQueryParser = BoolQueryBuilder::fromXContent;
        queryParsers.put(BoolQueryBuilder.NAME, new Tuple<>(BoolQueryBuilder.QUERY_NAME_FIELD, boolQueryParser));
        IndicesQueriesRegistry indicesQueriesRegistry = new IndicesQueriesRegistry(settings, queryParsers);

        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(new Index("_index", ClusterState.UNKNOWN_UUID), indexSettings);
        SimilarityService similarityService = new SimilarityService(idxSettings, Collections.emptyMap());
        AnalysisService analysisService = new AnalysisRegistry(null, new Environment(settings)).build(idxSettings);
        MapperRegistry mapperRegistry = new IndicesModule().getMapperRegistry();
        MapperService mapperService = new MapperService(idxSettings, analysisService, similarityService, mapperRegistry,
                () -> queryShardContext);
        mapperService.merge("type", new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef("type", fields).string()),
                MapperService.MergeReason.MAPPING_UPDATE, false);
        cache = new PercolatorQueryCache(idxSettings, () -> queryShardContext);
        queryShardContext = new QueryShardContext(idxSettings, null, null, mapperService, similarityService, null,
                    indicesQueriesRegistry, cache, null);
    }

    public void testLoadQueries() throws Exception {
        Directory directory = newDirectory();
        IndexWriter indexWriter = new IndexWriter(
                directory,
                newIndexWriterConfig(new MockAnalyzer(random()))
                        .setMergePolicy(NoMergePolicy.INSTANCE)
                        .setMaxBufferedDocs(16)
        );

        boolean legacyFormat = randomBoolean();
        Version version = legacyFormat ? Version.V_2_0_0 : Version.CURRENT;

        storeQuery("0", indexWriter, termQuery("field1", "value1"), true, legacyFormat);
        storeQuery("1", indexWriter, wildcardQuery("field1", "v*"), true, legacyFormat);
        storeQuery("2", indexWriter, boolQuery().must(termQuery("field1", "value1")).must(termQuery("field2", "value2")),
                true, legacyFormat);
        // dymmy docs should be skipped during loading:
        Document doc = new Document();
        doc.add(new StringField("dummy", "value", Field.Store.YES));
        indexWriter.addDocument(doc);
        storeQuery("4", indexWriter, termQuery("field2", "value2"), true, legacyFormat);
        // only documents that .percolator type should be loaded:
        storeQuery("5", indexWriter, termQuery("field2", "value2"), false, legacyFormat);
        storeQuery("6", indexWriter, termQuery("field3", "value3"), true, legacyFormat);
        indexWriter.forceMerge(1);

        // also include queries for percolator docs marked as deleted:
        indexWriter.deleteDocuments(new Term("id", "6"));
        indexWriter.close();

        ShardId shardId = new ShardId("_index", ClusterState.UNKNOWN_UUID, 0);
        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), shardId);
        assertThat(indexReader.leaves().size(), equalTo(1));
        assertThat(indexReader.numDeletedDocs(), equalTo(1));
        assertThat(indexReader.maxDoc(), equalTo(7));

        initialize("field1", "type=keyword", "field2", "type=keyword", "field3", "type=keyword");

        PercolatorQueryCache.QueriesLeaf leaf = cache.loadQueries(indexReader.leaves().get(0), version);
        assertThat(leaf.queries.size(), equalTo(5));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("field1", "value1"))));
        assertThat(leaf.getQuery(1), equalTo(new WildcardQuery(new Term("field1", "v*"))));
        assertThat(leaf.getQuery(2), equalTo(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("field1", "value1")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field2", "value2")), BooleanClause.Occur.MUST)
                .build()
        ));
        assertThat(leaf.getQuery(4), equalTo(new TermQuery(new Term("field2", "value2"))));
        assertThat(leaf.getQuery(6), equalTo(new TermQuery(new Term("field3", "value3"))));

        indexReader.close();
        directory.close();
    }

    public void testGetQueries() throws Exception {
        Directory directory = newDirectory();
        IndexWriter indexWriter = new IndexWriter(
                directory,
                newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE)
        );

        storeQuery("0", indexWriter, termQuery("a", "0"), true, false);
        storeQuery("1", indexWriter, termQuery("a", "1"), true, false);
        storeQuery("2", indexWriter, termQuery("a", "2"), true, false);
        indexWriter.flush();
        storeQuery("3", indexWriter, termQuery("a", "3"), true, false);
        storeQuery("4", indexWriter, termQuery("a", "4"), true, false);
        storeQuery("5", indexWriter, termQuery("a", "5"), true, false);
        indexWriter.flush();
        storeQuery("6", indexWriter, termQuery("a", "6"), true, false);
        storeQuery("7", indexWriter, termQuery("a", "7"), true, false);
        storeQuery("8", indexWriter, termQuery("a", "8"), true, false);
        indexWriter.flush();
        indexWriter.close();

        ShardId shardId = new ShardId("_index", ClusterState.UNKNOWN_UUID , 0);
        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), shardId);
        assertThat(indexReader.leaves().size(), equalTo(3));
        assertThat(indexReader.maxDoc(), equalTo(9));

        initialize("a", "type=keyword");

        try {
            cache.getQueries(indexReader.leaves().get(0));
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("queries not loaded, queries should be have been preloaded during index warming..."));
        }

        IndexShard indexShard = mockIndexShard();
        ThreadPool threadPool = mockThreadPool();
        IndexWarmer.Listener listener = cache.createListener(threadPool);
        listener.warmReader(indexShard, new Engine.Searcher("test", new IndexSearcher(indexReader)));
        PercolatorQueryCacheStats stats = cache.getStats(shardId);
        assertThat(stats.getNumQueries(), equalTo(9L));

        PercolatorQuery.QueryRegistry.Leaf leaf = cache.getQueries(indexReader.leaves().get(0));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "0"))));
        assertThat(leaf.getQuery(1), equalTo(new TermQuery(new Term("a", "1"))));
        assertThat(leaf.getQuery(2), equalTo(new TermQuery(new Term("a", "2"))));

        leaf = cache.getQueries(indexReader.leaves().get(1));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "3"))));
        assertThat(leaf.getQuery(1), equalTo(new TermQuery(new Term("a", "4"))));
        assertThat(leaf.getQuery(2), equalTo(new TermQuery(new Term("a", "5"))));

        leaf = cache.getQueries(indexReader.leaves().get(2));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "6"))));
        assertThat(leaf.getQuery(1), equalTo(new TermQuery(new Term("a", "7"))));
        assertThat(leaf.getQuery(2), equalTo(new TermQuery(new Term("a", "8"))));

        indexReader.close();
        directory.close();
    }

    public void testInvalidateEntries() throws Exception {
        Directory directory = newDirectory();
        IndexWriter indexWriter = new IndexWriter(
                directory,
                newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE)
        );

        storeQuery("0", indexWriter, termQuery("a", "0"), true, false);
        indexWriter.flush();
        storeQuery("1", indexWriter, termQuery("a", "1"), true, false);
        indexWriter.flush();
        storeQuery("2", indexWriter, termQuery("a", "2"), true, false);
        indexWriter.flush();

        ShardId shardId = new ShardId("_index", ClusterState.UNKNOWN_UUID, 0);
        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
        assertThat(indexReader.leaves().size(), equalTo(3));
        assertThat(indexReader.maxDoc(), equalTo(3));

        initialize("a", "type=keyword");

        IndexShard indexShard = mockIndexShard();
        ThreadPool threadPool = mockThreadPool();
        IndexWarmer.Listener listener = cache.createListener(threadPool);
        listener.warmReader(indexShard, new Engine.Searcher("test", new IndexSearcher(indexReader)));
        assertThat(cache.getStats(shardId).getNumQueries(), equalTo(3L));

        PercolatorQuery.QueryRegistry.Leaf leaf = cache.getQueries(indexReader.leaves().get(0));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "0"))));

        leaf = cache.getQueries(indexReader.leaves().get(1));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "1"))));

        leaf = cache.getQueries(indexReader.leaves().get(2));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "2"))));

        // change merge policy, so that merges will actually happen:
        indexWriter.getConfig().setMergePolicy(new TieredMergePolicy());
        indexWriter.deleteDocuments(new Term("id", "1"));
        indexWriter.forceMergeDeletes();
        indexReader.close();
        indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
        assertThat(indexReader.leaves().size(), equalTo(2));
        assertThat(indexReader.maxDoc(), equalTo(2));
        listener.warmReader(indexShard, new Engine.Searcher("test", new IndexSearcher(indexReader)));
        assertThat(cache.getStats(shardId).getNumQueries(), equalTo(2L));

        leaf = cache.getQueries(indexReader.leaves().get(0));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "0"))));

        leaf = cache.getQueries(indexReader.leaves().get(1));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "2"))));

        indexWriter.forceMerge(1);
        indexReader.close();
        indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
        assertThat(indexReader.leaves().size(), equalTo(1));
        assertThat(indexReader.maxDoc(), equalTo(2));
        listener.warmReader(indexShard, new Engine.Searcher("test", new IndexSearcher(indexReader)));
        assertThat(cache.getStats(shardId).getNumQueries(), equalTo(2L));

        leaf = cache.getQueries(indexReader.leaves().get(0));
        assertThat(leaf.getQuery(0), equalTo(new TermQuery(new Term("a", "0"))));
        assertThat(leaf.getQuery(1), equalTo(new TermQuery(new Term("a", "2"))));

        indexWriter.close();
        indexReader.close();
        directory.close();
    }

    void storeQuery(String id, IndexWriter indexWriter, QueryBuilder queryBuilder, boolean typeField, boolean legacy) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.NO));
        if (typeField) {
            doc.add(new StringField(TypeFieldMapper.NAME, PercolatorFieldMapper.TYPE_NAME, Field.Store.NO));
        }
        if (legacy) {
            BytesReference percolatorQuery = XContentFactory.jsonBuilder().startObject()
                    .field("query", queryBuilder)
                    .endObject().bytes();
            doc.add(new StoredField(
                    SourceFieldMapper.NAME,
                    percolatorQuery.array(), percolatorQuery.arrayOffset(), percolatorQuery.length())
            );
        } else {
            BytesRef queryBuilderAsBytes = new BytesRef(
                    XContentFactory.contentBuilder(PercolatorQueryCache.QUERY_BUILDER_CONTENT_TYPE).value(queryBuilder).bytes().toBytes()
            );
            doc.add(new BinaryDocValuesField(PercolatorFieldMapper.QUERY_BUILDER_FULL_FIELD_NAME, queryBuilderAsBytes));
        }
        indexWriter.addDocument(doc);
    }

    IndexShard mockIndexShard() {
        IndexShard indexShard = mock(IndexShard.class);
        ShardIndexWarmerService shardIndexWarmerService = mock(ShardIndexWarmerService.class);
        when(shardIndexWarmerService.logger()).thenReturn(logger);
        when(indexShard.warmerService()).thenReturn(shardIndexWarmerService);
        IndexSettings indexSettings = new IndexSettings(
                IndexMetaData.builder("_index").settings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                ).build(),
                Settings.EMPTY
        );
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        return indexShard;
    }

    ThreadPool mockThreadPool() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(Runnable::run);
        return threadPool;
    }

}
