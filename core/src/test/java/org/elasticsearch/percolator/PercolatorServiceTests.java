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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.PercolatorFieldMapper;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.percolator.ExtractQueryTermsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PercolatorServiceTests extends ESTestCase {

    private Directory directory;
    private IndexWriter indexWriter;
    private DirectoryReader directoryReader;

    @Before
    public void init() throws Exception {
        directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        indexWriter = new IndexWriter(directory, config);
    }

    @After
    public void destroy() throws Exception {
        directoryReader.close();
        directory.close();
    }

    public void testCount() throws Exception {
        PercolateContext context = mock(PercolateContext.class);
        when(context.shardTarget()).thenReturn(new SearchShardTarget("_id", "_index", 0));
        when(context.percolatorTypeFilter()).thenReturn(new MatchAllDocsQuery());
        when(context.isOnlyCount()).thenReturn(true);

        PercolatorQueriesRegistry registry = createRegistry();
        addPercolatorQuery("1", new TermQuery(new Term("field", "brown")), indexWriter, registry);
        addPercolatorQuery("2", new TermQuery(new Term("field", "fox")), indexWriter, registry);
        addPercolatorQuery("3", new TermQuery(new Term("field", "monkey")), indexWriter, registry);

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        when(context.searcher()).thenReturn(new ContextIndexSearcher(new Engine.Searcher("test", shardSearcher), shardSearcher.getQueryCache(), shardSearcher.getQueryCachingPolicy()));

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        when(context.docSearcher()).thenReturn(percolateSearcher);

        PercolateShardResponse response = PercolatorService.doPercolate(context, registry, null, null, null);
        assertThat(response.topDocs().totalHits, equalTo(2));
    }

    public void testTopMatching() throws Exception {
        PercolateContext context = mock(PercolateContext.class);
        when(context.shardTarget()).thenReturn(new SearchShardTarget("_id", "_index", 0));
        when(context.percolatorTypeFilter()).thenReturn(new MatchAllDocsQuery());
        when(context.size()).thenReturn(10);

        PercolatorQueriesRegistry registry = createRegistry();
        addPercolatorQuery("1", new TermQuery(new Term("field", "brown")), indexWriter, registry);
        addPercolatorQuery("2", new TermQuery(new Term("field", "monkey")), indexWriter, registry);
        addPercolatorQuery("3", new TermQuery(new Term("field", "fox")), indexWriter, registry);

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);
        when(context.searcher()).thenReturn(new ContextIndexSearcher(new Engine.Searcher("test", shardSearcher), shardSearcher.getQueryCache(), shardSearcher.getQueryCachingPolicy()));

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        when(context.docSearcher()).thenReturn(percolateSearcher);

        PercolateShardResponse response = PercolatorService.doPercolate(context, registry, null, null, null);
        TopDocs topDocs = response.topDocs();
        assertThat(topDocs.totalHits, equalTo(2));
        assertThat(topDocs.scoreDocs.length, equalTo(2));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
    }

    void addPercolatorQuery(String id, Query query, IndexWriter writer, PercolatorQueriesRegistry registry) throws IOException {
        registry.getPercolateQueries().put(new BytesRef(id), query);
        ParseContext.Document document = new ParseContext.Document();
        FieldType extractedQueryTermsFieldType = new FieldType();
        extractedQueryTermsFieldType.setTokenized(false);
        extractedQueryTermsFieldType.setIndexOptions(IndexOptions.DOCS);
        extractedQueryTermsFieldType.freeze();
        ExtractQueryTermsService.extractQueryTerms(query, document, PercolatorFieldMapper.EXTRACTED_TERMS_FULL_FIELD_NAME, PercolatorFieldMapper.UNKNOWN_QUERY_FULL_FIELD_NAME, extractedQueryTermsFieldType);
        document.add(new StoredField(UidFieldMapper.NAME, Uid.createUid(PercolatorService.TYPE_NAME, id)));
        writer.addDocument(document);
    }

    PercolatorQueriesRegistry createRegistry() {
        Index index = new Index("_index");
        IndexSettings indexSettings = new IndexSettings(new IndexMetaData.Builder("_index").settings(
                Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .build(),
                Settings.EMPTY, Collections.emptyList()
        );
        return new PercolatorQueriesRegistry(
                new ShardId(index, 0),
                indexSettings,
                null
        );
    }

}
