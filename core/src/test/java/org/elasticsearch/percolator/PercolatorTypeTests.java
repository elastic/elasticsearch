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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.*;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.percolator.QueryMetadataService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class PercolatorTypeTests extends ESTestCase {

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
        PercolatorQueriesRegistry registry = createRegistry();
        addPercolatorQuery("1", new TermQuery(new Term("field", "brown")), indexWriter, registry);
        addPercolatorQuery("2", new TermQuery(new Term("field", "fox")), indexWriter, registry);
        addPercolatorQuery("3", new TermQuery(new Term("field", "monkey")), indexWriter, registry);

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);


        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();

        CountPercolatorType countPercolatorType = new CountPercolatorType(null, null);
        TotalHitCountCollector countCollector = countPercolatorType.doPercolate(null, null, new MatchAllDocsQuery(), registry, shardSearcher, percolateSearcher, 0);
        assertThat(countCollector.getTotalHits(), equalTo(2));
    }

    public void testTopMatching() throws Exception {
        PercolatorQueriesRegistry registry = createRegistry();
        addPercolatorQuery("1", new TermQuery(new Term("field", "brown")), indexWriter, registry);
        addPercolatorQuery("2", new TermQuery(new Term("field", "monkey")), indexWriter, registry);
        addPercolatorQuery("3", new TermQuery(new Term("field", "fox")), indexWriter, registry);

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher =  new IndexSearcher(directoryReader);


        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();

        TopMatchingPercolatorType countPercolatorType = new TopMatchingPercolatorType(null, null, null);
        TopDocsCollector topDocsCollector = countPercolatorType.doPercolate(null, null, new MatchAllDocsQuery(), registry, shardSearcher, percolateSearcher, 10);
        TopDocs topDocs = topDocsCollector.topDocs();
        assertThat(topDocs.totalHits, equalTo(2));
        assertThat(topDocs.scoreDocs.length, equalTo(2));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
    }

    void addPercolatorQuery(String id, Query query, IndexWriter writer, PercolatorQueriesRegistry registry) throws IOException {
        registry.getPercolateQueries().put(new BytesRef(id), query);
        ParseContext.Document document = new ParseContext.Document();
        QueryMetadataService.extractQueryMetadata(query, document);
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
        AnalysisService analysisService = new AnalysisService(indexSettings, Collections.<String, AnalyzerProvider>emptyMap(), Collections.<String, TokenizerFactory>emptyMap(), Collections.<String, CharFilterFactory>emptyMap(), Collections.<String, TokenFilterFactory>emptyMap());
        MapperService mapperService = new MapperService(indexSettings, analysisService, new SimilarityService(indexSettings, Collections.emptyMap()));
        return new PercolatorQueriesRegistry(
                new ShardId(index, 0),
                indexSettings,
                null,
                null,
                mapperService,
                null
        );
    }

}
