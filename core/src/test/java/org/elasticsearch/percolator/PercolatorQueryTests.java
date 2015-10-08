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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.QueryMetadataService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PercolatorQueryTests extends ESTestCase {

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

    public void testVariousQueries() throws Exception {
        Map<BytesRef, Query> queries = new HashMap<>();
        QueryMetadataService queryMetadataService = new QueryMetadataService();
        addPercolatorQuery("1", new TermQuery(new Term("field", "brown")), indexWriter, queryMetadataService, queries);
        addPercolatorQuery("2", new TermQuery(new Term("field", "monkey")), indexWriter, queryMetadataService, queries);
        addPercolatorQuery("3", new TermQuery(new Term("field", "fox")), indexWriter, queryMetadataService, queries);
        BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
        bq1.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.SHOULD);
        bq1.add(new TermQuery(new Term("field", "monkey")), BooleanClause.Occur.SHOULD);
        addPercolatorQuery("4", bq1.build(), indexWriter, queryMetadataService, queries);
        BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
        bq2.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.MUST);
        bq2.add(new TermQuery(new Term("field", "monkey")), BooleanClause.Occur.MUST);
        addPercolatorQuery("5", bq2.build(), indexWriter, queryMetadataService, queries);
        BooleanQuery.Builder bq3 = new BooleanQuery.Builder();
        bq3.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.MUST);
        bq3.add(new TermQuery(new Term("field", "apes")), BooleanClause.Occur.MUST_NOT);
        addPercolatorQuery("6", bq3.build(), indexWriter, queryMetadataService, queries);
        BooleanQuery.Builder bq4 = new BooleanQuery.Builder();
        bq4.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.MUST_NOT);
        bq4.add(new TermQuery(new Term("field", "apes")), BooleanClause.Occur.MUST);
        addPercolatorQuery("7", bq4.build(), indexWriter, queryMetadataService, queries);
        PhraseQuery.Builder pq1 = new PhraseQuery.Builder();
        pq1.add(new Term("field", "lazy"));
        pq1.add(new Term("field", "dog"));
        addPercolatorQuery("8", pq1.build(), indexWriter, queryMetadataService, queries);

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher =  new IndexSearcher(directoryReader);

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();

        PercolatorQuery.Builder builder = new PercolatorQuery.Builder(
                percolateSearcher,
                queries,
                new MatchAllDocsQuery()
        );
        builder.setQueriesMetaDataQuery(queryMetadataService);
        TopDocs topDocs = shardSearcher.search(builder.build(), 10);
        assertThat(topDocs.totalHits, equalTo(5));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(5));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(7));
    }

    public void testWithScoring() throws Exception {
        Map<BytesRef, Query> queries = new HashMap<>();
        QueryMetadataService queryMetadataService = new QueryMetadataService();
        addPercolatorQuery("1", new TermQuery(new Term("field", "brown")), indexWriter, queryMetadataService, queries, "field", "value1");

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher =  new IndexSearcher(directoryReader);

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();

        PercolatorQuery.Builder builder = new PercolatorQuery.Builder(
                percolateSearcher,
                queries,
                new MatchAllDocsQuery()
        );
        builder.setQueriesMetaDataQuery(queryMetadataService);
        builder.setPercolateQuery(new TermQuery(new Term("field", "value1")));

        PercolatorQuery percolatorQuery = builder.build();
        TopDocs topDocs = shardSearcher.search(percolatorQuery, 1);
        assertThat(topDocs.totalHits, equalTo(1));
        assertThat(topDocs.scoreDocs.length, equalTo(1));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertEquals(topDocs.scoreDocs[0].score, 0.3f, 0.01f);

        Explanation explanation = shardSearcher.explain(percolatorQuery, 0);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[0].score));
    }

    void addPercolatorQuery(String id, Query query, IndexWriter writer, QueryMetadataService queryMetadataService, Map<BytesRef, Query> queries, String... extraFields) throws IOException {
        queries.put(new BytesRef(id), query);
        Document document = new Document();
        Set<Term> queryTerms = queryMetadataService.extractQueryMetadata(query);
        for (Term term : queryTerms) {
            document.add(new Field(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + term.field(), term.bytes(), QueryMetadataService.QUERY_METADATA_FIELD_TYPE));
        }
        document.add(new StoredField(UidFieldMapper.NAME, Uid.createUid(PercolatorService.TYPE_NAME, id)));
        assert extraFields.length % 2 == 0;
        for (int i = 0; i < extraFields.length; i++) {
            document.add(new StringField(extraFields[i], extraFields[++i], Field.Store.NO));
        }

        writer.addDocument(document);
    }

}
