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

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.ExtractQueryTermsService;
import org.elasticsearch.index.percolator.PercolatorFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PercolatorQueryTests extends ESTestCase {

    public final static String EXTRACTED_TERMS_FIELD_NAME = "extracted_terms";
    public final static String UNKNOWN_QUERY_FIELD_NAME = "unknown_query";
    public static FieldType EXTRACTED_TERMS_FIELD_TYPE = new FieldType();

    static {
        EXTRACTED_TERMS_FIELD_TYPE.setTokenized(false);
        EXTRACTED_TERMS_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        EXTRACTED_TERMS_FIELD_TYPE.freeze();
    }

    private Directory directory;
    private IndexWriter indexWriter;
    private Map<String, Query> queries;
    private PercolatorQuery.QueryRegistry queryRegistry;
    private DirectoryReader directoryReader;

    @Before
    public void init() throws Exception {
        directory = newDirectory();
        queries = new HashMap<>();
        queryRegistry = ctx -> docId -> {
            try {
                String val = ctx.reader().document(docId).get(UidFieldMapper.NAME);
                return queries.get(Uid.createUid(val).id());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
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
        addPercolatorQuery("1", new TermQuery(new Term("field", "brown")));
        addPercolatorQuery("2", new TermQuery(new Term("field", "monkey")));
        addPercolatorQuery("3", new TermQuery(new Term("field", "fox")));
        BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
        bq1.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.SHOULD);
        bq1.add(new TermQuery(new Term("field", "monkey")), BooleanClause.Occur.SHOULD);
        addPercolatorQuery("4", bq1.build());
        BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
        bq2.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.MUST);
        bq2.add(new TermQuery(new Term("field", "monkey")), BooleanClause.Occur.MUST);
        addPercolatorQuery("5", bq2.build());
        BooleanQuery.Builder bq3 = new BooleanQuery.Builder();
        bq3.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.MUST);
        bq3.add(new TermQuery(new Term("field", "apes")), BooleanClause.Occur.MUST_NOT);
        addPercolatorQuery("6", bq3.build());
        BooleanQuery.Builder bq4 = new BooleanQuery.Builder();
        bq4.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.MUST_NOT);
        bq4.add(new TermQuery(new Term("field", "apes")), BooleanClause.Occur.MUST);
        addPercolatorQuery("7", bq4.build());
        PhraseQuery.Builder pq1 = new PhraseQuery.Builder();
        pq1.add(new Term("field", "lazy"));
        pq1.add(new Term("field", "dog"));
        addPercolatorQuery("8", pq1.build());

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();

        PercolatorQuery.Builder builder = new PercolatorQuery.Builder(
                "docType",
                queryRegistry,
                new BytesArray("{}"),
                percolateSearcher,
                new MatchAllDocsQuery()
        );
        builder.extractQueryTermsQuery(EXTRACTED_TERMS_FIELD_NAME, UNKNOWN_QUERY_FIELD_NAME);
        TopDocs topDocs = shardSearcher.search(builder.build(), 10);
        assertThat(topDocs.totalHits, equalTo(5));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(5));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(7));
    }

    public void testDuel() throws Exception {
        int numQueries = scaledRandomIntBetween(32, 256);
        for (int i = 0; i < numQueries; i++) {
            String id = Integer.toString(i);
            Query query;
            if (randomBoolean()) {
                query = new PrefixQuery(new Term("field", id));
            } else if (randomBoolean()) {
                query = new WildcardQuery(new Term("field", id + "*"));
            } else if (randomBoolean()) {
                query = new CustomQuery(new Term("field", id + "*"));
            } else {
                query = new TermQuery(new Term("field", id));
            }
            addPercolatorQuery(id, query);
        }

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);

        for (int i = 0; i < numQueries; i++) {
            MemoryIndex memoryIndex = new MemoryIndex();
            String id = Integer.toString(i);
            memoryIndex.addField("field", id, new WhitespaceAnalyzer());
            IndexSearcher percolateSearcher = memoryIndex.createSearcher();

            PercolatorQuery.Builder builder1 = new PercolatorQuery.Builder(
                    "docType",
                    queryRegistry,
                    new BytesArray("{}"),
                    percolateSearcher,
                    new MatchAllDocsQuery()
            );
            // enables the optimization that prevents queries from being evaluated that don't match
            builder1.extractQueryTermsQuery(EXTRACTED_TERMS_FIELD_NAME, UNKNOWN_QUERY_FIELD_NAME);
            TopDocs topDocs1 = shardSearcher.search(builder1.build(), 10);

            PercolatorQuery.Builder builder2 = new PercolatorQuery.Builder(
                    "docType",
                    queryRegistry,
                    new BytesArray("{}"),
                    percolateSearcher,
                    new MatchAllDocsQuery()
            );
            TopDocs topDocs2 = shardSearcher.search(builder2.build(), 10);

            assertThat(topDocs1.totalHits, equalTo(topDocs2.totalHits));
            assertThat(topDocs1.scoreDocs.length, equalTo(topDocs2.scoreDocs.length));
            for (int j = 0; j < topDocs1.scoreDocs.length; j++) {
                assertThat(topDocs1.scoreDocs[j].doc, equalTo(topDocs2.scoreDocs[j].doc));
            }
        }
    }

    void addPercolatorQuery(String id, Query query, String... extraFields) throws IOException {
        queries.put(id, query);
        ParseContext.Document document = new ParseContext.Document();
        ExtractQueryTermsService.extractQueryTerms(query, document, EXTRACTED_TERMS_FIELD_NAME, UNKNOWN_QUERY_FIELD_NAME,
                EXTRACTED_TERMS_FIELD_TYPE);
        document.add(new StoredField(UidFieldMapper.NAME, Uid.createUid(PercolatorFieldMapper.TYPE_NAME, id)));
        assert extraFields.length % 2 == 0;
        for (int i = 0; i < extraFields.length; i++) {
            document.add(new StringField(extraFields[i], extraFields[++i], Field.Store.NO));
        }
        indexWriter.addDocument(document);
    }

    private final static class CustomQuery extends Query {

        private final Term term;

        private CustomQuery(Term term) {
            this.term = term;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            return new TermQuery(term);
        }

        @Override
        public String toString(String field) {
            return "custom{" + field + "}";
        }
    }

}
