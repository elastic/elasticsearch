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
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PercolateQueryTests extends ESTestCase {

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
    private PercolateQuery.QueryStore queryStore;
    private DirectoryReader directoryReader;

    @Before
    public void init() throws Exception {
        directory = newDirectory();
        queries = new HashMap<>();
        queryStore = ctx -> docId -> {
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

        PercolateQuery.Builder builder = new PercolateQuery.Builder(
                "docType",
                queryStore,
                new BytesArray("{}"),
                percolateSearcher
        );
        builder.extractQueryTermsQuery(EXTRACTED_TERMS_FIELD_NAME, UNKNOWN_QUERY_FIELD_NAME);
        // no scoring, wrapping it in a constant score query:
        Query query = new ConstantScoreQuery(builder.build());
        TopDocs topDocs = shardSearcher.search(query, 10);
        assertThat(topDocs.totalHits, equalTo(5));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        Explanation explanation = shardSearcher.explain(query, 0);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[0].score));

        explanation = shardSearcher.explain(query, 1);
        assertThat(explanation.isMatch(), is(false));

        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        explanation = shardSearcher.explain(query, 2);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[1].score));

        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        explanation = shardSearcher.explain(query, 3);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[2].score));

        explanation = shardSearcher.explain(query, 4);
        assertThat(explanation.isMatch(), is(false));

        assertThat(topDocs.scoreDocs[3].doc, equalTo(5));
        explanation = shardSearcher.explain(query, 5);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[3].score));

        explanation = shardSearcher.explain(query, 6);
        assertThat(explanation.isMatch(), is(false));

        assertThat(topDocs.scoreDocs[4].doc, equalTo(7));
        explanation = shardSearcher.explain(query, 7);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[4].score));
    }

    public void testVariousQueries_withScoring() throws Exception {
        SpanNearQuery.Builder snp = new SpanNearQuery.Builder("field", true);
        snp.addClause(new SpanTermQuery(new Term("field", "jumps")));
        snp.addClause(new SpanTermQuery(new Term("field", "lazy")));
        snp.addClause(new SpanTermQuery(new Term("field", "dog")));
        snp.setSlop(2);
        addPercolatorQuery("1", snp.build());
        PhraseQuery.Builder pq1 = new PhraseQuery.Builder();
        pq1.add(new Term("field", "quick"));
        pq1.add(new Term("field", "brown"));
        pq1.add(new Term("field", "jumps"));
        pq1.setSlop(1);
        addPercolatorQuery("2", pq1.build());
        BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
        bq1.add(new TermQuery(new Term("field", "quick")), BooleanClause.Occur.MUST);
        bq1.add(new TermQuery(new Term("field", "brown")), BooleanClause.Occur.MUST);
        bq1.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.MUST);
        addPercolatorQuery("3", bq1.build());

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();

        PercolateQuery.Builder builder = new PercolateQuery.Builder(
                "docType",
                queryStore,
                new BytesArray("{}"),
                percolateSearcher
        );
        builder.extractQueryTermsQuery(EXTRACTED_TERMS_FIELD_NAME, UNKNOWN_QUERY_FIELD_NAME);
        Query query = builder.build();
        TopDocs topDocs = shardSearcher.search(query, 10);
        assertThat(topDocs.totalHits, equalTo(3));

        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        Explanation explanation = shardSearcher.explain(query, 2);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[0].score));
        assertThat(explanation.getDetails(), arrayWithSize(1));

        assertThat(topDocs.scoreDocs[1].doc, equalTo(1));
        explanation = shardSearcher.explain(query, 1);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[1].score));
        assertThat(explanation.getDetails(), arrayWithSize(1));

        assertThat(topDocs.scoreDocs[2].doc, equalTo(0));
        explanation = shardSearcher.explain(query, 0);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[2].score));
        assertThat(explanation.getDetails(), arrayWithSize(1));
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
            } else if (randomBoolean()) {
                query = new SpanTermQuery(new Term("field", id));
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
            duelRun(memoryIndex, shardSearcher);
        }
    }

    public void testDuelSpecificQueries() throws Exception {
        CommonTermsQuery commonTermsQuery = new CommonTermsQuery(BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD, 128);
        commonTermsQuery.add(new Term("field", "quick"));
        commonTermsQuery.add(new Term("field", "brown"));
        commonTermsQuery.add(new Term("field", "fox"));
        addPercolatorQuery("_id1", commonTermsQuery);

        BlendedTermQuery blendedTermQuery = BlendedTermQuery.booleanBlendedQuery(new Term[]{new Term("field", "quick"),
                new Term("field", "brown"), new Term("field", "fox")}, false);
        addPercolatorQuery("_id2", blendedTermQuery);

        SpanNearQuery spanNearQuery = new SpanNearQuery.Builder("field", true)
                .addClause(new SpanTermQuery(new Term("field", "quick")))
                .addClause(new SpanTermQuery(new Term("field", "brown")))
                .addClause(new SpanTermQuery(new Term("field", "fox")))
                .build();
        addPercolatorQuery("_id3", spanNearQuery);

        SpanNearQuery spanNearQuery2 = new SpanNearQuery.Builder("field", true)
                .addClause(new SpanTermQuery(new Term("field", "the")))
                .addClause(new SpanTermQuery(new Term("field", "lazy")))
                .addClause(new SpanTermQuery(new Term("field", "doc")))
                .build();
        SpanOrQuery spanOrQuery = new SpanOrQuery(
                spanNearQuery,
                spanNearQuery2
        );
        addPercolatorQuery("_id4", spanOrQuery);

        SpanNotQuery spanNotQuery = new SpanNotQuery(spanNearQuery, spanNearQuery);
        addPercolatorQuery("_id5", spanNotQuery);

        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        duelRun(memoryIndex, shardSearcher);
    }

    void addPercolatorQuery(String id, Query query, String... extraFields) throws IOException {
        queries.put(id, query);
        ParseContext.Document document = new ParseContext.Document();
        ExtractQueryTermsService.extractQueryTerms(query, document, EXTRACTED_TERMS_FIELD_NAME, UNKNOWN_QUERY_FIELD_NAME,
                EXTRACTED_TERMS_FIELD_TYPE);
        document.add(new StoredField(UidFieldMapper.NAME, Uid.createUid(MapperService.PERCOLATOR_LEGACY_TYPE_NAME, id)));
        assert extraFields.length % 2 == 0;
        for (int i = 0; i < extraFields.length; i++) {
            document.add(new StringField(extraFields[i], extraFields[++i], Field.Store.NO));
        }
        indexWriter.addDocument(document);
    }

    private void duelRun(MemoryIndex memoryIndex, IndexSearcher shardSearcher) throws IOException {
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        PercolateQuery.Builder builder1 = new PercolateQuery.Builder(
                "docType",
                queryStore,
                new BytesArray("{}"),
                percolateSearcher
        );
        // enables the optimization that prevents queries from being evaluated that don't match
        builder1.extractQueryTermsQuery(EXTRACTED_TERMS_FIELD_NAME, UNKNOWN_QUERY_FIELD_NAME);
        TopDocs topDocs1 = shardSearcher.search(builder1.build(), 10);

        PercolateQuery.Builder builder2 = new PercolateQuery.Builder(
                "docType",
                queryStore,
                new BytesArray("{}"),
                percolateSearcher
        );
        builder2.setPercolateTypeQuery(new MatchAllDocsQuery());
        TopDocs topDocs2 = shardSearcher.search(builder2.build(), 10);
        assertThat(topDocs1.totalHits, equalTo(topDocs2.totalHits));
        assertThat(topDocs1.scoreDocs.length, equalTo(topDocs2.scoreDocs.length));
        for (int j = 0; j < topDocs1.scoreDocs.length; j++) {
            assertThat(topDocs1.scoreDocs[j].doc, equalTo(topDocs2.scoreDocs[j].doc));
            assertThat(topDocs1.scoreDocs[j].score, equalTo(topDocs2.scoreDocs[j].score));
            Explanation explain1 = shardSearcher.explain(builder1.build(), topDocs1.scoreDocs[j].doc);
            Explanation explain2 = shardSearcher.explain(builder2.build(), topDocs2.scoreDocs[j].doc);
            assertThat(explain1.toHtml(), equalTo(explain2.toHtml()));
        }
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

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj);
        }

        @Override
        public int hashCode() {
            return classHash();
        }
    }

}
