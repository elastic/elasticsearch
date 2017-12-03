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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PercolateQueryTests extends ESTestCase {

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

    public void testPercolateQuery() throws Exception {
        List<Iterable<? extends IndexableField>> docs = new ArrayList<>();
        List<Query> queries = new ArrayList<>();
        PercolateQuery.QueryStore queryStore = ctx -> queries::get;

        queries.add(new TermQuery(new Term("field", "fox")));
        docs.add(Collections.singleton(new StringField("select", "a", Field.Store.NO)));

        SpanNearQuery.Builder snp = new SpanNearQuery.Builder("field", true);
        snp.addClause(new SpanTermQuery(new Term("field", "jumps")));
        snp.addClause(new SpanTermQuery(new Term("field", "lazy")));
        snp.addClause(new SpanTermQuery(new Term("field", "dog")));
        snp.setSlop(2);
        queries.add(snp.build());
        docs.add(Collections.singleton(new StringField("select", "b", Field.Store.NO)));

        PhraseQuery.Builder pq1 = new PhraseQuery.Builder();
        pq1.add(new Term("field", "quick"));
        pq1.add(new Term("field", "brown"));
        pq1.add(new Term("field", "jumps"));
        pq1.setSlop(1);
        queries.add(pq1.build());
        docs.add(Collections.singleton(new StringField("select", "b", Field.Store.NO)));

        BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
        bq1.add(new TermQuery(new Term("field", "quick")), BooleanClause.Occur.MUST);
        bq1.add(new TermQuery(new Term("field", "brown")), BooleanClause.Occur.MUST);
        bq1.add(new TermQuery(new Term("field", "fox")), BooleanClause.Occur.MUST);
        queries.add(bq1.build());
        docs.add(Collections.singleton(new StringField("select", "b", Field.Store.NO)));

        indexWriter.addDocuments(docs);
        indexWriter.close();
        directoryReader = DirectoryReader.open(directory);
        IndexSearcher shardSearcher = newSearcher(directoryReader);

        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "the quick brown fox jumps over the lazy dog", new WhitespaceAnalyzer());
        IndexSearcher percolateSearcher = memoryIndex.createSearcher();
        // no scoring, wrapping it in a constant score query:
        Query query = new ConstantScoreQuery(new PercolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("a")),
                new TermQuery(new Term("select", "a")), percolateSearcher, new MatchNoDocsQuery("")));
        TopDocs topDocs = shardSearcher.search(query, 10);
        assertThat(topDocs.totalHits, equalTo(1L));
        assertThat(topDocs.scoreDocs.length, equalTo(1));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        Explanation explanation = shardSearcher.explain(query, 0);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[0].score));

        query = new ConstantScoreQuery(new PercolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("b")),
                new TermQuery(new Term("select", "b")), percolateSearcher, new MatchNoDocsQuery("")));
        topDocs = shardSearcher.search(query, 10);
        assertThat(topDocs.totalHits, equalTo(3L));
        assertThat(topDocs.scoreDocs.length, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        explanation = shardSearcher.explain(query, 1);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[0].score));

        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        explanation = shardSearcher.explain(query, 2);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[1].score));

        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        explanation = shardSearcher.explain(query, 2);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[2].score));

        query = new ConstantScoreQuery(new PercolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("c")),
                new MatchAllDocsQuery(), percolateSearcher, new MatchAllDocsQuery()));
        topDocs = shardSearcher.search(query, 10);
        assertThat(topDocs.totalHits, equalTo(4L));

        query = new PercolateQuery("_name", queryStore, Collections.singletonList(new BytesArray("{}")),
            new TermQuery(new Term("select", "b")), percolateSearcher, new MatchNoDocsQuery(""));
        topDocs = shardSearcher.search(query, 10);
        assertThat(topDocs.totalHits, equalTo(3L));
        assertThat(topDocs.scoreDocs.length, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(3));
        explanation = shardSearcher.explain(query, 3);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[0].score));
        assertThat(explanation.getDetails(), arrayWithSize(1));

        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        explanation = shardSearcher.explain(query, 2);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[1].score));
        assertThat(explanation.getDetails(), arrayWithSize(1));

        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));
        explanation = shardSearcher.explain(query, 1);
        assertThat(explanation.isMatch(), is(true));
        assertThat(explanation.getValue(), equalTo(topDocs.scoreDocs[2].score));
        assertThat(explanation.getDetails(), arrayWithSize(1));
    }

}
