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

package org.elasticsearch.deps.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.vectorhighlight.CustomFieldQuery;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class VectorHighlighterTests extends ElasticsearchTestCase {

    @Test
    public void testVectorHighlighter() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new Field("content", "the big bad dog", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits, equalTo(1));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, notNullValue());
        assertThat(fragment, equalTo("the big <b>bad</b> dog"));
    }

    @Test
    public void testVectorHighlighterPrefixQuery() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new Field("content", "the big bad dog", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits, equalTo(1));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();

        PrefixQuery prefixQuery = new PrefixQuery(new Term("content", "ba"));
        assertThat(prefixQuery.getRewriteMethod().getClass().getName(), equalTo(PrefixQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT.getClass().getName()));
        String fragment = highlighter.getBestFragment(highlighter.getFieldQuery(prefixQuery),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, nullValue());

        prefixQuery.setRewriteMethod(PrefixQuery.SCORING_BOOLEAN_QUERY_REWRITE);
        Query rewriteQuery = prefixQuery.rewrite(reader);
        fragment = highlighter.getBestFragment(highlighter.getFieldQuery(rewriteQuery),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, notNullValue());

        // now check with the custom field query
        prefixQuery = new PrefixQuery(new Term("content", "ba"));
        assertThat(prefixQuery.getRewriteMethod().getClass().getName(), equalTo(PrefixQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT.getClass().getName()));
        fragment = highlighter.getBestFragment(new CustomFieldQuery(prefixQuery, reader, highlighter),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, notNullValue());
    }

    @Test
    public void testVectorHighlighterNoStore() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new Field("content", "the big bad dog", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits, equalTo(1));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, nullValue());
    }

    @Test
    public void testVectorHighlighterNoTermVector() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new Field("content", "the big bad dog", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits, equalTo(1));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, nullValue());
    }
}
