/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.vectorhighlight.CustomFieldQuery;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.testng.annotations.Test;

import static org.elasticsearch.common.lucene.DocumentBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class VectorHighlighterTests {

    @Test public void testVectorHighlighter() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        indexWriter.addDocument(doc().add(field("_id", "1")).add(field("content", "the big bad dog", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS)).build());

        IndexReader reader = IndexReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits, equalTo(1));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, notNullValue());
        System.out.println(fragment);
    }

    @Test public void testVectorHighlighterPrefixQuery() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        indexWriter.addDocument(doc().add(field("_id", "1")).add(field("content", "the big bad dog", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS)).build());

        IndexReader reader = IndexReader.open(indexWriter, true);
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

        System.out.println(fragment);

        // now check with the custom field query
        prefixQuery = new PrefixQuery(new Term("content", "ba"));
        assertThat(prefixQuery.getRewriteMethod().getClass().getName(), equalTo(PrefixQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT.getClass().getName()));
        CustomFieldQuery.reader.set(reader);
        fragment = highlighter.getBestFragment(new CustomFieldQuery(prefixQuery, highlighter),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, notNullValue());

        System.out.println(fragment);
    }

    @Test public void testVectorHighlighterNoStore() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        indexWriter.addDocument(doc().add(field("_id", "1")).add(field("content", "the big bad dog", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS)).build());

        IndexReader reader = IndexReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits, equalTo(1));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, nullValue());
    }

    @Test public void testVectorHighlighterNoTermVector() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        indexWriter.addDocument(doc().add(field("_id", "1")).add(field("content", "the big bad dog", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO)).build());

        IndexReader reader = IndexReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);

        assertThat(topDocs.totalHits, equalTo(1));

        FastVectorHighlighter highlighter = new FastVectorHighlighter();
        String fragment = highlighter.getBestFragment(highlighter.getFieldQuery(new TermQuery(new Term("content", "bad"))),
                reader, topDocs.scoreDocs[0].doc, "content", 30);
        assertThat(fragment, nullValue());
    }
}
