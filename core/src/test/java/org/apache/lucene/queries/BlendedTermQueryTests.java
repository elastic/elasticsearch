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
package org.apache.lucene.queries;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class BlendedTermQueryTests extends ESTestCase {
    public void testBooleanQuery() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        String[] firstNames = new String[]{
                "simon", "paul"
        };
        String[] surNames = new String[]{
                "willnauer", "simon"
        };
        for (int i = 0; i < surNames.length; i++) {
            Document d = new Document();
            d.add(new TextField("id", Integer.toString(i), Field.Store.YES));
            d.add(new TextField("firstname", firstNames[i], Field.Store.NO));
            d.add(new TextField("surname", surNames[i], Field.Store.NO));
            w.addDocument(d);
        }
        int iters = scaledRandomIntBetween(25, 100);
        for (int j = 0; j < iters; j++) {
            Document d = new Document();
            d.add(new TextField("id", Integer.toString(firstNames.length + j), Field.Store.YES));
            d.add(new TextField("firstname", rarely() ? "some_other_name" :
                    "simon the sorcerer", Field.Store.NO)); // make sure length-norm is the tie-breaker
            d.add(new TextField("surname", "bogus", Field.Store.NO));
            w.addDocument(d);
        }
        w.commit();
        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = setSimilarity(newSearcher(reader));

        {
            Term[] terms = new Term[]{new Term("firstname", "simon"), new Term("surname", "simon")};
            BlendedTermQuery query = BlendedTermQuery.booleanBlendedQuery(terms, true);
            TopDocs search = searcher.search(query, 3);
            ScoreDoc[] scoreDocs = search.scoreDocs;
            assertEquals(3, scoreDocs.length);
            assertEquals(Integer.toString(0), reader.document(scoreDocs[0].doc).getField("id").stringValue());
        }
        {
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            query.setDisableCoord(true);
            query.add(new TermQuery(new Term("firstname", "simon")), BooleanClause.Occur.SHOULD);
            query.add(new TermQuery(new Term("surname", "simon")), BooleanClause.Occur.SHOULD);
            TopDocs search = searcher.search(query.build(), 1);
            ScoreDoc[] scoreDocs = search.scoreDocs;
            assertEquals(Integer.toString(1), reader.document(scoreDocs[0].doc).getField("id").stringValue());

        }
        reader.close();
        w.close();
        dir.close();

    }

    public void testDismaxQuery() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        String[] username = new String[]{
                "foo fighters", "some cool fan", "cover band"};
        String[] song = new String[]{
                "generator", "foo fighers - generator", "foo fighters generator"
        };
        final boolean omitNorms = random().nextBoolean();
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(omitNorms);
        ft.freeze();

        FieldType ft1 = new FieldType(TextField.TYPE_NOT_STORED);
        ft1.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft1.setOmitNorms(omitNorms);
        ft1.freeze();
        for (int i = 0; i < username.length; i++) {
            Document d = new Document();
            d.add(new TextField("id", Integer.toString(i), Field.Store.YES));
            d.add(new Field("username", username[i], ft));
            d.add(new Field("song", song[i], ft));
            w.addDocument(d);
        }
        int iters = scaledRandomIntBetween(25, 100);
        for (int j = 0; j < iters; j++) {
            Document d = new Document();
            d.add(new TextField("id", Integer.toString(username.length + j), Field.Store.YES));
            d.add(new Field("username", "foo fighters", ft1));
            d.add(new Field("song", "some bogus text to bump up IDF", ft1));
            w.addDocument(d);
        }
        w.commit();
        DirectoryReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = setSimilarity(newSearcher(reader));
        {
            String[] fields = new String[]{"username", "song"};
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            query.setDisableCoord(true);
            query.add(BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "foo"), 0.1f), BooleanClause.Occur.SHOULD);
            query.add(BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "fighters"), 0.1f), BooleanClause.Occur.SHOULD);
            query.add(BlendedTermQuery.dismaxBlendedQuery(toTerms(fields, "generator"), 0.1f), BooleanClause.Occur.SHOULD);
            TopDocs search = searcher.search(query.build(), 10);
            ScoreDoc[] scoreDocs = search.scoreDocs;
            assertEquals(Integer.toString(0), reader.document(scoreDocs[0].doc).getField("id").stringValue());
        }
        {
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            query.setDisableCoord(true);
            DisjunctionMaxQuery uname = new DisjunctionMaxQuery(
                    Arrays.asList(new TermQuery(new Term("username", "foo")), new TermQuery(new Term("song", "foo"))), 0.0f);

            DisjunctionMaxQuery s = new DisjunctionMaxQuery(
                    Arrays.asList(new TermQuery(new Term("username", "fighers")), new TermQuery(new Term("song", "fighers"))), 0.0f);
            DisjunctionMaxQuery gen = new DisjunctionMaxQuery(
                    Arrays.asList(new TermQuery(new Term("username", "generator")), new TermQuery(new Term("song", "generator"))), 0f);
            query.add(uname, BooleanClause.Occur.SHOULD);
            query.add(s, BooleanClause.Occur.SHOULD);
            query.add(gen, BooleanClause.Occur.SHOULD);
            TopDocs search = searcher.search(query.build(), 4);
            ScoreDoc[] scoreDocs = search.scoreDocs;
            assertEquals(Integer.toString(1), reader.document(scoreDocs[0].doc).getField("id").stringValue());

        }
        reader.close();
        w.close();
        dir.close();
    }

    public void testBasics() {
        final int iters = scaledRandomIntBetween(5, 25);
        for (int j = 0; j < iters; j++) {
            String[] fields = new String[1 + random().nextInt(10)];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = TestUtil.randomRealisticUnicodeString(random(), 1, 10);
            }
            String term = TestUtil.randomRealisticUnicodeString(random(), 1, 10);
            Term[] terms = toTerms(fields, term);
            boolean disableCoord = random().nextBoolean();
            boolean useBoolean = random().nextBoolean();
            float tieBreaker = random().nextFloat();
            BlendedTermQuery query = useBoolean ? BlendedTermQuery.booleanBlendedQuery(terms, disableCoord) : BlendedTermQuery.dismaxBlendedQuery(terms, tieBreaker);
            QueryUtils.check(query);
            terms = toTerms(fields, term);
            BlendedTermQuery query2 = useBoolean ? BlendedTermQuery.booleanBlendedQuery(terms, disableCoord) : BlendedTermQuery.dismaxBlendedQuery(terms, tieBreaker);
            assertEquals(query, query2);
        }
    }

    public Term[] toTerms(String[] fields, String term) {
        Term[] terms = new Term[fields.length];
        List<String> fieldsList = Arrays.asList(fields);
        Collections.shuffle(fieldsList, random());
        fields = fieldsList.toArray(new String[0]);
        for (int i = 0; i < fields.length; i++) {
            terms[i] = new Term(fields[i], term);
        }
        return terms;
    }

    public IndexSearcher setSimilarity(IndexSearcher searcher) {
        Similarity similarity = random().nextBoolean() ? new BM25Similarity() : new ClassicSimilarity();
        searcher.setSimilarity(similarity);
        return searcher;
    }

    public void testExtractTerms() throws IOException {
        Set<Term> terms = new HashSet<>();
        int num = scaledRandomIntBetween(1, 10);
        for (int i = 0; i < num; i++) {
            terms.add(new Term(TestUtil.randomRealisticUnicodeString(random(), 1, 10), TestUtil.randomRealisticUnicodeString(random(), 1, 10)));
        }

        BlendedTermQuery blendedTermQuery = random().nextBoolean() ? BlendedTermQuery.dismaxBlendedQuery(terms.toArray(new Term[0]), random().nextFloat()) :
                BlendedTermQuery.booleanBlendedQuery(terms.toArray(new Term[0]), random().nextBoolean());
        Set<Term> extracted = new HashSet<>();
        IndexSearcher searcher = new IndexSearcher(new MultiReader());
        searcher.createNormalizedWeight(blendedTermQuery, false).extractTerms(extracted);
        assertThat(extracted.size(), equalTo(terms.size()));
        assertThat(extracted, containsInAnyOrder(terms.toArray(new Term[0])));
    }
}
