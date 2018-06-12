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

package org.elasticsearch.common.lucene.search.morelikethis;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

public class XMoreLikeThisTests extends ESTestCase {
    private void addDoc(RandomIndexWriter writer, String[] texts) throws IOException {
        Document doc = new Document();
        for (String text : texts) {
            doc.add(newTextField("text", text, Field.Store.YES));
        }
        writer.addDocument(doc);
    }

    public void testTopN() throws Exception {
        int numDocs = 100;
        int topN = 25;

        // add series of docs with terms of decreasing df
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        for (int i = 0; i < numDocs; i++) {
            addDoc(writer, generateStrSeq(0, i + 1));
        }
        IndexReader reader = writer.getReader();
        writer.close();

        // setup MLT query
        MoreLikeThis mlt = new MoreLikeThis(reader);
        mlt.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
        mlt.setMaxQueryTerms(topN);
        mlt.setMinDocFreq(1);
        mlt.setMinTermFreq(1);
        mlt.setMinWordLen(1);
        mlt.setFieldNames(new String[]{"text"});

        // perform MLT query
        String likeText = "";
        for (String text : generateStrSeq(0, numDocs)) {
            likeText += text + " ";
        }
        BooleanQuery query = (BooleanQuery) mlt.like("text", new StringReader(likeText));

        // check best terms are topN of highest idf
        List<BooleanClause> clauses = query.clauses();
        assertEquals("Expected" + topN + "clauses only!", topN, clauses.size());

        Term[] expectedTerms = new Term[topN];
        int idx = 0;
        for (String text : generateStrSeq(numDocs - topN, topN)) {
            expectedTerms[idx++] = new Term("text", text);
        }
        for (BooleanClause clause : clauses) {
            Term term = ((TermQuery) clause.getQuery()).getTerm();
            assertTrue(Arrays.asList(expectedTerms).contains(term));
        }

        // clean up
        reader.close();
        dir.close();
    }

    private String[] generateStrSeq(int from, int size) {
        String[] generatedStrings = new String[size];
        for (int i = 0; i < generatedStrings.length; i++) {
            generatedStrings[i] = String.valueOf(from + i);
        }
        return generatedStrings;
    }

}
