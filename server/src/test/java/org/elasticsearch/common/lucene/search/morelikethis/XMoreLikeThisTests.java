/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search.morelikethis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenFilter;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.lucene.search.XMoreLikeThis;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class XMoreLikeThisTests extends ESTestCase {
    private void addDoc(RandomIndexWriter writer, String... texts) throws IOException {
        Document doc = new Document();
        for (String text : texts) {
            doc.add(newTextField("text", text, Field.Store.YES));
        }
        writer.addDocument(doc);
    }

    // Copied from Lucene. See Lucene: https://issues.apache.org/jira/browse/LUCENE-8756
    public void testCustomFrequency() throws IOException {
        Analyzer analyzer = new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false, 100);
                MockTokenFilter filter = new MockTokenFilter(tokenizer, MockTokenFilter.EMPTY_STOPSET);
                return new TokenStreamComponents(tokenizer, addCustomTokenFilter(filter));
            }

            TokenStream addCustomTokenFilter(TokenStream input) {
                return new TokenFilter(input) {
                    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
                    final TermFrequencyAttribute tfAtt = addAttribute(TermFrequencyAttribute.class);

                    @Override
                    public boolean incrementToken() throws IOException {
                        if (input.incrementToken()) {
                            final char[] buffer = termAtt.buffer();
                            final int length = termAtt.length();
                            for (int i = 0; i < length; i++) {
                                if (buffer[i] == '|') {
                                    termAtt.setLength(i);
                                    i++;
                                    tfAtt.setTermFrequency(ArrayUtil.parseInt(buffer, i, length - i));
                                    return true;
                                }
                            }
                            return true;
                        }
                        return false;
                    }
                };
            }
        };

        Directory directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

        // Add series of docs with specific information for MoreLikeThis
        addDoc(writer, "text", "lucene");
        addDoc(writer, "text", "lucene release");
        addDoc(writer, "text", "apache");
        addDoc(writer, "text", "apache lucene");

        // one more time to increase the doc frequencies
        addDoc(writer, "text", "lucene2");
        addDoc(writer, "text", "lucene2 release2");
        addDoc(writer, "text", "apache2");
        addDoc(writer, "text", "apache2 lucene2");

        addDoc(writer, "text2", "lucene2");
        addDoc(writer, "text2", "lucene2 release2");
        addDoc(writer, "text2", "apache2");
        addDoc(writer, "text2", "apache2 lucene2");

        IndexReader reader = writer.getReader();
        writer.close();
        XMoreLikeThis mlt = new XMoreLikeThis(reader, new ClassicSimilarity());
        mlt.setMinDocFreq(0);
        mlt.setMinTermFreq(1);
        mlt.setMinWordLen(1);
        mlt.setAnalyzer(analyzer);
        mlt.setFieldNames(new String[] { "text" });
        mlt.setBoost(true);

        final double boost10 = ((BooleanQuery) mlt.like("text", new StringReader("lucene|10 release|1"))).clauses()
            .stream()
            .map(BooleanClause::query)
            .map(BoostQuery.class::cast)
            .filter(x -> ((TermQuery) x.getQuery()).getTerm().text().equals("lucene"))
            .mapToDouble(BoostQuery::getBoost)
            .sum();

        final double boost1 = ((BooleanQuery) mlt.like("text", new StringReader("lucene|1 release|1"))).clauses()
            .stream()
            .map(BooleanClause::query)
            .map(BoostQuery.class::cast)
            .filter(x -> ((TermQuery) x.getQuery()).getTerm().text().equals("lucene"))
            .mapToDouble(BoostQuery::getBoost)
            .sum();

        // mlt should use the custom frequencies provided by the analyzer so "lucene|10" should be
        // boosted more than "lucene|1"
        assertTrue(String.format(Locale.ROOT, "%s should be greater than %s", boost10, boost1), boost10 > boost1);
        analyzer.close();
        reader.close();
        directory.close();
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
        mlt.setFieldNames(new String[] { "text" });

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
            Term term = ((TermQuery) clause.query()).getTerm();
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
