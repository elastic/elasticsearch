/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.hamcrest.Matchers.equalTo;

public class UniqueTokenFilterTests extends ESTestCase {
    public void testSimple() throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(t, new UniqueTokenFilter(t));
            }
        };

        TokenStream test = analyzer.tokenStream("test", "this test with test");
        test.reset();
        CharTermAttribute termAttribute = test.addAttribute(CharTermAttribute.class);
        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("this"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("test"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("with"));

        assertThat(test.incrementToken(), equalTo(false));
    }

    public void testOnlyOnSamePosition() throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(t, new UniqueTokenFilter(t, true));
            }
        };

        TokenStream test = analyzer.tokenStream("test", "this test with test");
        test.reset();
        CharTermAttribute termAttribute = test.addAttribute(CharTermAttribute.class);
        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("this"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("test"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("with"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("test"));

        assertThat(test.incrementToken(), equalTo(false));
    }

    public void testPositionIncrement() throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                TokenFilter filters = new EdgeNGramTokenFilter(tokenizer, 1, 3, false);
                filters = new UniqueTokenFilter(filters);
                return new TokenStreamComponents(tokenizer, filters);
            }
        };

        assertAnalyzesTo(analyzer, "foo bar bro bar bro baz",
                new String[]{"f", "fo", "foo", "b", "ba", "bar", "br", "bro", "baz"},
                new int[]{0, 0, 0, 4, 4, 4, 8, 8, 20},
                new int[]{3, 3, 3, 7, 7, 7, 11, 11, 23},
                new int[]{1, 0, 0, 1, 0, 0, 1, 0, 3});
        analyzer.close();
    }
}
