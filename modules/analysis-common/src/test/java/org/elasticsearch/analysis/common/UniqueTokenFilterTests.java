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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
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
