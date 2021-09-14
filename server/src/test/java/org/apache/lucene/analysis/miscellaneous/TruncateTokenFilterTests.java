/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TruncateTokenFilterTests extends ESTestCase {
    public void testSimple() throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(t, new TruncateTokenFilter(t, 3));
            }
        };

        TokenStream test = analyzer.tokenStream("test", "a bb ccc dddd eeeee");
        test.reset();
        CharTermAttribute termAttribute = test.addAttribute(CharTermAttribute.class);
        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("a"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("bb"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("ccc"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("ddd"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("eee"));

        assertThat(test.incrementToken(), equalTo(false));
    }
}
