/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;

public class StringAccentTokenFilterTests extends ESTestCase {

    public void testStripAccents() throws Exception {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(t, new StripAccentTokenFilter(t));
            }
        };

        TokenStream test = analyzer.tokenStream("test", "HäLLo how are you");
        test.reset();
        CharTermAttribute term = test.addAttribute(CharTermAttribute.class);
        List<String> luceneTokens = new ArrayList<>();
        while (test.incrementToken()) {
            luceneTokens.add(term.toString());
        }
        assertThat(luceneTokens, contains("HaLLo", "how", "are", "you"));
    }

}
