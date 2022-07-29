/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;

public class MockRepeatAnalyzer extends Analyzer {
    private static class MockRepeatFilter extends TokenFilter {
        CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
        String repeat;

        MockRepeatFilter(TokenStream input) {
            super(input);
        }

        @Override
        public final boolean incrementToken() throws IOException {
            if (repeat != null) {
                // add repeat token
                clearAttributes();
                termAtt.setEmpty().append(repeat);
                posIncAtt.setPositionIncrement(0);
                repeat = null;
                return true;
            }

            if (input.incrementToken()) {
                repeat = termAtt.toString();
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new StandardTokenizer();
        TokenStream repeatFilter = new MockRepeatFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, repeatFilter);
    }
}
