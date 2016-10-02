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

        public MockRepeatFilter(TokenStream input) {
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
