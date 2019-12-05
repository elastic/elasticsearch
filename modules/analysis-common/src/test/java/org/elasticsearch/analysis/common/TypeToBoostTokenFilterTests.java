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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.TokenBoostAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

public class TypeToBoostTokenFilterTests extends ESTestCase {

    public void testSimple() throws IOException {
        String tokenType = "MY_TYPE";
        float boost = randomFloat();

        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                final Tokenizer source = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                // mock filte applies custom token type to all-uppercase tokens
                final TokenStream addTypeFilter = new TokenFilter(source) {

                    TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
                    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

                    @Override
                    public boolean incrementToken() throws IOException {
                        if (input.incrementToken()) {
                            String token = termAtt.toString();
                            if (token.toUpperCase(Locale.ROOT).equals(token)) {
                                typeAtt.setType(tokenType);
                            }
                            return true;
                        }
                        return false;
                    }
                };
                return new TokenStreamComponents(source, new TypeToBoostTokenFilter(addTypeFilter, tokenType, boost));
            }
        };

        TokenStream test = analyzer.tokenStream("test", "one TWO three FOUR five");
        test.reset();
        CharTermAttribute termAttribute = test.addAttribute(CharTermAttribute.class);
        TokenBoostAttribute boostAttribut = test.addAttribute(TokenBoostAttribute.class);
        while (test.incrementToken()) {
            String term = termAttribute.toString();
            if (term.toUpperCase(Locale.ROOT).equals(term)) {
                assertEquals(boost, boostAttribut.getBoost(), Float.MIN_VALUE);
            } else {
                assertEquals(1f, boostAttribut.getBoost(), Float.MIN_VALUE);
            }
        }
    }
}
