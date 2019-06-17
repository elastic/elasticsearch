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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

import java.io.IOException;

public final class EnglishPluralStemFilter extends TokenFilter {
    private final EnglishPluralStemmer stemmer = new EnglishPluralStemmer();
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

    public EnglishPluralStemFilter(TokenStream input) {
        super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            if (!keywordAttr.isKeyword()) {
                final int newlen = stemmer.stem(termAtt.buffer(), termAtt.length());
                termAtt.setLength(newlen);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Plural stemmer for English based on the {@link EnglishMinimalStemFilter}
     * <p>
     * This stemmer removes plurals but beyond EnglishMinimalStemFilter adds
     * four new suffix rules to remove dangling e characters:
     * <ul>
     * <li>xes - "boxes" becomes "box"</li>
     * <li>sses - "dresses" becomes "dress"</li>
     * <li>shes - "dishes" becomes "dish"</li>
     * <li>tches - "watches" becomes "watch"</li>
     * </ul>
     * See https://github.com/elastic/elasticsearch/issues/42892 
     * <p>
     * In addition the s stemmer logic is amended so that
     * <ul>
     * <li>ees-&gt;ee so that bees matches bee</li>
     * <li>ies-&gt;y only on longer words to that ties matches tie</li>
     * <li>oes-&gt;o rule so that tomatoes matches tomato but retains e for some words eg shoes to shoe</li>
     * </ul>
     */
    public static class EnglishPluralStemmer {
        
        // Words ending in oes that retain the e when stemmed 
        public static final char [][] oesExceptions = { 
                "shoes".toCharArray(), 
                "canoes".toCharArray(), 
                "oboes".toCharArray() 
                }; 
        
        @SuppressWarnings("fallthrough")
        public int stem(char s[], int len) {
            if (len < 3 || s[len - 1] != 's')
                return len;

            switch (s[len - 2]) {
            case 'u':
            case 's':
                return len;
            case 'e':
                // Modified ies->y logic from original s-stemmer - only work on strings > 4
                // so spies -> spy still but pies->pie.
                // The original code also special-cased aies and eies for no good reason as far as I can tell.
                // ( no words of consequence - eg http://www.thefreedictionary.com/words-that-end-in-aies )
                if (len > 4 && s[len - 3] == 'i') {
                    s[len - 3] = 'y';
                    return len - 2;
                }
                
                // Suffix rules to remove any dangling "e"                
                if (len > 3) {
                    // xes (but >1 prefix so we can stem "boxes->box" but keep "axes->axe")
                    if (len > 4 && s[len -3] == 'x') {
                        return len - 2;
                    }
                    // oes
                    if (len > 3 && s[len -3] == 'o') {
                        if (isOesException(s, len)) {
                            // Only remove the S
                            return len -1;
                        }
                        // Remove the es 
                        return len - 2;
                    }                    
                    if (len > 4) {
                        // shes/sses
                        if (s[len -4] == 's' && (s[len -3] == 'h' || s[len -3] == 's')){
                            return len - 2;
                        }
                        
                        // tches (TODO consider just ches? Gains: lunches == lunch, losses: moustaches!= moustache
                        if (len > 5) {
                            if (s[len -5] == 't' && s[len -4] == 'c' && s[len -3] == 'h' ){
                                return len - 2;
                            }                            
                        }                        
                    }
                }

            default:
                return len - 1;
            }
        }

        private final boolean isOesException(char[] s, int len) {
            for (char[] oesRule : oesExceptions) {
                int rulePos = oesRule.length - 1;
                int sPos = len - 1;
                boolean matched = true;
                while (rulePos >= 0 && sPos >= 0) {
                    if (oesRule[rulePos] != s[sPos]) {
                        matched = false;
                        break;
                    }
                    rulePos--;
                    sPos--;
                }
                if (matched) {
                    return true;
                }
            }
            return false;
        }
    }

}
