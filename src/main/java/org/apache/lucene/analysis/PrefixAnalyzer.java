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

package org.apache.lucene.analysis;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * This {@link Analyzer} wraps another analyzer and adds a set of prefixes to the
 * underlying TokenStream. While these prefixes are iterated the position attribute
 * will not be incremented. Also each prefix will be separated from the other tokens
 * by a separator character.
 * NOTE: The sequence of prefixes needs to be not empty 
 */
public class PrefixAnalyzer extends Analyzer {

    private final char separator;
    private final Iterable<? extends CharSequence> prefix;
    private final Analyzer analyzer;

    /**
     * Create a new {@link PrefixAnalyzer}. The separator will be set to the DEFAULT_SEPARATOR.
     * 
     * @param analyzer {@link Analyzer} to wrap
     * @param prefix Single prefix 
     */
    public PrefixAnalyzer(Analyzer analyzer, char separator, CharSequence prefix) {
        this(analyzer, separator, Collections.singleton(prefix));
    }

    /**
     * Create a new {@link PrefixAnalyzer}. The separator will be set to the DEFAULT_SEPARATOR.
     * 
     * @param analyzer {@link Analyzer} to wrap
     * @param prefix {@link Iterable} of {@link CharSequence} which keeps all prefixes 
     */
    public PrefixAnalyzer(Analyzer analyzer, char separator, Iterable<? extends CharSequence> prefix) {
        super();
        this.analyzer = analyzer;
        this.prefix = prefix;
        this.separator = separator;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        TokenStreamComponents createComponents = analyzer.createComponents(fieldName);
        TokenStream stream = new PrefixTokenFilter(createComponents.getTokenStream(), separator, prefix);
        TokenStreamComponents tsc = new TokenStreamComponents(createComponents.getTokenizer(), stream);
        return tsc;
    }

    /**
     * The {@link PrefixTokenFilter} wraps a {@link TokenStream} and adds a set
     * prefixes ahead. The position attribute will not be incremented for the prefixes.
     */
    public static final class PrefixTokenFilter extends TokenFilter {

        private final char separator;
        private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
        private final PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
        private final Iterable<? extends CharSequence> prefixes;

        private Iterator<? extends CharSequence> currentPrefix;

        /**
         * Create a new {@link PrefixTokenFilter}. The separator will be set to the DEFAULT_SEPARATOR.
         * 
         * @param input {@link TokenStream} to wrap
         * @param separator Character used separate prefixes from other tokens
         * @param prefixes {@link Iterable} of {@link CharSequence} which keeps all prefixes 
         */
        public PrefixTokenFilter(TokenStream input, char separator, Iterable<? extends CharSequence> prefixes) {
            super(input);
            this.prefixes = prefixes;
            this.currentPrefix = null;
            this.separator = separator;
            if (prefixes == null || !prefixes.iterator().hasNext()) {
                throw new ElasticsearchIllegalArgumentException("one or more prefixes needed");
            }
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (currentPrefix != null) {
                if (!currentPrefix.hasNext()) {
                    return input.incrementToken();
                } else {
                    posAttr.setPositionIncrement(0);
                }
            } else {
                currentPrefix = prefixes.iterator();
                termAttr.setEmpty();
                posAttr.setPositionIncrement(1);
                assert (currentPrefix.hasNext()) : "one or more prefixes needed";
            }
            termAttr.setEmpty();
            termAttr.append(currentPrefix.next());
            termAttr.append(separator);
            return true;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            currentPrefix = null;
        }
    }
}
