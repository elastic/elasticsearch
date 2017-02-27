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

package org.apache.lucene.search.suggest.document;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.search.suggest.filteredsuggest.FilteredSuggestSuggestionBuilder;
import org.elasticsearch.search.suggest.filteredsuggest.filter.FilteredSuggestFilterValues;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.DEFAULT_PRESERVE_POSITION_INCREMENTS;
import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.DEFAULT_PRESERVE_SEP;

// TODO This class will be removed once the maximum expansion is allowed to be passed from subclasses.
public class FilteredSuggestField extends ContextSuggestField {
    private final Map<String, FilteredSuggestFilterValues> filters;
    private final ParseContext.Document document;

    public FilteredSuggestField(String name, String value, int weight, Map<String, FilteredSuggestFilterValues> filters,
            ParseContext.Document document) {
        super(name, value, weight);
        this.filters = filters;
        this.document = document;
    }

    @Override
    protected Iterable<CharSequence> contexts() {
        Set<CharSequence> filterValues = new HashSet<>();

        for (FilteredSuggestFilterValues context : filters.values()) {
            context.generate(document, filterValues);
        }

        // Empty filter value for empty filter query
        filterValues.add(FilteredSuggestSuggestionBuilder.EMPTY_FILTER_FILLER);

        return filterValues;
    }

    @Override
    protected CompletionTokenStream wrapTokenStream(TokenStream stream) {
        final Iterable<CharSequence> contexts = contexts();
        for (CharSequence context : contexts) {
            validate(context);
        }
        CompletionTokenStream completionTokenStream;
        if (stream instanceof CompletionTokenStream) {
            completionTokenStream = (CompletionTokenStream) stream;
            PrefixTokenFilter prefixTokenFilter = new PrefixTokenFilter(completionTokenStream.inputTokenStream, (char) CONTEXT_SEPARATOR,
                    contexts);
            completionTokenStream = new CompletionTokenStream(prefixTokenFilter, completionTokenStream.preserveSep,
                    completionTokenStream.preservePositionIncrements, completionTokenStream.maxGraphExpansions);
        } else {
            completionTokenStream = new CompletionTokenStream(new PrefixTokenFilter(stream, (char) CONTEXT_SEPARATOR, contexts),
                    DEFAULT_PRESERVE_SEP, DEFAULT_PRESERVE_POSITION_INCREMENTS, 200000);
        }
        return completionTokenStream;
    }

    /**
     * The {@link PrefixTokenFilter} wraps a {@link TokenStream} and adds a set prefixes ahead. The position attribute
     * will not be incremented for the prefixes.
     */
    private static final class PrefixTokenFilter extends TokenFilter {

        private final char separator;
        private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
        private final PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
        private final Iterable<CharSequence> prefixes;

        private Iterator<CharSequence> currentPrefix;

        /**
         * Create a new {@link PrefixTokenFilter}
         *
         * @param input
         *            {@link TokenStream} to wrap
         * @param separator
         *            Character used separate prefixes from other tokens
         * @param prefixes
         *            {@link Iterable} of {@link CharSequence} which keeps all prefixes
         */
        PrefixTokenFilter(TokenStream input, char separator, Iterable<CharSequence> prefixes) {
            super(input);
            this.prefixes = prefixes;
            this.currentPrefix = null;
            this.separator = separator;
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
            }
            termAttr.setEmpty();
            if (currentPrefix.hasNext()) {
                termAttr.append(currentPrefix.next());
            }
            termAttr.append(separator);
            return true;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            currentPrefix = null;
        }
    }

    private void validate(final CharSequence value) {
        for (int i = 0; i < value.length(); i++) {
            if (CONTEXT_SEPARATOR == value.charAt(i)) {
                throw new IllegalArgumentException("Illegal value [" + value + "] UTF-16 codepoint [0x"
                        + Integer.toHexString((int) value.charAt(i)) + "] at position " + i + " is a reserved character");
            }
        }
    }
}
