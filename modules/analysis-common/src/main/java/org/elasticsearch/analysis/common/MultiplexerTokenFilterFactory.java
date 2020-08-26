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
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilter;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilter;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class MultiplexerTokenFilterFactory extends AbstractTokenFilterFactory {

    private List<String> filterNames;
    private final boolean preserveOriginal;

    public MultiplexerTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);
        this.filterNames = settings.getAsList("filters");
        this.preserveOriginal = settings.getAsBoolean("preserve_original", true);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new UnsupportedOperationException("TokenFilterFactory.getChainAwareTokenFilterFactory() must be called first");
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
    }

    @Override
    public TokenFilterFactory getChainAwareTokenFilterFactory(TokenizerFactory tokenizer, List<CharFilterFactory> charFilters,
                                                              List<TokenFilterFactory> previousTokenFilters,
                                                              Function<String, TokenFilterFactory> allFilters) {
        List<TokenFilterFactory> filters = new ArrayList<>();
        if (preserveOriginal) {
            filters.add(IDENTITY_FILTER);
        }
        // also merge and transfer token filter analysis modes with analyzer
        AnalysisMode mode = AnalysisMode.ALL;
        for (String filter : filterNames) {
            String[] parts = Strings.tokenizeToStringArray(filter, ",");
            if (parts.length == 1) {
                TokenFilterFactory factory = resolveFilterFactory(allFilters, parts[0]);
                factory = factory.getChainAwareTokenFilterFactory(tokenizer, charFilters, previousTokenFilters, allFilters);
                filters.add(factory);
                mode = mode.merge(factory.getAnalysisMode());
            } else {
                List<TokenFilterFactory> existingChain = new ArrayList<>(previousTokenFilters);
                List<TokenFilterFactory> chain = new ArrayList<>();
                for (String subfilter : parts) {
                    TokenFilterFactory factory = resolveFilterFactory(allFilters, subfilter);
                    factory = factory.getChainAwareTokenFilterFactory(tokenizer, charFilters, existingChain, allFilters);
                    chain.add(factory);
                    existingChain.add(factory);
                    mode = mode.merge(factory.getAnalysisMode());
                }
                filters.add(chainFilters(filter, chain));
            }
        }
        final AnalysisMode analysisMode = mode;

        return new TokenFilterFactory() {
            @Override
            public String name() {
                return MultiplexerTokenFilterFactory.this.name();
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                List<Function<TokenStream, TokenStream>> functions = new ArrayList<>();
                for (TokenFilterFactory tff : filters) {
                    functions.add(tff::create);
                }
                return new RemoveDuplicatesTokenFilter(new MultiplexTokenFilter(tokenStream, functions));
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }
        };
    }

    private TokenFilterFactory chainFilters(String name, List<TokenFilterFactory> filters) {
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                for (TokenFilterFactory tff : filters) {
                    tokenStream = tff.create(tokenStream);
                }
                return tokenStream;
            }
        };
    }

    private TokenFilterFactory resolveFilterFactory(Function<String, TokenFilterFactory> factories, String name) {
        TokenFilterFactory factory = factories.apply(name);
        if (factory == null) {
            throw new IllegalArgumentException("Multiplexing filter [" + name() + "] refers to undefined tokenfilter [" + name + "]");
        } else {
            return factory;
        }
    }

    private final class MultiplexTokenFilter extends TokenFilter {

        private final TokenStream source;
        private final int filterCount;

        private int selector;

        /**
         * Creates a MultiplexTokenFilter on the given input with a set of filters
         */
        MultiplexTokenFilter(TokenStream input, List<Function<TokenStream, TokenStream>> filters) {
            super(input);
            TokenStream source = new MultiplexerFilter(input);
            for (int i = 0; i < filters.size(); i++) {
                final int slot = i;
                source = new ConditionalTokenFilter(source, filters.get(i)) {
                    @Override
                    protected boolean shouldFilter() {
                        return slot == selector;
                    }
                };
            }
            this.source = source;
            this.filterCount = filters.size();
            this.selector = filterCount - 1;
        }

        @Override
        public boolean incrementToken() throws IOException {
            return source.incrementToken();
        }

        @Override
        public void end() throws IOException {
            source.end();
        }

        @Override
        public void reset() throws IOException {
            source.reset();
        }

        private final class MultiplexerFilter extends TokenFilter {

            State state;
            PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

            private MultiplexerFilter(TokenStream input) {
                super(input);
            }

            @Override
            public boolean incrementToken() throws IOException {
                if (selector >= filterCount - 1) {
                    selector = 0;
                    if (input.incrementToken() == false) {
                        return false;
                    }
                    state = captureState();
                    return true;
                }
                restoreState(state);
                posIncAtt.setPositionIncrement(0);
                selector++;
                return true;
            }

            @Override
            public void reset() throws IOException {
                super.reset();
                selector = filterCount - 1;
                this.state = null;
            }
        }

    }
}
