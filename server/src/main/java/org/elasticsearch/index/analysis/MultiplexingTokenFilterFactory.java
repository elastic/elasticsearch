package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilter;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MultiplexingTokenFilterFactory extends AbstractTokenFilterFactory {

    private List<TokenFilterFactory> filters;
    private List<String> filterNames;

    private static final TokenFilterFactory IDENTITY_FACTORY = new TokenFilterFactory() {
        @Override
        public String name() {
            return "identity";
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return tokenStream;
        }
    };

    public MultiplexingTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);
        this.filterNames = settings.getAsList("filters");
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        List<Function<TokenStream, TokenStream>> functions = new ArrayList<>();
        for (TokenFilterFactory tff : filters) {
            functions.add(tff::create);
        }
        return new MultiplexTokenFilter(tokenStream, functions);
    }

    public void buildFilters(Map<String, TokenFilterFactory> factories) {
        filters = new ArrayList<>();
        for (String filter : filterNames) {
            String[] parts = Strings.tokenizeToStringArray(filter, ",");
            if (parts.length == 1) {
                filters.add(resolveFilterFactory(factories, parts[0]));
            }
            else {
                List<TokenFilterFactory> chain = new ArrayList<>();
                for (String subfilter : parts) {
                    chain.add(resolveFilterFactory(factories, subfilter));
                }
                filters.add(chainFilters(filter, chain));
            }
        }
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

    private TokenFilterFactory resolveFilterFactory(Map<String, TokenFilterFactory> factories, String name) {
        if ("identity".equals(name)) {
            return IDENTITY_FACTORY;
        }
        else if (factories.containsKey(name) == false) {
            throw new IllegalArgumentException("Multiplexing filter [" + name() + "] refers to undefined tokenfilter [" + name + "]");
        }
        else {
            return factories.get(name);
        }
    }

    private final class MultiplexTokenFilter extends TokenFilter {

        private final TokenStream source;
        private final int filterCount;

        private int selector;

        /**
         * Creates a MultiplexTokenFilter on the given input with a set of filters
         */
        public MultiplexTokenFilter(TokenStream input, List<Function<TokenStream, TokenStream>> filters) {
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
                    state = null;
                    selector = 0;
                }
                if (state == null) {
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
                selector = 0;
                this.state = null;
            }
        }

    }
}
