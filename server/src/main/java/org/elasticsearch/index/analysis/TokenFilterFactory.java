/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.search.fetch.subphase.highlight.FastVectorHighlighter;

import java.util.List;
import java.util.function.Function;

public interface TokenFilterFactory {
    String name();

    TokenStream create(TokenStream tokenStream);

    /**
     * Normalize a tokenStream for use in multi-term queries
     *
     * The default implementation is a no-op
     */
    default TokenStream normalize(TokenStream tokenStream) {
        return tokenStream;
    }

    /**
     * Does this analyzer mess up the {@link OffsetAttribute}s in such as way as to break the
     * {@link FastVectorHighlighter}? If this is {@code true} then the
     * {@linkplain FastVectorHighlighter} will attempt to work around the broken offsets.
     */
    default boolean breaksFastVectorHighlighter() {
        return false;
    }

    /**
     * Rewrite the TokenFilterFactory to take into account the preceding analysis chain, or refer
     * to other TokenFilterFactories
     * @param tokenizer             the TokenizerFactory for the preceding chain
     * @param charFilters           any CharFilterFactories for the preceding chain
     * @param previousTokenFilters  a list of TokenFilterFactories in the preceding chain
     * @param allFilters            access to previously defined TokenFilterFactories
     */
    default TokenFilterFactory getChainAwareTokenFilterFactory(TokenizerFactory tokenizer, List<CharFilterFactory> charFilters,
                                                               List<TokenFilterFactory> previousTokenFilters,
                                                               Function<String, TokenFilterFactory> allFilters) {
        return this;
    }

    /**
     * Return a version of this TokenFilterFactory appropriate for synonym parsing
     *
     * Filters that should not be applied to synonyms (for example, those that produce
     * multiple tokens) should throw an exception
     *
     */
    default TokenFilterFactory getSynonymFilter() {
        return this;
    }

    /**
     * Get the {@link AnalysisMode} this filter is allowed to be used in. The default is
     * {@link AnalysisMode#ALL}. Instances need to override this method to define their
     * own restrictions.
     */
    default AnalysisMode getAnalysisMode() {
        return AnalysisMode.ALL;
    }

    /**
     * A TokenFilterFactory that does no filtering to its TokenStream
     */
    TokenFilterFactory IDENTITY_FILTER = new TokenFilterFactory() {
        @Override
        public String name() {
            return "identity";
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return tokenStream;
        }
    };
}
