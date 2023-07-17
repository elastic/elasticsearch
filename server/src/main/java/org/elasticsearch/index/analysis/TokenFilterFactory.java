/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymGraphFilterFactory;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.index.IndexService.IndexCreationContext;
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
     * If the token filter is part of the definition of a  {@link ReloadableCustomAnalyzer},
     * this function is called twice, once at index creation with {@link IndexCreationContext#CREATE_INDEX}
     * and then later with {@link IndexCreationContext#RELOAD_ANALYZERS} on shard recovery.
     * The {@link IndexCreationContext#RELOAD_ANALYZERS} context should be used to load expensive resources
     * on a generic thread pool. See {@link SynonymGraphFilterFactory} for an example of how this context
     * is used.
     * @param context               the IndexCreationContext for the underlying index
     * @param tokenizer             the TokenizerFactory for the preceding chain
     * @param charFilters           any CharFilterFactories for the preceding chain
     * @param previousTokenFilters  a list of TokenFilterFactories in the preceding chain
     * @param allFilters            access to previously defined TokenFilterFactories
     */
    default TokenFilterFactory getChainAwareTokenFilterFactory(
        IndexCreationContext context,
        TokenizerFactory tokenizer,
        List<CharFilterFactory> charFilters,
        List<TokenFilterFactory> previousTokenFilters,
        Function<String, TokenFilterFactory> allFilters
    ) {
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
     * Get the name of the resource that this filter is based on.
     * Used to reload analyzers on this resource changes.
     *
     * For an example, see @SynonymGraphTokenFilterFactory#getResourceName()
     *
     * @return the name of the resource that this filter was loaded from if any
     */
    default String getResourceName() {
        return null;
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
