/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymGraphFilterFactory;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.search.fetch.subphase.highlight.FastVectorHighlighter;

import java.util.List;
import java.util.Set;
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
     * Returns the set of resource names this filter depends on.
     * Used by {@link ReloadableCustomAnalyzer} to determine which synonym sets
     * trigger a reload for this filter.
     */
    default Set<String> getResourceNames() {
        return Set.of();
    }

    /**
     * Returns a value-typed key used by {@link AnalysisRegistry} to share analyzers that reference
     * this factory across indices. Two factories whose {@code sharingKey()} results are equal (via
     * {@code Object.equals}/{@code hashCode} on the returned value) are treated as behaviorally
     * equivalent and their analyzers can be shared.
     *
     * <p><b>Defaults to identity</b> ({@code this}): a factory that does not override this never
     * shares, which is always safe — at worst it misses a deduplication, never produces wrong
     * tokenization. Override it to opt into sharing: hold a single {@code Config} record over all
     * settings that influence behavior and return that record, so adding a setting means adding a
     * record component (no separate place to forget). Stateless factories return a constant.
     *
     * <p>An override MUST capture all state that affects behavior, including any external resource
     * version (file mtime, synonyms-set generation, etc). The returned value's {@code equals}/
     * {@code hashCode} MUST be stable across calls — notably, raw
     * {@link org.apache.lucene.analysis.CharArraySet} fields are unsafe (their {@code hashCode}
     * is unstable). Wrap them in {@link Analysis.StableCharArraySet} so the key holds a
     * stable-hash reference (equality still defers to
     * {@link org.apache.lucene.analysis.CharArraySet#equals}).
     *
     * <p><b>Testing contract</b>: every setting folded into this key must also be declared as a
     * distinguishing setting in the factory's {@code AnalysisFactoryTestCase} sharing probe (or the
     * factory marked stateless / identity there). That test fails the build if a registered factory
     * is left unclassified, and asserts each declared setting actually changes the key — so adding a
     * setting here means adding it to that declaration too.
     */
    default Object sharingKey() {
        return this;
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

        @Override
        public Object sharingKey() {
            // Constant singleton — every reference is the same instance.
            return this;
        }
    };
}
