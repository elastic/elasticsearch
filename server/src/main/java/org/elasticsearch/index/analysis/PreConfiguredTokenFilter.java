/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides pre-configured, shared {@link TokenFilter}s.
 */
public final class PreConfiguredTokenFilter extends PreConfiguredAnalysisComponent<TokenFilterFactory> {

    /**
     * Create a pre-configured token filter that may not vary at all.
     */
    public static PreConfiguredTokenFilter singleton(
        String name,
        boolean useFilterForMultitermQueries,
        Function<TokenStream, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(
            name,
            useFilterForMultitermQueries,
            true,
            CachingStrategy.ONE,
            (tokenStream, version) -> create.apply(tokenStream)
        );
    }

    /**
     * Create a pre-configured token filter that may not vary at all.
     */
    public static PreConfiguredTokenFilter singleton(
        String name,
        boolean useFilterForMultitermQueries,
        boolean allowForSynonymParsing,
        Function<TokenStream, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(
            name,
            useFilterForMultitermQueries,
            allowForSynonymParsing,
            CachingStrategy.ONE,
            (tokenStream, version) -> create.apply(tokenStream)
        );
    }

    /**
     * Create a pre-configured token filter that may vary based on the Lucene version.
     */
    public static PreConfiguredTokenFilter luceneVersion(
        String name,
        boolean useFilterForMultitermQueries,
        BiFunction<TokenStream, org.apache.lucene.util.Version, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(
            name,
            useFilterForMultitermQueries,
            true,
            CachingStrategy.LUCENE,
            (tokenStream, version) -> create.apply(tokenStream, version.luceneVersion())
        );
    }

    /**
     * Create a pre-configured token filter that may vary based on the index version.
     */
    public static PreConfiguredTokenFilter indexVersion(
        String name,
        boolean useFilterForMultitermQueries,
        BiFunction<TokenStream, IndexVersion, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(name, useFilterForMultitermQueries, true, CachingStrategy.INDEX, create);
    }

    /**
     * Create a pre-configured token filter that may vary based on the index version.
     */
    public static PreConfiguredTokenFilter indexVersion(
        String name,
        boolean useFilterForMultitermQueries,
        boolean useFilterForParsingSynonyms,
        BiFunction<TokenStream, IndexVersion, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(name, useFilterForMultitermQueries, useFilterForParsingSynonyms, CachingStrategy.INDEX, create);
    }

    private final boolean useFilterForMultitermQueries;
    private final boolean allowForSynonymParsing;
    private final BiFunction<TokenStream, IndexVersion, TokenStream> create;

    private PreConfiguredTokenFilter(
        String name,
        boolean useFilterForMultitermQueries,
        boolean allowForSynonymParsing,
        PreBuiltCacheFactory.CachingStrategy cache,
        BiFunction<TokenStream, IndexVersion, TokenStream> create
    ) {
        super(name, cache);
        this.useFilterForMultitermQueries = useFilterForMultitermQueries;
        this.allowForSynonymParsing = allowForSynonymParsing;
        this.create = create;
    }

    @Override
    protected TokenFilterFactory create(IndexVersion version) {
        if (useFilterForMultitermQueries) {
            return new NormalizingTokenFilterFactory() {

                @Override
                public TokenStream normalize(TokenStream tokenStream) {
                    return create.apply(tokenStream, version);
                }

                @Override
                public String name() {
                    return getName();
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return create.apply(tokenStream, version);
                }

                @Override
                public TokenFilterFactory getSynonymFilter() {
                    if (allowForSynonymParsing) {
                        return this;
                    }
                    throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
                }
            };
        }
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return getName();
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return create.apply(tokenStream, version);
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                if (allowForSynonymParsing) {
                    return this;
                }
                throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
            }
        };
    }
}
