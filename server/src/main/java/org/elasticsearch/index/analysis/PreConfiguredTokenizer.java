/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides pre-configured, shared {@link Tokenizer}s.
 */
public final class PreConfiguredTokenizer extends PreConfiguredAnalysisComponent<TokenizerFactory> {
    /**
     * Create a pre-configured tokenizer that may not vary at all.
     *
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     */
    public static PreConfiguredTokenizer singleton(String name, Supplier<Tokenizer> create) {
        return new PreConfiguredTokenizer(name, CachingStrategy.ONE, version -> create.get());
    }

    /**
     * Create a pre-configured tokenizer that may vary based on the Lucene version.
     *
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     */
    public static PreConfiguredTokenizer luceneVersion(String name, Function<org.apache.lucene.util.Version, Tokenizer> create) {
        return new PreConfiguredTokenizer(name, CachingStrategy.LUCENE, version -> create.apply(version.luceneVersion));
    }

    /**
     * Create a pre-configured tokenizer that may vary based on the Elasticsearch version.
     *
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     */
    public static PreConfiguredTokenizer elasticsearchVersion(String name, Function<org.elasticsearch.Version, Tokenizer> create) {
        return new PreConfiguredTokenizer(name, CachingStrategy.ELASTICSEARCH, create);
    }

    private final Function<Version, Tokenizer> create;

    private PreConfiguredTokenizer(String name, PreBuiltCacheFactory.CachingStrategy cache, Function<Version, Tokenizer> create) {
        super(name, cache);
        this.create = create;
    }

    @Override
    protected TokenizerFactory create(Version version) {
        return TokenizerFactory.newFactory(name, () -> create.apply(version));
    }
}
