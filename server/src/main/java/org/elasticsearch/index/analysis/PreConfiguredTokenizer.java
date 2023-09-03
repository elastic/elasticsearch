/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.index.IndexVersion;
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
        return new PreConfiguredTokenizer(name, CachingStrategy.LUCENE, create.compose(IndexVersion::luceneVersion));
    }

    /**
     * Create a pre-configured tokenizer that may vary based on the index version.
     *
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     */
    public static PreConfiguredTokenizer indexVersion(String name, Function<IndexVersion, Tokenizer> create) {
        return new PreConfiguredTokenizer(name, CachingStrategy.INDEX, create);
    }

    private final Function<IndexVersion, Tokenizer> create;

    private PreConfiguredTokenizer(String name, PreBuiltCacheFactory.CachingStrategy cache, Function<IndexVersion, Tokenizer> create) {
        super(name, cache);
        this.create = create;
    }

    @Override
    protected TokenizerFactory create(IndexVersion version) {
        return TokenizerFactory.newFactory(name, () -> create.apply(version));
    }
}
