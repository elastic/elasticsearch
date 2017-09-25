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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
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
     * @param multiTermComponent null if this tokenizer shouldn't be used for multi-term queries, otherwise a supplier for the
     *        {@link TokenFilterFactory} that stands in for this tokenizer in multi-term queries.
     */
    public static PreConfiguredTokenizer singleton(String name, Supplier<Tokenizer> create,
            @Nullable Supplier<TokenFilterFactory> multiTermComponent) {
        return new PreConfiguredTokenizer(name, CachingStrategy.ONE, version -> create.get(),
                multiTermComponent == null ? null : version -> multiTermComponent.get());
    }

    /**
     * Create a pre-configured tokenizer that may vary based on the Lucene version.
     * 
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     * @param multiTermComponent null if this tokenizer shouldn't be used for multi-term queries, otherwise a supplier for the
     *        {@link TokenFilterFactory} that stands in for this tokenizer in multi-term queries.
     */
    public static PreConfiguredTokenizer luceneVersion(String name, Function<org.apache.lucene.util.Version, Tokenizer> create,
            @Nullable Function<org.apache.lucene.util.Version, TokenFilterFactory> multiTermComponent) {
        return new PreConfiguredTokenizer(name, CachingStrategy.LUCENE, version -> create.apply(version.luceneVersion),
                multiTermComponent == null ? null : version -> multiTermComponent.apply(version.luceneVersion));
    }

    /**
     * Create a pre-configured tokenizer that may vary based on the Elasticsearch version.
     * 
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     * @param multiTermComponent null if this tokenizer shouldn't be used for multi-term queries, otherwise a supplier for the
     *        {@link TokenFilterFactory} that stands in for this tokenizer in multi-term queries.
     */
    public static PreConfiguredTokenizer elasticsearchVersion(String name, Function<org.elasticsearch.Version, Tokenizer> create,
            @Nullable Function<Version, TokenFilterFactory> multiTermComponent) {
        return new PreConfiguredTokenizer(name, CachingStrategy.ELASTICSEARCH, create, multiTermComponent);
    }

    private final Function<Version, Tokenizer> create;
    private final Function<Version, TokenFilterFactory> multiTermComponent;
    
    private PreConfiguredTokenizer(String name, PreBuiltCacheFactory.CachingStrategy cache, Function<Version, Tokenizer> create,
            @Nullable Function<Version, TokenFilterFactory> multiTermComponent) {
        super(name, cache);
        this.create = create;
        this.multiTermComponent = multiTermComponent;
    }

    /**
     * Does this tokenizer has an equivalent component for analyzing multi-term queries?
     */
    public boolean hasMultiTermComponent() {
        return multiTermComponent != null;
    }

    private interface MultiTermAwareTokenizerFactory extends TokenizerFactory, MultiTermAwareComponent {}

    @Override
    protected TokenizerFactory create(Version version) {
        if (multiTermComponent != null) {
            return new MultiTermAwareTokenizerFactory() {
                @Override
                public Tokenizer create() {
                    return create.apply(version);
                }

                @Override
                public Object getMultiTermComponent() {
                    return multiTermComponent.apply(version);
                }
            };
        } else {
            return () -> create.apply(version);
        }
    }
}
