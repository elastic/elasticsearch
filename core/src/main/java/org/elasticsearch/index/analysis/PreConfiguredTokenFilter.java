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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides pre-configured, shared {@link TokenFilter}s.
 */
public final class PreConfiguredTokenFilter implements AnalysisModule.AnalysisProvider<TokenFilterFactory> {
    /**
     * Create a pre-configured token filter that may not vary at all.
     */
    public static PreConfiguredTokenFilter singleton(String name, boolean useFilterForMultitermQueries,
            Function<TokenStream, TokenStream> create) {
        return new PreConfiguredTokenFilter(name, useFilterForMultitermQueries, CachingStrategy.ONE,
                (tokenStream, version) -> create.apply(tokenStream));
    }

    /**
     * Create a pre-configured token filter that may vary based on the Lucene version.
     */
    public static PreConfiguredTokenFilter luceneVersion(String name, boolean useFilterForMultitermQueries,
            BiFunction<TokenStream, org.apache.lucene.util.Version, TokenStream> create) {
        return new PreConfiguredTokenFilter(name, useFilterForMultitermQueries, CachingStrategy.LUCENE,
                (tokenStream, version) -> create.apply(tokenStream, version.luceneVersion));
    }

    /**
     * Create a pre-configured token filter that may vary based on the Elasticsearch version.
     */
    public static PreConfiguredTokenFilter elasticsearchVersion(String name, boolean useFilterForMultitermQueries,
            BiFunction<TokenStream, org.elasticsearch.Version, TokenStream> create) {
        return new PreConfiguredTokenFilter(name, useFilterForMultitermQueries, CachingStrategy.ELASTICSEARCH,
                (tokenStream, version) -> create.apply(tokenStream, version));
    }

    private final String name;
    private final boolean useFilterForMultitermQueries;
    private final PreBuiltCacheFactory.PreBuiltCache<TokenFilterFactory> cache;
    private final BiFunction<TokenStream, Version, TokenStream> create;

    private PreConfiguredTokenFilter(String name, boolean useFilterForMultitermQueries,
            PreBuiltCacheFactory.CachingStrategy cache, BiFunction<TokenStream, Version, TokenStream> create) {
        this.name = name;
        this.useFilterForMultitermQueries = useFilterForMultitermQueries;
        this.cache = PreBuiltCacheFactory.getCache(cache);
        this.create = create;
    }

    @Override
    public TokenFilterFactory get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
        return getTokenFilterFactory(Version.indexCreated(settings));
    }

    /**
     * The name of the {@link TokenFilter} in the API.
     */
    public String getName() {
        return name;
    }

    /**
     * Can this {@link TokenFilter} be used in multi-term queries?
     */
    public boolean shouldUseFilterForMultitermQueries() {
        return useFilterForMultitermQueries;
    }

    private interface MultiTermAwareTokenFilterFactory extends TokenFilterFactory, MultiTermAwareComponent {}

    private synchronized TokenFilterFactory getTokenFilterFactory(final Version version) {
        TokenFilterFactory factory = cache.get(version);
        if (factory == null) {
            if (useFilterForMultitermQueries) {
                factory = new MultiTermAwareTokenFilterFactory() {
                    @Override
                    public String name() {
                        return name;
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return create.apply(tokenStream, version);
                    }

                    @Override
                    public Object getMultiTermComponent() {
                        return this;
                    }
                };
            } else {
                factory = new TokenFilterFactory() {
                    @Override
                    public String name() {
                        return name;
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return create.apply(tokenStream, version);
                    }
                };
            }
            cache.put(version, factory);
        }

        return factory;
    }
}
