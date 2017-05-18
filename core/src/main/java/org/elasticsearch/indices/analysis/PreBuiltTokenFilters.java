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
package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.Version;
import org.elasticsearch.index.analysis.MultiTermAwareComponent;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.Locale;

public enum PreBuiltTokenFilters {
    // TODO remove this entire class when PreBuiltTokenizers no longer needs it.....
    LOWERCASE(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new LowerCaseFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    };

    protected boolean isMultiTermAware() {
        return false;
    }

    public abstract TokenStream create(TokenStream tokenStream, Version version);

    protected final PreBuiltCacheFactory.PreBuiltCache<TokenFilterFactory> cache;


    private final CachingStrategy cachingStrategy;
    PreBuiltTokenFilters(CachingStrategy cachingStrategy) {
        this.cachingStrategy = cachingStrategy;
        cache = PreBuiltCacheFactory.getCache(cachingStrategy);
    }

    public CachingStrategy getCachingStrategy() {
        return cachingStrategy;
    }

    private interface MultiTermAwareTokenFilterFactory extends TokenFilterFactory, MultiTermAwareComponent {}

    public synchronized TokenFilterFactory getTokenFilterFactory(final Version version) {
        TokenFilterFactory factory = cache.get(version);
        if (factory == null) {
            final String finalName = name().toLowerCase(Locale.ROOT);
            if (isMultiTermAware()) {
                factory = new MultiTermAwareTokenFilterFactory() {
                    @Override
                    public String name() {
                        return finalName;
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return PreBuiltTokenFilters.this.create(tokenStream, version);
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
                        return finalName;
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return PreBuiltTokenFilters.this.create(tokenStream, version);
                    }
                };
            }
            cache.put(version, factory);
        }

        return factory;
    }
}
