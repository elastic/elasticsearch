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

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory;
import org.elasticsearch.plugins.AnalysisPlugin.PreBuiltTokenFilterSpec;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Resolves pre-built {@link TokenFilterFactory}s based on the specifications provided by plugins.
 */
public class PreBuiltTokenFilterFactoryProvider implements AnalysisModule.AnalysisProvider<TokenFilterFactory> {
    private final String name;
    private final PreBuiltCacheFactory.PreBuiltCache<TokenFilterFactory> cache;
    private final boolean useFilterForMultitermQueries;
    private final BiFunction<TokenStream, Version, TokenStream> create;

    public PreBuiltTokenFilterFactoryProvider(String name, PreBuiltTokenFilterSpec spec) {
        this.name = name;
        cache = PreBuiltCacheFactory.getCache(spec.getCachingStrategy());
        this.useFilterForMultitermQueries = spec.shouldUseFilterForMultitermQueries();
        this.create = spec.getCreate();
    }

    @Override
    public TokenFilterFactory get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
        return getTokenFilterFactory(Version.indexCreated(settings));
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
