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

package org.elasticsearch.index.similarity;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * {@link SimilarityModule} is responsible gathering registered and configured {@link SimilarityProvider}
 * implementations and making them available through the {@link SimilarityLookupService} and {@link SimilarityService}.
 *
 * New {@link SimilarityProvider} implementations can be registered through {@link #addSimilarity(String, Class)}
 * while existing Providers can be referenced through Settings under the {@link #SIMILARITY_SETTINGS_PREFIX} prefix
 * along with the "type" value.  For example, to reference the {@link BM25SimilarityProvider}, the configuration
 * <tt>"index.similarity.my_similarity.type : "BM25"</tt> can be used.
 */
public class SimilarityModule extends AbstractModule {

    public static final String SIMILARITY_SETTINGS_PREFIX = "index.similarity";

    private final Settings settings;
    private final Map<String, Class<? extends SimilarityProvider>> similarities = Maps.newHashMap();

    public SimilarityModule(Settings settings) {
        this.settings = settings;
    }

    /**
     * Registers the given {@link SimilarityProvider} with the given name
     *
     * @param name Name of the SimilarityProvider
     * @param similarity SimilarityProvider to register
     */
    public void addSimilarity(String name, Class<? extends SimilarityProvider> similarity) {
        similarities.put(name, similarity);
    }

    @Override
    protected void configure() {
        Map<String, Class<? extends SimilarityProvider>> providers = Maps.newHashMap(similarities);

        Map<String, Settings> similaritySettings = settings.getGroups(SIMILARITY_SETTINGS_PREFIX);
        for (Map.Entry<String, Settings> entry : similaritySettings.entrySet()) {
            String name = entry.getKey();
            Settings settings = entry.getValue();

            Class<? extends SimilarityProvider> type =
                    settings.getAsClass("type", null, "org.elasticsearch.index.similarity.", "SimilarityProvider");
            if (type == null) {
                throw new ElasticsearchIllegalArgumentException("SimilarityProvider [" + name + "] must have an associated type");
            }
            providers.put(name, type);
        }

        MapBinder<String, SimilarityProvider.Factory> similarityBinder =
                MapBinder.newMapBinder(binder(), String.class, SimilarityProvider.Factory.class);

        for (Map.Entry<String, Class<? extends SimilarityProvider>> entry : providers.entrySet()) {
            similarityBinder.addBinding(entry.getKey()).toProvider(FactoryProvider.newFactory(SimilarityProvider.Factory.class, entry.getValue())).in(Scopes.SINGLETON);
        }

        for (PreBuiltSimilarityProvider.Factory factory : Similarities.listFactories()) {
            if (!providers.containsKey(factory.name())) {
                similarityBinder.addBinding(factory.name()).toInstance(factory);
            }
        }

        bind(SimilarityLookupService.class).asEagerSingleton();
        bind(SimilarityService.class).asEagerSingleton();
    }
}
