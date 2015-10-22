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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * {@link SimilarityModule} is responsible gathering registered and configured {@link SimilarityProvider}
 * implementations and making them available through the {@link SimilarityService}.
 *
 * New {@link SimilarityProvider} implementations can be registered through {@link #addSimilarity(String, BiFunction)}
 * while existing Providers can be referenced through Settings under the {@link #SIMILARITY_SETTINGS_PREFIX} prefix
 * along with the "type" value.  For example, to reference the {@link BM25SimilarityProvider}, the configuration
 * <tt>"index.similarity.my_similarity.type : "BM25"</tt> can be used.
 */
public class SimilarityModule extends AbstractModule {

    public static final String SIMILARITY_SETTINGS_PREFIX = "index.similarity";

    private final Settings settings;
    private final Map<String, BiFunction<String, Settings, SimilarityProvider>> similarities = new HashMap<>();
    private final Index index;

    public SimilarityModule(Index index, Settings settings) {
        this.settings = settings;
        this.index = index;
    }

    /**
     * Registers the given {@link SimilarityProvider} with the given name
     *
     * @param name Name of the SimilarityProvider
     * @param similarity SimilarityProvider to register
     */
    public void addSimilarity(String name, BiFunction<String, Settings, SimilarityProvider> similarity) {
        if (similarities.containsKey(name) || SimilarityService.BUILT_IN.containsKey(name)) {
            throw new IllegalArgumentException("similarity for name: [" + name + " is already registered");
        }
        similarities.put(name, similarity);
    }

    @Override
    protected void configure() {
        SimilarityService service = new SimilarityService(index, settings, new HashMap<>(similarities));
        bind(SimilarityService.class).toInstance(service);
    }
}
