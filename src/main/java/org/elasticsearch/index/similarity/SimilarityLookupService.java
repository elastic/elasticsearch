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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 * Service for looking up configured {@link SimilarityProvider} implementations by name.
 * <p/>
 * The service instantiates the Providers through their Factories using configuration
 * values found with the {@link SimilarityModule#SIMILARITY_SETTINGS_PREFIX} prefix.
 */
public class SimilarityLookupService extends AbstractIndexComponent {

    public final static String DEFAULT_SIMILARITY = "default";

    private final ImmutableMap<String, SimilarityProvider> similarities;

    public SimilarityLookupService(Index index, Settings indexSettings) {
        this(index, indexSettings, ImmutableMap.<String, SimilarityProvider.Factory>of());
    }

    @Inject
    public SimilarityLookupService(Index index, @IndexSettings Settings indexSettings, Map<String, SimilarityProvider.Factory> similarities) {
        super(index, indexSettings);

        MapBuilder<String, SimilarityProvider> providers = MapBuilder.newMapBuilder();

        Map<String, Settings> similaritySettings = indexSettings.getGroups(SimilarityModule.SIMILARITY_SETTINGS_PREFIX);
        for (Map.Entry<String, SimilarityProvider.Factory> entry : similarities.entrySet()) {
            String name = entry.getKey();
            SimilarityProvider.Factory factory = entry.getValue();

            Settings settings = similaritySettings.get(name);
            if (settings == null) {
                settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            providers.put(name, factory.create(name, settings));
        }

        // For testing
        for (PreBuiltSimilarityProvider.Factory factory : Similarities.listFactories()) {
            if (!providers.containsKey(factory.name())) {
                providers.put(factory.name(), factory.get());
            }
        }

        this.similarities = providers.immutableMap();
    }

    /**
     * Returns the {@link SimilarityProvider} with the given name
     *
     * @param name Name of the SimilarityProvider to find
     * @return {@link SimilarityProvider} with the given name, or {@code null} if no Provider exists
     */
    public SimilarityProvider similarity(String name) {
        return similarities.get(name);
    }
}
