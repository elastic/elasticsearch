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

import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public final class SimilarityService extends AbstractIndexComponent {

    public final static String DEFAULT_SIMILARITY = "default";
    private final Similarity defaultSimilarity;
    private final Similarity baseSimilarity;
    private final Map<String, SimilarityProvider> similarities;
    static final Map<String, BiFunction<String, Settings, SimilarityProvider>> DEFAULTS;
    public static final Map<String, BiFunction<String, Settings, SimilarityProvider>> BUILT_IN;
    static {
        Map<String, BiFunction<String, Settings, SimilarityProvider>> defaults = new HashMap<>();
        Map<String, BiFunction<String, Settings, SimilarityProvider>> buildIn = new HashMap<>();
        defaults.put("default", DefaultSimilarityProvider::new);
        defaults.put("BM25", BM25SimilarityProvider::new);
        buildIn.put("default", DefaultSimilarityProvider::new);
        buildIn.put("BM25", BM25SimilarityProvider::new);
        buildIn.put("DFR", DFRSimilarityProvider::new);
        buildIn.put("IB", IBSimilarityProvider::new);
        buildIn.put("LMDirichlet", LMDirichletSimilarityProvider::new);
        buildIn.put("LMJelinekMercer", LMJelinekMercerSimilarityProvider::new);
        DEFAULTS = Collections.unmodifiableMap(defaults);
        BUILT_IN = Collections.unmodifiableMap(buildIn);
    }

    public SimilarityService(IndexSettings indexSettings, Map<String, BiFunction<String, Settings, SimilarityProvider>> similarities) {
        super(indexSettings);
        Map<String, SimilarityProvider> providers = new HashMap<>(similarities.size());
        Map<String, Settings> similaritySettings = this.indexSettings.getSettings().getGroups(IndexModule.SIMILARITY_SETTINGS_PREFIX);
        for (Map.Entry<String, Settings> entry : similaritySettings.entrySet()) {
            String name = entry.getKey();
            Settings settings = entry.getValue();
            String typeName = settings.get("type");
            if (typeName == null) {
                throw new IllegalArgumentException("Similarity [" + name + "] must have an associated type");
            } else if ((similarities.containsKey(typeName) || BUILT_IN.containsKey(typeName)) == false) {
                throw new IllegalArgumentException("Unknown Similarity type [" + typeName + "] for [" + name + "]");
            }
            BiFunction<String, Settings, SimilarityProvider> factory = similarities.getOrDefault(typeName, BUILT_IN.get(typeName));
            if (settings == null) {
                settings = Settings.Builder.EMPTY_SETTINGS;
            }
            providers.put(name, factory.apply(name, settings));
        }
        addSimilarities(similaritySettings, providers, DEFAULTS);
        this.similarities = providers;
        defaultSimilarity = providers.get(SimilarityService.DEFAULT_SIMILARITY).get();
        // Expert users can configure the base type as being different to default, but out-of-box we use default.
        baseSimilarity = (providers.get("base") != null) ? providers.get("base").get() :
                defaultSimilarity;
    }

    public Similarity similarity(MapperService mapperService) {
        // TODO we can maybe factor out MapperService here entirely by introducing an interface for the lookup?
        return (mapperService != null) ? new PerFieldSimilarity(defaultSimilarity, baseSimilarity, mapperService) :
                defaultSimilarity;
    }

    private void addSimilarities(Map<String, Settings>  similaritySettings, Map<String, SimilarityProvider> providers, Map<String, BiFunction<String, Settings, SimilarityProvider>> similarities)  {
        for (Map.Entry<String, BiFunction<String, Settings, SimilarityProvider>> entry : similarities.entrySet()) {
            String name = entry.getKey();
            BiFunction<String, Settings, SimilarityProvider> factory = entry.getValue();
            Settings settings = similaritySettings.get(name);
            if (settings == null) {
                settings = Settings.Builder.EMPTY_SETTINGS;
            }
            providers.put(name, factory.apply(name, settings));
        }
    }

    public SimilarityProvider getSimilarity(String name) {
        return similarities.get(name);
    }

    static class PerFieldSimilarity extends PerFieldSimilarityWrapper {

        private final Similarity defaultSimilarity;
        private final Similarity baseSimilarity;
        private final MapperService mapperService;

        PerFieldSimilarity(Similarity defaultSimilarity, Similarity baseSimilarity, MapperService mapperService) {
            this.defaultSimilarity = defaultSimilarity;
            this.baseSimilarity = baseSimilarity;
            this.mapperService = mapperService;
        }

        @Override
        public float coord(int overlap, int maxOverlap) {
            return baseSimilarity.coord(overlap, maxOverlap);
        }

        @Override
        public float queryNorm(float valueForNormalization) {
            return baseSimilarity.queryNorm(valueForNormalization);
        }

        @Override
        public Similarity get(String name) {
            MappedFieldType fieldType = mapperService.fullName(name);
            return (fieldType != null && fieldType.similarity() != null) ? fieldType.similarity().get() : defaultSimilarity;
        }
    }
}
