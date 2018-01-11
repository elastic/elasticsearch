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
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.ScriptService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class SimilarityService extends AbstractIndexComponent {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(SimilarityService.class));
    public static final String DEFAULT_SIMILARITY = "BM25";
    private final Similarity defaultSimilarity;
    private final Map<String, SimilarityProvider> similarities;
    private static final Map<String, SimilarityProvider.Factory> DEFAULTS;
    public static final Map<String, SimilarityProvider.Factory> BUILT_IN;
    static {
        Map<String, SimilarityProvider.Factory> defaults = new HashMap<>();
        defaults.put("classic",
                (name, settings, indexSettings, scriptService) -> new ClassicSimilarityProvider(name, settings, indexSettings));
        defaults.put("BM25",
                (name, settings, indexSettings, scriptService) -> new BM25SimilarityProvider(name, settings, indexSettings));
        defaults.put("boolean",
                (name, settings, indexSettings, scriptService) -> new BooleanSimilarityProvider(name, settings, indexSettings));

        Map<String, SimilarityProvider.Factory> builtIn = new HashMap<>(defaults);
        builtIn.put("DFR",
                (name, settings, indexSettings, scriptService) -> new DFRSimilarityProvider(name, settings, indexSettings));
        builtIn.put("IB",
                (name, settings, indexSettings, scriptService) -> new IBSimilarityProvider(name, settings, indexSettings));
        builtIn.put("LMDirichlet",
                (name, settings, indexSettings, scriptService) -> new LMDirichletSimilarityProvider(name, settings, indexSettings));
        builtIn.put("LMJelinekMercer",
                (name, settings, indexSettings, scriptService) -> new LMJelinekMercerSimilarityProvider(name, settings, indexSettings));
        builtIn.put("DFI",
                (name, settings, indexSettings, scriptService) -> new DFISimilarityProvider(name, settings, indexSettings));
        builtIn.put("scripted", ScriptedSimilarityProvider::new);
        DEFAULTS = Collections.unmodifiableMap(defaults);
        BUILT_IN = Collections.unmodifiableMap(builtIn);
    }

    public SimilarityService(IndexSettings indexSettings, ScriptService scriptService,
                             Map<String, SimilarityProvider.Factory> similarities) {
        super(indexSettings);
        Map<String, SimilarityProvider> providers = new HashMap<>(similarities.size());
        Map<String, Settings> similaritySettings = this.indexSettings.getSettings().getGroups(IndexModule.SIMILARITY_SETTINGS_PREFIX);
        for (Map.Entry<String, Settings> entry : similaritySettings.entrySet()) {
            String name = entry.getKey();
            // Starting with v5.0 indices, it should no longer be possible to redefine built-in similarities
            if(BUILT_IN.containsKey(name) && indexSettings.getIndexVersionCreated().onOrAfter(Version.V_5_0_0_alpha1)) {
                throw new IllegalArgumentException("Cannot redefine built-in Similarity [" + name + "]");
            }
            Settings providerSettings = entry.getValue();
            String typeName = providerSettings.get("type");
            if (typeName == null) {
                throw new IllegalArgumentException("Similarity [" + name + "] must have an associated type");
            } else if ((similarities.containsKey(typeName) || BUILT_IN.containsKey(typeName)) == false) {
                throw new IllegalArgumentException("Unknown Similarity type [" + typeName + "] for [" + name + "]");
            }
            SimilarityProvider.Factory defaultFactory = BUILT_IN.get(typeName);
            SimilarityProvider.Factory factory = similarities.getOrDefault(typeName, defaultFactory);
            providers.put(name, factory.create(name, providerSettings, indexSettings.getSettings(), scriptService));
        }
        Map<String, SimilarityProvider> providerMapping = addSimilarities(similaritySettings, indexSettings.getSettings(), scriptService,
                DEFAULTS);
        for (Map.Entry<String, SimilarityProvider> entry : providerMapping.entrySet()) {
            // Avoid overwriting custom providers for indices older that v5.0
            if (providers.containsKey(entry.getKey()) && indexSettings.getIndexVersionCreated().before(Version.V_5_0_0_alpha1)) {
                continue;
            }
            providers.put(entry.getKey(), entry.getValue());
        }
        this.similarities = providers;
        defaultSimilarity = (providers.get("default") != null) ? providers.get("default").get()
                                                              : providers.get(SimilarityService.DEFAULT_SIMILARITY).get();
        if (providers.get("base") != null) {
            DEPRECATION_LOGGER.deprecated("The [base] similarity is ignored since query normalization and coords have been removed");
        }
    }

    public Similarity similarity(MapperService mapperService) {
        // TODO we can maybe factor out MapperService here entirely by introducing an interface for the lookup?
        return (mapperService != null) ? new PerFieldSimilarity(defaultSimilarity, mapperService) :
                defaultSimilarity;
    }

    private Map<String, SimilarityProvider> addSimilarities(Map<String, Settings>  similaritySettings, Settings indexSettings,
                                 ScriptService scriptService, Map<String, SimilarityProvider.Factory> similarities)  {
        Map<String, SimilarityProvider> providers = new HashMap<>(similarities.size());
        for (Map.Entry<String, SimilarityProvider.Factory> entry : similarities.entrySet()) {
            String name = entry.getKey();
            SimilarityProvider.Factory factory = entry.getValue();
            Settings providerSettings = similaritySettings.get(name);
            if (providerSettings == null) {
                providerSettings = Settings.Builder.EMPTY_SETTINGS;
            }
            providers.put(name, factory.create(name, providerSettings, indexSettings, scriptService));
        }
        return providers;
    }

    public SimilarityProvider getSimilarity(String name) {
        return similarities.get(name);
    }

    Similarity getDefaultSimilarity() {
        return defaultSimilarity;
    }

    static class PerFieldSimilarity extends PerFieldSimilarityWrapper {

        private final Similarity defaultSimilarity;
        private final MapperService mapperService;

        PerFieldSimilarity(Similarity defaultSimilarity, MapperService mapperService) {
            super();
            this.defaultSimilarity = defaultSimilarity;
            this.mapperService = mapperService;
        }

        @Override
        public Similarity get(String name) {
            MappedFieldType fieldType = mapperService.fullName(name);
            return (fieldType != null && fieldType.similarity() != null) ? fieldType.similarity().get() : defaultSimilarity;
        }
    }
}
