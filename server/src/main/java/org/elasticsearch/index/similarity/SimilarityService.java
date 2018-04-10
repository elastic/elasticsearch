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

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.common.TriFunction;
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
import java.util.function.Function;
import java.util.function.Supplier;

public final class SimilarityService extends AbstractIndexComponent {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(SimilarityService.class));
    public static final String DEFAULT_SIMILARITY = "BM25";
    private static final String CLASSIC_SIMILARITY = "classic";
    private static final Map<String, Function<Version, Supplier<Similarity>>> DEFAULTS;
    public static final Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> BUILT_IN;
    static {
        Map<String, Function<Version, Supplier<Similarity>>> defaults = new HashMap<>();
        defaults.put(CLASSIC_SIMILARITY, version -> {
            if (version.onOrAfter(Version.V_7_0_0_alpha1)) {
                return () -> {
                    throw new IllegalArgumentException("The [classic] similarity may not be used anymore. Please use the [BM25] "
                            + "similarity or build a custom [scripted] similarity instead.");
                };
            } else {
                final ClassicSimilarity similarity = SimilarityProviders.createClassicSimilarity(Settings.EMPTY, version);
                return () -> {
                    DEPRECATION_LOGGER.deprecated("The [classic] similarity is now deprecated in favour of BM25, which is generally "
                            + "accepted as a better alternative. Use the [BM25] similarity or build a custom [scripted] similarity "
                            + "instead.");
                    return similarity;
                };
            }
        });
        defaults.put("BM25", version -> {
            final BM25Similarity similarity = SimilarityProviders.createBM25Similarity(Settings.EMPTY, version);
            return () -> similarity;
        });
        defaults.put("boolean", version -> {
            final Similarity similarity = new BooleanSimilarity();
            return () -> similarity;
        });

        Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> builtIn = new HashMap<>();
        builtIn.put(CLASSIC_SIMILARITY,
                (settings, version, script) -> {
                    if (version.onOrAfter(Version.V_7_0_0_alpha1)) {
                        throw new IllegalArgumentException("The [classic] similarity may not be used anymore. Please use the [BM25] "
                                + "similarity or build a custom [scripted] similarity instead.");
                    } else {
                        DEPRECATION_LOGGER.deprecated("The [classic] similarity is now deprecated in favour of BM25, which is generally "
                                + "accepted as a better alternative. Use the [BM25] similarity or build a custom [scripted] similarity "
                                + "instead.");
                        return SimilarityProviders.createClassicSimilarity(settings, version);
                    }
                });
        builtIn.put("BM25",
                (settings, version, scriptService) -> SimilarityProviders.createBM25Similarity(settings, version));
        builtIn.put("boolean",
                (settings, version, scriptService) -> SimilarityProviders.createBooleanSimilarity(settings, version));
        builtIn.put("DFR",
                (settings, version, scriptService) -> SimilarityProviders.createDfrSimilarity(settings, version));
        builtIn.put("IB",
                (settings, version, scriptService) -> SimilarityProviders.createIBSimilarity(settings, version));
        builtIn.put("LMDirichlet",
                (settings, version, scriptService) -> SimilarityProviders.createLMDirichletSimilarity(settings, version));
        builtIn.put("LMJelinekMercer",
                (settings, version, scriptService) -> SimilarityProviders.createLMJelinekMercerSimilarity(settings, version));
        builtIn.put("DFI",
                (settings, version, scriptService) -> SimilarityProviders.createDfiSimilarity(settings, version));
        builtIn.put("scripted", new ScriptedSimilarityProvider());
        DEFAULTS = Collections.unmodifiableMap(defaults);
        BUILT_IN = Collections.unmodifiableMap(builtIn);
    }

    private final Similarity defaultSimilarity;
    private final Map<String, Supplier<Similarity>> similarities;

    public SimilarityService(IndexSettings indexSettings, ScriptService scriptService,
                             Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> similarities) {
        super(indexSettings);
        Map<String, Supplier<Similarity>> providers = new HashMap<>(similarities.size());
        Map<String, Settings> similaritySettings = this.indexSettings.getSettings().getGroups(IndexModule.SIMILARITY_SETTINGS_PREFIX);

        for (Map.Entry<String, Settings> entry : similaritySettings.entrySet()) {
            String name = entry.getKey();
            if (BUILT_IN.containsKey(name)) {
                throw new IllegalArgumentException("Cannot redefine built-in Similarity [" + name + "]");
            }
            Settings providerSettings = entry.getValue();
            String typeName = providerSettings.get("type");
            if (typeName == null) {
                throw new IllegalArgumentException("Similarity [" + name + "] must have an associated type");
            } else if ((similarities.containsKey(typeName) || BUILT_IN.containsKey(typeName)) == false) {
                throw new IllegalArgumentException("Unknown Similarity type [" + typeName + "] for [" + name + "]");
            }
            TriFunction<Settings, Version, ScriptService, Similarity> defaultFactory = BUILT_IN.get(typeName);
            TriFunction<Settings, Version, ScriptService, Similarity> factory = similarities.getOrDefault(typeName, defaultFactory);
            final Similarity similarity = factory.apply(providerSettings, indexSettings.getIndexVersionCreated(), scriptService);
            providers.put(name, () -> similarity);
        }
        for (Map.Entry<String, Function<Version, Supplier<Similarity>>> entry : DEFAULTS.entrySet()) {
            providers.put(entry.getKey(), entry.getValue().apply(indexSettings.getIndexVersionCreated()));
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

    
    public SimilarityProvider getSimilarity(String name) {
        Supplier<Similarity> sim = similarities.get(name);
        if (sim == null) {
            return null;
        }
        return new SimilarityProvider(name, sim.get());
    }

    // for testing
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
