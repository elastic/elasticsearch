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
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public final class SimilarityService extends AbstractIndexComponent {
    public static final String DEFAULT_SIMILARITY = "BM25";

    public static final Setting<Settings> SIMILARITY_SETTINGS = Setting.groupSetting("index.similarity.",
        Setting.Property.IndexScope, Setting.Property.Dynamic);

    static final Map<String, BiFunction<String, Settings, SimilarityProvider>> DEFAULTS;
    public static final Map<String, BiFunction<String, Settings, SimilarityProvider>> BUILT_IN;
    static {
        Map<String, BiFunction<String, Settings, SimilarityProvider>> defaults = new HashMap<>();
        Map<String, BiFunction<String, Settings, SimilarityProvider>> buildIn = new HashMap<>();
        defaults.put("classic", ClassicSimilarityProvider::new);
        defaults.put("BM25", BM25SimilarityProvider::new);
        buildIn.put("classic", ClassicSimilarityProvider::new);
        buildIn.put("BM25", BM25SimilarityProvider::new);
        buildIn.put("DFR", DFRSimilarityProvider::new);
        buildIn.put("IB", IBSimilarityProvider::new);
        buildIn.put("LMDirichlet", LMDirichletSimilarityProvider::new);
        buildIn.put("LMJelinekMercer", LMJelinekMercerSimilarityProvider::new);
        buildIn.put("DFI", DFISimilarityProvider::new);
        DEFAULTS = Collections.unmodifiableMap(defaults);
        BUILT_IN = Collections.unmodifiableMap(buildIn);
    }

    private final Map<String, BiFunction<String, Settings, SimilarityProvider> > customProviders;
    private final Map<String, SimilarityProvider> similarities =
        Collections.synchronizedMap(new HashMap<> ());

    private final SimilarityProvider defaultSimilarityProvider;
    private final SimilarityProvider baseSimilarityProvider;

    public SimilarityService(IndexSettings indexSettings,
                             Map<String, BiFunction<String, Settings, SimilarityProvider>> customProviders) {
        super(indexSettings);
        this.customProviders = customProviders;
        Map<String, Settings> groupSettings =
            new HashMap<> (indexSettings.getSettings().getGroups(SIMILARITY_SETTINGS.getKey()));

        // Add default similarity (BM25 and classic)
        for (Map.Entry<String, BiFunction<String, Settings, SimilarityProvider>> entry : DEFAULTS.entrySet()) {
            String name = entry.getKey();
            Settings settings = groupSettings.get(name);
            if (settings == null) {
                settings = Settings.Builder.EMPTY_SETTINGS;
            }
            String type = settings.get("type");
            BiFunction<String, Settings, SimilarityProvider> factory;
            if (type != null) {
                if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_5_0_0_alpha1)) {
                    throw new IllegalArgumentException("Cannot redefine built-in Similarity [" + entry.getKey() + "]");
                }
                if (BUILT_IN.containsKey(type) == false && customProviders.containsKey(type) == false) {
                    throw new IllegalArgumentException("Unknown Similarity type [" + type + "] for [" +
                        entry.getKey() + "]");
                }
                factory = customProviders.getOrDefault(type, BUILT_IN.get(type));
            } else {
                factory = entry.getValue();
            }
            SimilarityProvider provider = factory.apply(name, settings);
            similarities.put(name, provider);
            groupSettings.remove(name);
        }
        addOrUpdateSimilarities(groupSettings, false);

        defaultSimilarityProvider = similarities.containsKey("default") ? similarities.get("default")
            : similarities.get(SimilarityService.DEFAULT_SIMILARITY);
        // Expert users can configure the base type as being different to default, but out-of-box we use default.
        baseSimilarityProvider = similarities.containsKey("base") ? similarities.get("base") :
            defaultSimilarityProvider;
        similarities.put("default", defaultSimilarityProvider);
        similarities.put("base", baseSimilarityProvider);
    }

    public SimilarityProvider getSimilarity(String name) {
        return similarities.get(name);
    }

    public Similarity similarity(MapperService mapperService) {
        // TODO we can maybe factor out MapperService here entirely by introducing an interface for the lookup?
        return new PerFieldSimilarity(mapperService);
    }

    public void addSettingsUpdateConsumer(IndexScopedSettings settings) {
        settings.addSettingsUpdateConsumer(SIMILARITY_SETTINGS, this::updateSettings, this::validateSettings);
    }

    /**
     * validates that the updated settings are valid.
     **/
    void validateSettings(Settings settings) {
        Map<String, Settings> groupSettings = settings.getAsGroups();
        addOrUpdateSimilarities(groupSettings, true);
    }

    /**
     * update in place the similarity provider with the new settings
     */
    void updateSettings(Settings settings) {
        Map<String, Settings> groupSettings = settings.getAsGroups();
        addOrUpdateSimilarities(groupSettings, false);
    }

    private void addOrUpdateSimilarities(Map<String, Settings> groupSettings, boolean dryRun) {
        for (Map.Entry<String, Settings> entry : groupSettings.entrySet()) {
            final String name = entry.getKey();
            final Settings settings = entry.getValue();
            SimilarityProvider provider = similarities.get(name);
            if (provider != null) {
                provider.validateUpdateSettings(settings);
                if (dryRun == false) {
                    provider.updateSettings(settings);
                }
            } else {
                String type = settings.get("type");
                if (type == null) {
                    throw new IllegalArgumentException("Similarity [" + name +
                        "] must have an associated type");
                }
                if (BUILT_IN.containsKey(type) == false && customProviders.containsKey(type) == false) {
                    throw new IllegalArgumentException("Unknown Similarity type [" + type + "] for [" +
                        name + "]");
                }
                BiFunction<String, Settings, SimilarityProvider> factory =
                    customProviders.getOrDefault(type, BUILT_IN.get(type));
                provider = factory.apply(name, settings);
                if (dryRun == false) {
                    similarities.put(name, provider);
                }
            }
        }
    }

    class PerFieldSimilarity extends PerFieldSimilarityWrapper {
        final MapperService mapperService;

        PerFieldSimilarity(MapperService mapperService) {
            this.mapperService = mapperService;
        }

        @Override
        public float coord(int overlap, int maxOverlap) {
            return baseSimilarityProvider.get().coord(overlap, maxOverlap);
        }

        @Override
        public float queryNorm(float valueForNormalization) {
            return baseSimilarityProvider.get().queryNorm(valueForNormalization);
        }

        @Override
        public Similarity get(String name) {
            if (mapperService == null) {
                return defaultSimilarityProvider.get();
            }
            MappedFieldType fieldType = mapperService.fullName(name);
            return (fieldType != null && fieldType.similarity() != null) ?
                fieldType.similarity().get() : defaultSimilarityProvider.get();
        }
    }
}
