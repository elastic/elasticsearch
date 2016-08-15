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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.indices.analysis.PreBuiltCharFilters;
import org.elasticsearch.indices.analysis.PreBuiltTokenFilters;
import org.elasticsearch.indices.analysis.PreBuiltTokenizers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * An internal registry for tokenizer, token filter, char filter and analyzer.
 * This class exists per node and allows to create per-index {@link AnalysisService} via {@link #build(IndexSettings)}
 */
public final class AnalysisRegistry implements Closeable {
    public static final String INDEX_ANALYSIS_CHAR_FILTER = "index.analysis.char_filter";
    public static final String INDEX_ANALYSIS_FILTER = "index.analysis.filter";
    public static final String INDEX_ANALYSIS_TOKENIZER = "index.analysis.tokenizer";
    private final PrebuiltAnalysis prebuiltAnalysis = new PrebuiltAnalysis();
    private final Map<String, Analyzer> cachedAnalyzer = new ConcurrentHashMap<>();

    private final Environment environment;
    private final Map<String, AnalysisProvider<CharFilterFactory>> charFilters;
    private final Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters;
    private final Map<String, AnalysisProvider<TokenizerFactory>> tokenizers;
    private final Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzers;

    public AnalysisRegistry(Environment environment,
                            Map<String, AnalysisProvider<CharFilterFactory>> charFilters,
                            Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters,
                            Map<String, AnalysisProvider<TokenizerFactory>> tokenizers,
                            Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzers) {
        this.environment = environment;
        this.charFilters = unmodifiableMap(charFilters);
        this.tokenFilters = unmodifiableMap(tokenFilters);
        this.tokenizers = unmodifiableMap(tokenizers);
        this.analyzers = unmodifiableMap(analyzers);
    }

    /**
     * Returns a {@link Settings} by groupName from {@link IndexSettings} or a default {@link Settings}
     * @param indexSettings an index settings
     * @param groupName tokenizer/token filter/char filter name
     * @return {@link Settings}
     */
    public static Settings getSettingsFromIndexSettings(IndexSettings indexSettings, String groupName) {
        Settings settings = indexSettings.getSettings().getAsSettings(groupName);
        if (settings.isEmpty()) {
            settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, indexSettings.getIndexVersionCreated()).build();
        }
        return settings;
    }

    /**
     * Returns a registered {@link TokenizerFactory} provider by name or <code>null</code> if the tokenizer was not registered
     */
    public AnalysisModule.AnalysisProvider<TokenizerFactory> getTokenizerProvider(String tokenizer) {
        return tokenizers.getOrDefault(tokenizer, this.prebuiltAnalysis.getTokenizerFactory(tokenizer));
    }

    /**
     * Returns a registered {@link TokenFilterFactory} provider by name or <code>null</code> if the token filter was not registered
     */
    public AnalysisModule.AnalysisProvider<TokenFilterFactory> getTokenFilterProvider(String tokenFilter) {
        return tokenFilters.getOrDefault(tokenFilter, this.prebuiltAnalysis.getTokenFilterFactory(tokenFilter));
    }

    /**
     * Returns a registered {@link CharFilterFactory} provider by name or <code>null</code> if the char filter was not registered
     */
    public AnalysisModule.AnalysisProvider<CharFilterFactory> getCharFilterProvider(String charFilter) {
        return charFilters.getOrDefault(charFilter, this.prebuiltAnalysis.getCharFilterFactory(charFilter));
    }

    /**
     * Returns a registered {@link Analyzer} provider by name or <code>null</code> if the analyzer was not registered
     */
    public Analyzer getAnalyzer(String analyzer) throws IOException {
        AnalysisModule.AnalysisProvider<AnalyzerProvider<?>> analyzerProvider = this.prebuiltAnalysis.getAnalyzerProvider(analyzer);
        if (analyzerProvider == null) {
            AnalysisModule.AnalysisProvider<AnalyzerProvider<?>> provider = analyzers.get(analyzer);
            return provider == null ? null : cachedAnalyzer.computeIfAbsent(analyzer, (key) -> {
                        try {
                            return provider.get(environment, key).get();
                        } catch (IOException ex) {
                            throw new ElasticsearchException("failed to load analyzer for name " + key, ex);
                        }}
            );
        }
        return analyzerProvider.get(environment, analyzer).get();
    }

    @Override
    public void close() throws IOException {
        try {
            prebuiltAnalysis.close();
        } finally {
            IOUtils.close(cachedAnalyzer.values());
        }
    }

    /**
     * Creates an index-level {@link AnalysisService} from this registry using the given index settings
     */
    public AnalysisService build(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> charFiltersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_CHAR_FILTER);
        final Map<String, Settings> tokenFiltersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_FILTER);
        final Map<String, Settings> tokenizersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_TOKENIZER);
        final Map<String, Settings> analyzersSettings = indexSettings.getSettings().getGroups("index.analysis.analyzer");

        final Map<String, CharFilterFactory> charFilterFactories = buildMapping(false, "charfilter", indexSettings, charFiltersSettings, charFilters, prebuiltAnalysis.charFilterFactories);
        final Map<String, TokenizerFactory> tokenizerFactories = buildMapping(false, "tokenizer", indexSettings, tokenizersSettings, tokenizers, prebuiltAnalysis.tokenizerFactories);

        Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> tokenFilters = new HashMap<>(this.tokenFilters);
        /*
         * synonym is different than everything else since it needs access to the tokenizer factories for this index.
         * instead of building the infrastructure for plugins we rather make it a real exception to not pollute the general interface and
         * hide internal data-structures as much as possible.
         */
        tokenFilters.put("synonym", requriesAnalysisSettings((is, env, name, settings) -> new SynonymTokenFilterFactory(is, env, this, name, settings)));
        final Map<String, TokenFilterFactory> tokenFilterFactories = buildMapping(false, "tokenfilter", indexSettings, tokenFiltersSettings, Collections.unmodifiableMap(tokenFilters), prebuiltAnalysis.tokenFilterFactories);
        final Map<String, AnalyzerProvider<?>> analyzierFactories = buildMapping(true, "analyzer", indexSettings, analyzersSettings,
                analyzers, prebuiltAnalysis.analyzerProviderFactories);
        return new AnalysisService(indexSettings, analyzierFactories, tokenizerFactories, charFilterFactories, tokenFilterFactories);
    }

    /**
     * Returns a registered {@link TokenizerFactory} provider by {@link IndexSettings}
     *  or a registered {@link TokenizerFactory} provider by predefined name
     *  or <code>null</code> if the tokenizer was not registered
     * @param tokenizer global or defined tokenizer name
     * @param indexSettings an index settings
     * @return {@link TokenizerFactory} provider or <code>null</code>
     */
    public AnalysisProvider<TokenizerFactory> getTokenizerProvider(String tokenizer, IndexSettings indexSettings) {
        final Map<String, Settings> tokenizerSettings = indexSettings.getSettings().getGroups("index.analysis.tokenizer");
        if (tokenizerSettings.containsKey(tokenizer)) {
            Settings currentSettings = tokenizerSettings.get(tokenizer);
            return getAnalysisProvider("tokenizer", tokenizers, tokenizer, currentSettings.get("type"));
        } else {
            return prebuiltAnalysis.tokenizerFactories.get(tokenizer);
        }
    }

    /**
     * Returns a registered {@link TokenFilterFactory} provider by {@link IndexSettings}
     *  or a registered {@link TokenFilterFactory} provider by predefined name
     *  or <code>null</code> if the tokenFilter was not registered
     * @param tokenFilter global or defined tokenFilter name
     * @param indexSettings an index settings
     * @return {@link TokenFilterFactory} provider or <code>null</code>
     */
    public AnalysisProvider<TokenFilterFactory> getTokenFilterProvider(String tokenFilter, IndexSettings indexSettings) {
        final Map<String, Settings> tokenFilterSettings = indexSettings.getSettings().getGroups("index.analysis.filter");
        if (tokenFilterSettings.containsKey(tokenFilter)) {
            Settings currentSettings = tokenFilterSettings.get(tokenFilter);
            String typeName = currentSettings.get("type");
            /*
             * synonym is different than everything else since it needs access to the tokenizer factories for this index.
             * instead of building the infrastructure for plugins we rather make it a real exception to not pollute the general interface and
             * hide internal data-structures as much as possible.
             */
            if ("synonym".equals(typeName)) {
                return requriesAnalysisSettings((is, env, name, settings) -> new SynonymTokenFilterFactory(is, env, this, name, settings));
            } else {
                return getAnalysisProvider("tokenfilter", tokenFilters, tokenFilter, typeName);
            }
        } else {
            return prebuiltAnalysis.tokenFilterFactories.get(tokenFilter);
        }
    }

    /**
     * Returns a registered {@link CharFilterFactory} provider by {@link IndexSettings}
     *  or a registered {@link CharFilterFactory} provider by predefined name
     *  or <code>null</code> if the charFilter was not registered
     * @param charFilter global or defined charFilter name
     * @param indexSettings an index settings
     * @return {@link CharFilterFactory} provider or <code>null</code>
     */
    public AnalysisProvider<CharFilterFactory> getCharFilterProvider(String charFilter, IndexSettings indexSettings) {
        final Map<String, Settings> tokenFilterSettings = indexSettings.getSettings().getGroups("index.analysis.char_filter");
        if (tokenFilterSettings.containsKey(charFilter)) {
            Settings currentSettings = tokenFilterSettings.get(charFilter);
            return getAnalysisProvider("charfilter", charFilters, charFilter, currentSettings.get("type"));
        } else {
            return prebuiltAnalysis.charFilterFactories.get(charFilter);
        }
    }

    private static <T> AnalysisModule.AnalysisProvider<T> requriesAnalysisSettings(AnalysisModule.AnalysisProvider<T> provider) {
        return new AnalysisModule.AnalysisProvider<T>() {
            @Override
            public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
                return provider.get(indexSettings, environment, name, settings);
            }
            @Override
            public boolean requiresAnalysisSettings() {
                return true;
            }
        };
    }

    private <T> Map<String, T> buildMapping(boolean analyzer, String toBuild, IndexSettings settings, Map<String, Settings> settingsMap,
            Map<String, AnalysisModule.AnalysisProvider<T>> providerMap, Map<String, AnalysisModule.AnalysisProvider<T>> defaultInstance)
            throws IOException {
        Settings defaultSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, settings.getIndexVersionCreated()).build();
        Map<String, T> factories = new HashMap<>();
        for (Map.Entry<String, Settings> entry : settingsMap.entrySet()) {
            String name = entry.getKey();
            Settings currentSettings = entry.getValue();
            String typeName = currentSettings.get("type");
            if (analyzer) {
                T factory;
                if (typeName == null) {
                    if (currentSettings.get("tokenizer") != null) {
                        factory = (T) new CustomAnalyzerProvider(settings, name, currentSettings);
                    } else {
                        throw new IllegalArgumentException(toBuild + " [" + name + "] must specify either an analyzer type, or a tokenizer");
                    }
                } else if (typeName.equals("custom")) {
                    factory = (T) new CustomAnalyzerProvider(settings, name, currentSettings);
                } else {
                    AnalysisModule.AnalysisProvider<T> type = providerMap.get(typeName);
                    if (type == null) {
                        throw new IllegalArgumentException("Unknown " + toBuild + " type [" + typeName + "] for [" + name + "]");
                    }
                    factory = type.get(settings, environment, name, currentSettings);
                }
                factories.put(name, factory);
            }  else {
                AnalysisProvider<T> type = getAnalysisProvider(toBuild, providerMap, name, typeName);
                final T factory = type.get(settings, environment, name, currentSettings);
                factories.put(name, factory);
            }

        }
        // go over the char filters in the bindings and register the ones that are not configured
        for (Map.Entry<String, AnalysisModule.AnalysisProvider<T>> entry : providerMap.entrySet()) {
            String name = entry.getKey();
            AnalysisModule.AnalysisProvider<T> provider = entry.getValue();
            // we don't want to re-register one that already exists
            if (settingsMap.containsKey(name)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (provider.requiresAnalysisSettings()) {
                continue;
            }
            AnalysisModule.AnalysisProvider<T> defaultProvider = defaultInstance.get(name);
            final T instance;
            if (defaultProvider == null) {
                instance = provider.get(settings, environment, name, defaultSettings);
            } else {
                instance = defaultProvider.get(settings, environment, name, defaultSettings);
            }
            factories.put(name, instance);
        }

        for (Map.Entry<String, AnalysisModule.AnalysisProvider<T>> entry : defaultInstance.entrySet()) {
            final String name = entry.getKey();
            final AnalysisModule.AnalysisProvider<T> provider = entry.getValue();
            if (factories.containsKey(name) == false) {
                final T instance = provider.get(settings, environment, name, defaultSettings);
                if (factories.containsKey(name) == false) {
                    factories.put(name, instance);
                }
            }
        }
        return factories;
    }

    private <T> AnalysisProvider<T> getAnalysisProvider(String toBuild, Map<String, AnalysisProvider<T>> providerMap, String name, String typeName) {
        if (typeName == null) {
            throw new IllegalArgumentException(toBuild + " [" + name + "] must specify either an analyzer type, or a tokenizer");
        }
        AnalysisProvider<T> type = providerMap.get(typeName);
        if (type == null) {
            throw new IllegalArgumentException("Unknown " + toBuild + " type [" + typeName + "] for [" + name + "]");
        }
        return type;
    }

    private static class PrebuiltAnalysis implements Closeable {

        final Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<?>>> analyzerProviderFactories;
        final Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> tokenizerFactories;
        final Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> tokenFilterFactories;
        final Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> charFilterFactories;

        private PrebuiltAnalysis() {
            Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories = new HashMap<>();
            Map<String, PreBuiltTokenizerFactoryFactory> tokenizerFactories = new HashMap<>();
            Map<String, PreBuiltTokenFilterFactoryFactory> tokenFilterFactories = new HashMap<>();
            Map<String, PreBuiltCharFilterFactoryFactory> charFilterFactories = new HashMap<>();
            // Analyzers
            for (PreBuiltAnalyzers preBuiltAnalyzerEnum : PreBuiltAnalyzers.values()) {
                String name = preBuiltAnalyzerEnum.name().toLowerCase(Locale.ROOT);
                analyzerProviderFactories.put(name, new PreBuiltAnalyzerProviderFactory(name, AnalyzerScope.INDICES, preBuiltAnalyzerEnum.getAnalyzer(Version.CURRENT)));
            }

            // Tokenizers
            for (PreBuiltTokenizers preBuiltTokenizer : PreBuiltTokenizers.values()) {
                String name = preBuiltTokenizer.name().toLowerCase(Locale.ROOT);
                tokenizerFactories.put(name, new PreBuiltTokenizerFactoryFactory(preBuiltTokenizer.getTokenizerFactory(Version.CURRENT)));
            }

            // Tokenizer aliases
            tokenizerFactories.put("nGram", new PreBuiltTokenizerFactoryFactory(PreBuiltTokenizers.NGRAM.getTokenizerFactory(Version.CURRENT)));
            tokenizerFactories.put("edgeNGram", new PreBuiltTokenizerFactoryFactory(PreBuiltTokenizers.EDGE_NGRAM.getTokenizerFactory(Version.CURRENT)));
            tokenizerFactories.put("PathHierarchy", new PreBuiltTokenizerFactoryFactory(PreBuiltTokenizers.PATH_HIERARCHY.getTokenizerFactory(Version.CURRENT)));


            // Token filters
            for (PreBuiltTokenFilters preBuiltTokenFilter : PreBuiltTokenFilters.values()) {
                String name = preBuiltTokenFilter.name().toLowerCase(Locale.ROOT);
                tokenFilterFactories.put(name, new PreBuiltTokenFilterFactoryFactory(preBuiltTokenFilter.getTokenFilterFactory(Version.CURRENT)));
            }
            // Token filter aliases
            tokenFilterFactories.put("nGram", new PreBuiltTokenFilterFactoryFactory(PreBuiltTokenFilters.NGRAM.getTokenFilterFactory(Version.CURRENT)));
            tokenFilterFactories.put("edgeNGram", new PreBuiltTokenFilterFactoryFactory(PreBuiltTokenFilters.EDGE_NGRAM.getTokenFilterFactory(Version.CURRENT)));


            // Char Filters
            for (PreBuiltCharFilters preBuiltCharFilter : PreBuiltCharFilters.values()) {
                String name = preBuiltCharFilter.name().toLowerCase(Locale.ROOT);
                charFilterFactories.put(name, new PreBuiltCharFilterFactoryFactory(preBuiltCharFilter.getCharFilterFactory(Version.CURRENT)));
            }
            // Char filter aliases
            charFilterFactories.put("htmlStrip", new PreBuiltCharFilterFactoryFactory(PreBuiltCharFilters.HTML_STRIP.getCharFilterFactory(Version.CURRENT)));
            this.analyzerProviderFactories = Collections.unmodifiableMap(analyzerProviderFactories);
            this.charFilterFactories = Collections.unmodifiableMap(charFilterFactories);
            this.tokenFilterFactories = Collections.unmodifiableMap(tokenFilterFactories);
            this.tokenizerFactories = Collections.unmodifiableMap(tokenizerFactories);
        }

        public AnalysisModule.AnalysisProvider<CharFilterFactory> getCharFilterFactory(String name) {
            return charFilterFactories.get(name);
        }

        public AnalysisModule.AnalysisProvider<TokenFilterFactory> getTokenFilterFactory(String name) {
            return tokenFilterFactories.get(name);
        }

        public AnalysisModule.AnalysisProvider<TokenizerFactory> getTokenizerFactory(String name) {
            return tokenizerFactories.get(name);
        }

        public AnalysisModule.AnalysisProvider<AnalyzerProvider<?>> getAnalyzerProvider(String name) {
            return analyzerProviderFactories.get(name);
        }

        Analyzer analyzer(String name) {
            PreBuiltAnalyzerProviderFactory  analyzerProviderFactory = (PreBuiltAnalyzerProviderFactory) analyzerProviderFactories.get(name);
            if (analyzerProviderFactory == null) {
                return null;
            }
            return analyzerProviderFactory.analyzer();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(analyzerProviderFactories.values().stream().map((a) -> ((PreBuiltAnalyzerProviderFactory)a).analyzer()).collect(Collectors.toList()));
        }
    }
}
