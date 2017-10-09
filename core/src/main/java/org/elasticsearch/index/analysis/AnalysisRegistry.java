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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * An internal registry for tokenizer, token filter, char filter and analyzer.
 * This class exists per node and allows to create per-index {@link IndexAnalyzers} via {@link #build(IndexSettings)}
 */
public final class AnalysisRegistry implements Closeable {
    public static final String INDEX_ANALYSIS_CHAR_FILTER = "index.analysis.char_filter";
    public static final String INDEX_ANALYSIS_FILTER = "index.analysis.filter";
    public static final String INDEX_ANALYSIS_TOKENIZER = "index.analysis.tokenizer";
    private final PrebuiltAnalysis prebuiltAnalysis;
    private final Map<String, Analyzer> cachedAnalyzer = new ConcurrentHashMap<>();

    private final Environment environment;
    private final Map<String, AnalysisProvider<CharFilterFactory>> charFilters;
    private final Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters;
    private final Map<String, AnalysisProvider<TokenizerFactory>> tokenizers;
    private final Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzers;
    private final Map<String, AnalysisProvider<AnalyzerProvider<?>>> normalizers;

    public AnalysisRegistry(Environment environment,
                            Map<String, AnalysisProvider<CharFilterFactory>> charFilters,
                            Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters,
                            Map<String, AnalysisProvider<TokenizerFactory>> tokenizers,
                            Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzers,
                            Map<String, AnalysisProvider<AnalyzerProvider<?>>> normalizers,
                            Map<String, PreConfiguredCharFilter> preConfiguredCharFilters,
                            Map<String, PreConfiguredTokenFilter> preConfiguredTokenFilters,
                            Map<String, PreConfiguredTokenizer> preConfiguredTokenizers) {
        this.environment = environment;
        this.charFilters = unmodifiableMap(charFilters);
        this.tokenFilters = unmodifiableMap(tokenFilters);
        this.tokenizers = unmodifiableMap(tokenizers);
        this.analyzers = unmodifiableMap(analyzers);
        this.normalizers = unmodifiableMap(normalizers);
        prebuiltAnalysis = new PrebuiltAnalysis(preConfiguredCharFilters, preConfiguredTokenFilters, preConfiguredTokenizers);
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
     * Creates an index-level {@link IndexAnalyzers} from this registry using the given index settings
     */
    public IndexAnalyzers build(IndexSettings indexSettings) throws IOException {

        final Map<String, CharFilterFactory> charFilterFactories = buildCharFilterFactories(indexSettings);
        final Map<String, TokenizerFactory> tokenizerFactories = buildTokenizerFactories(indexSettings);
        final Map<String, TokenFilterFactory> tokenFilterFactories = buildTokenFilterFactories(indexSettings);
        final Map<String, AnalyzerProvider<?>> analyzierFactories = buildAnalyzerFactories(indexSettings);
        final Map<String, AnalyzerProvider<?>> normalizerFactories = buildNormalizerFactories(indexSettings);
        return build(indexSettings, analyzierFactories, normalizerFactories, tokenizerFactories, charFilterFactories, tokenFilterFactories);
    }

    public Map<String, TokenFilterFactory> buildTokenFilterFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> tokenFiltersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_FILTER);
        Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> tokenFilters = new HashMap<>(this.tokenFilters);
        /*
         * synonym and synonym_graph are different than everything else since they need access to the tokenizer factories for the index.
         * instead of building the infrastructure for plugins we rather make it a real exception to not pollute the general interface and
         * hide internal data-structures as much as possible.
         */
        tokenFilters.put("synonym", requiresAnalysisSettings((is, env, name, settings) -> new SynonymTokenFilterFactory(is, env, this, name, settings)));
        tokenFilters.put("synonym_graph", requiresAnalysisSettings((is, env, name, settings) -> new SynonymGraphTokenFilterFactory(is, env, this, name, settings)));
        return buildMapping(Component.FILTER, indexSettings, tokenFiltersSettings, Collections.unmodifiableMap(tokenFilters), prebuiltAnalysis.preConfiguredTokenFilters);
    }

    public Map<String, TokenizerFactory> buildTokenizerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> tokenizersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_TOKENIZER);
        return buildMapping(Component.TOKENIZER, indexSettings, tokenizersSettings, tokenizers, prebuiltAnalysis.preConfiguredTokenizers);
    }

    public Map<String, CharFilterFactory> buildCharFilterFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> charFiltersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_CHAR_FILTER);
        return buildMapping(Component.CHAR_FILTER, indexSettings, charFiltersSettings, charFilters, prebuiltAnalysis.preConfiguredCharFilterFactories);
    }

    public Map<String, AnalyzerProvider<?>> buildAnalyzerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> analyzersSettings = indexSettings.getSettings().getGroups("index.analysis.analyzer");
        return buildMapping(Component.ANALYZER, indexSettings, analyzersSettings, analyzers, prebuiltAnalysis.analyzerProviderFactories);
    }

    public Map<String, AnalyzerProvider<?>> buildNormalizerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> noralizersSettings = indexSettings.getSettings().getGroups("index.analysis.normalizer");
        // TODO: Have pre-built normalizers
        return buildMapping(Component.NORMALIZER, indexSettings, noralizersSettings, normalizers, Collections.emptyMap());
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
            return getAnalysisProvider(Component.TOKENIZER, tokenizers, tokenizer, currentSettings.get("type"));
        } else {
            return getTokenizerProvider(tokenizer);
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
             * synonym and synonym_graph are different than everything else since they need access to the tokenizer factories for the index.
             * instead of building the infrastructure for plugins we rather make it a real exception to not pollute the general interface and
             * hide internal data-structures as much as possible.
             */
            if ("synonym".equals(typeName)) {
                return requiresAnalysisSettings((is, env, name, settings) -> new SynonymTokenFilterFactory(is, env, this, name, settings));
            } else if ("synonym_graph".equals(typeName)) {
                return requiresAnalysisSettings((is, env, name, settings) -> new SynonymGraphTokenFilterFactory(is, env, this, name, settings));
            } else {
                return getAnalysisProvider(Component.FILTER, tokenFilters, tokenFilter, typeName);
            }
        } else {
            return getTokenFilterProvider(tokenFilter);
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
            return getAnalysisProvider(Component.CHAR_FILTER, charFilters, charFilter, currentSettings.get("type"));
        } else {
            return getCharFilterProvider(charFilter);
        }
    }

    private static <T> AnalysisModule.AnalysisProvider<T> requiresAnalysisSettings(AnalysisModule.AnalysisProvider<T> provider) {
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

    enum Component {
        ANALYZER {
            @Override
            public String toString() {
                return "analyzer";
            }
        },
        NORMALIZER {
            @Override
            public String toString() {
                return "normalizer";
            }
        },
        CHAR_FILTER {
            @Override
            public String toString() {
                return "char_filter";
            }
        },
        TOKENIZER {
            @Override
            public String toString() {
                return "tokenizer";
            }
        },
        FILTER {
            @Override
            public String toString() {
                return "filter";
            }
        };
    }

    private <T> Map<String, T> buildMapping(Component component, IndexSettings settings, Map<String, Settings> settingsMap,
                    Map<String, ? extends AnalysisModule.AnalysisProvider<T>> providerMap,
                    Map<String, ? extends AnalysisModule.AnalysisProvider<T>> defaultInstance) throws IOException {
        Settings defaultSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, settings.getIndexVersionCreated()).build();
        Map<String, T> factories = new HashMap<>();
        for (Map.Entry<String, Settings> entry : settingsMap.entrySet()) {
            String name = entry.getKey();
            Settings currentSettings = entry.getValue();
            String typeName = currentSettings.get("type");
            if (component == Component.ANALYZER) {
                T factory = null;
                if (typeName == null) {
                    if (currentSettings.get("tokenizer") != null) {
                        factory = (T) new CustomAnalyzerProvider(settings, name, currentSettings, environment);
                    } else {
                        throw new IllegalArgumentException(component + " [" + name + "] must specify either an analyzer type, or a tokenizer");
                    }
                } else if (typeName.equals("custom")) {
                    factory = (T) new CustomAnalyzerProvider(settings, name, currentSettings, environment);
                }
                if (factory != null) {
                    factories.put(name, factory);
                    continue;
                }
            } else if (component == Component.NORMALIZER) {
                if (typeName == null || typeName.equals("custom")) {
                    T factory = (T) new CustomNormalizerProvider(settings, name, currentSettings);
                    factories.put(name, factory);
                    continue;
                }
            }
            AnalysisProvider<T> type = getAnalysisProvider(component, providerMap, name, typeName);
            if (type == null) {
                throw new IllegalArgumentException("Unknown " + component + " type [" + typeName + "] for [" + name + "]");
            }
            final T factory = type.get(settings, environment, name, currentSettings);
            factories.put(name, factory);

        }
        // go over the char filters in the bindings and register the ones that are not configured
        for (Map.Entry<String, ? extends AnalysisModule.AnalysisProvider<T>> entry : providerMap.entrySet()) {
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

        for (Map.Entry<String, ? extends AnalysisModule.AnalysisProvider<T>> entry : defaultInstance.entrySet()) {
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

    private <T> AnalysisProvider<T> getAnalysisProvider(Component component, Map<String, ? extends AnalysisProvider<T>> providerMap,
            String name, String typeName) {
        if (typeName == null) {
            throw new IllegalArgumentException(component + " [" + name + "] must specify either an analyzer type, or a tokenizer");
        }
        AnalysisProvider<T> type = providerMap.get(typeName);
        if (type == null) {
            throw new IllegalArgumentException("Unknown " + component + " type [" + typeName + "] for [" + name + "]");
        }
        return type;
    }

    private static class PrebuiltAnalysis implements Closeable {

        final Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<?>>> analyzerProviderFactories;
        final Map<String, ? extends AnalysisProvider<TokenFilterFactory>> preConfiguredTokenFilters;
        final Map<String, ? extends AnalysisProvider<TokenizerFactory>> preConfiguredTokenizers;
        final Map<String, ? extends AnalysisProvider<CharFilterFactory>> preConfiguredCharFilterFactories;

        private PrebuiltAnalysis(
                Map<String, PreConfiguredCharFilter> preConfiguredCharFilters,
                Map<String, PreConfiguredTokenFilter> preConfiguredTokenFilters,
                Map<String, PreConfiguredTokenizer> preConfiguredTokenizers) {
            Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories = new HashMap<>();

            // Analyzers
            for (PreBuiltAnalyzers preBuiltAnalyzerEnum : PreBuiltAnalyzers.values()) {
                String name = preBuiltAnalyzerEnum.name().toLowerCase(Locale.ROOT);
                analyzerProviderFactories.put(name, new PreBuiltAnalyzerProviderFactory(name, AnalyzerScope.INDICES, preBuiltAnalyzerEnum.getAnalyzer(Version.CURRENT)));
            }

            this.analyzerProviderFactories = Collections.unmodifiableMap(analyzerProviderFactories);
            this.preConfiguredCharFilterFactories = preConfiguredCharFilters;
            this.preConfiguredTokenFilters = preConfiguredTokenFilters;
            this.preConfiguredTokenizers = preConfiguredTokenizers;
        }

        public AnalysisModule.AnalysisProvider<CharFilterFactory> getCharFilterFactory(String name) {
            return preConfiguredCharFilterFactories.get(name);
        }

        public AnalysisModule.AnalysisProvider<TokenFilterFactory> getTokenFilterFactory(String name) {
            return preConfiguredTokenFilters.get(name);
        }

        public AnalysisModule.AnalysisProvider<TokenizerFactory> getTokenizerFactory(String name) {
            return preConfiguredTokenizers.get(name);
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

    public IndexAnalyzers build(IndexSettings indexSettings,
                                Map<String, AnalyzerProvider<?>> analyzerProviders,
                                Map<String, AnalyzerProvider<?>> normalizerProviders,
                                Map<String, TokenizerFactory> tokenizerFactoryFactories,
                                Map<String, CharFilterFactory> charFilterFactoryFactories,
                                Map<String, TokenFilterFactory> tokenFilterFactoryFactories) {

        Index index = indexSettings.getIndex();
        analyzerProviders = new HashMap<>(analyzerProviders);
        Logger logger = Loggers.getLogger(getClass(), indexSettings.getSettings());
        DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
        Map<String, NamedAnalyzer> analyzerAliases = new HashMap<>();
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        Map<String, NamedAnalyzer> normalizers = new HashMap<>();
        for (Map.Entry<String, AnalyzerProvider<?>> entry : analyzerProviders.entrySet()) {
            processAnalyzerFactory(deprecationLogger, indexSettings, entry.getKey(), entry.getValue(), analyzerAliases, analyzers,
                tokenFilterFactoryFactories, charFilterFactoryFactories, tokenizerFactoryFactories);
        }
        for (Map.Entry<String, AnalyzerProvider<?>> entry : normalizerProviders.entrySet()) {
            processNormalizerFactory(deprecationLogger, indexSettings, entry.getKey(), entry.getValue(), normalizers,
                    tokenizerFactoryFactories.get("keyword"), tokenFilterFactoryFactories, charFilterFactoryFactories);
        }
        for (Map.Entry<String, NamedAnalyzer> entry : analyzerAliases.entrySet()) {
            String key = entry.getKey();
            if (analyzers.containsKey(key) &&
                ("default".equals(key) || "default_search".equals(key) || "default_search_quoted".equals(key)) == false) {
                throw new IllegalStateException("already registered analyzer with name: " + key);
            } else {
                NamedAnalyzer configured = entry.getValue();
                analyzers.put(key, configured);
            }
        }

        if (!analyzers.containsKey("default")) {
            processAnalyzerFactory(deprecationLogger, indexSettings, "default", new StandardAnalyzerProvider(indexSettings, null, "default", Settings.Builder.EMPTY_SETTINGS),
                analyzerAliases, analyzers, tokenFilterFactoryFactories, charFilterFactoryFactories, tokenizerFactoryFactories);
        }
        if (!analyzers.containsKey("default_search")) {
            analyzers.put("default_search", analyzers.get("default"));
        }
        if (!analyzers.containsKey("default_search_quoted")) {
            analyzers.put("default_search_quoted", analyzers.get("default_search"));
        }


        NamedAnalyzer defaultAnalyzer = analyzers.get("default");
        if (defaultAnalyzer == null) {
            throw new IllegalArgumentException("no default analyzer configured");
        }
        if (analyzers.containsKey("default_index")) {
            final Version createdVersion = indexSettings.getIndexVersionCreated();
            if (createdVersion.onOrAfter(Version.V_5_0_0_alpha1)) {
                throw new IllegalArgumentException("setting [index.analysis.analyzer.default_index] is not supported anymore, use [index.analysis.analyzer.default] instead for index [" + index.getName() + "]");
            } else {
                deprecationLogger.deprecated("setting [index.analysis.analyzer.default_index] is deprecated, use [index.analysis.analyzer.default] instead for index [{}]", index.getName());
            }
        }
        NamedAnalyzer defaultIndexAnalyzer = analyzers.containsKey("default_index") ? analyzers.get("default_index") : defaultAnalyzer;
        NamedAnalyzer defaultSearchAnalyzer = analyzers.containsKey("default_search") ? analyzers.get("default_search") : defaultAnalyzer;
        NamedAnalyzer defaultSearchQuoteAnalyzer = analyzers.containsKey("default_search_quote") ? analyzers.get("default_search_quote") : defaultSearchAnalyzer;

        for (Map.Entry<String, NamedAnalyzer> analyzer : analyzers.entrySet()) {
            if (analyzer.getKey().startsWith("_")) {
                throw new IllegalArgumentException("analyzer name must not start with '_'. got \"" + analyzer.getKey() + "\"");
            }
        }
        return new IndexAnalyzers(indexSettings, defaultIndexAnalyzer, defaultSearchAnalyzer, defaultSearchQuoteAnalyzer,
            unmodifiableMap(analyzers), unmodifiableMap(normalizers));
    }

    private void processAnalyzerFactory(DeprecationLogger deprecationLogger,
                                        IndexSettings indexSettings,
                                        String name,
                                        AnalyzerProvider<?> analyzerFactory,
                                        Map<String, NamedAnalyzer> analyzerAliases,
                                        Map<String, NamedAnalyzer> analyzers, Map<String, TokenFilterFactory> tokenFilters,
                                        Map<String, CharFilterFactory> charFilters, Map<String, TokenizerFactory> tokenizers) {
        /*
         * Lucene defaults positionIncrementGap to 0 in all analyzers but
         * Elasticsearch defaults them to 0 only before version 2.0
         * and 100 afterwards so we override the positionIncrementGap if it
         * doesn't match here.
         */
        int overridePositionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
        if (analyzerFactory instanceof CustomAnalyzerProvider) {
            ((CustomAnalyzerProvider) analyzerFactory).build(tokenizers, charFilters, tokenFilters);
            /*
             * Custom analyzers already default to the correct, version
             * dependent positionIncrementGap and the user is be able to
             * configure the positionIncrementGap directly on the analyzer so
             * we disable overriding the positionIncrementGap to preserve the
             * user's setting.
             */
            overridePositionIncrementGap = Integer.MIN_VALUE;
        }
        Analyzer analyzerF = analyzerFactory.get();
        if (analyzerF == null) {
            throw new IllegalArgumentException("analyzer [" + analyzerFactory.name() + "] created null analyzer");
        }
        NamedAnalyzer analyzer;
        if (analyzerF instanceof NamedAnalyzer) {
            // if we got a named analyzer back, use it...
            analyzer = (NamedAnalyzer) analyzerF;
            if (overridePositionIncrementGap >= 0 && analyzer.getPositionIncrementGap(analyzer.name()) != overridePositionIncrementGap) {
                // unless the positionIncrementGap needs to be overridden
                analyzer = new NamedAnalyzer(analyzer, overridePositionIncrementGap);
            }
        } else {
            analyzer = new NamedAnalyzer(name, analyzerFactory.scope(), analyzerF, overridePositionIncrementGap);
        }
        if (analyzers.containsKey(name)) {
            throw new IllegalStateException("already registered analyzer with name: " + name);
        }
        analyzers.put(name, analyzer);
        // TODO: remove alias support completely when we no longer support pre 5.0 indices
        final String analyzerAliasKey = "index.analysis.analyzer." + analyzerFactory.name() + ".alias";
        if (indexSettings.getSettings().get(analyzerAliasKey) != null) {
            if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_5_0_0_beta1)) {
                // do not allow alias creation if the index was created on or after v5.0 alpha6
                throw new IllegalArgumentException("setting [" + analyzerAliasKey + "] is not supported");
            }

            // the setting is now removed but we only support it for loading indices created before v5.0
            deprecationLogger.deprecated("setting [{}] is only allowed on index [{}] because it was created before 5.x; " +
                "analyzer aliases can no longer be created on new indices.", analyzerAliasKey, indexSettings.getIndex().getName());
            Set<String> aliases = Sets.newHashSet(indexSettings.getSettings().getAsList(analyzerAliasKey));
            for (String alias : aliases) {
                if (analyzerAliases.putIfAbsent(alias, analyzer) != null) {
                    throw new IllegalStateException("alias [" + alias + "] is already used by [" + analyzerAliases.get(alias).name() + "]");
                }
            }
        }
    }

    private void processNormalizerFactory(DeprecationLogger deprecationLogger,
            IndexSettings indexSettings,
            String name,
            AnalyzerProvider<?> normalizerFactory,
            Map<String, NamedAnalyzer> normalizers,
            TokenizerFactory keywordTokenizerFactory,
            Map<String, TokenFilterFactory> tokenFilters,
            Map<String, CharFilterFactory> charFilters) {
        if (normalizerFactory instanceof CustomNormalizerProvider) {
            ((CustomNormalizerProvider) normalizerFactory).build(keywordTokenizerFactory, charFilters, tokenFilters);
        }
        Analyzer normalizerF = normalizerFactory.get();
        if (normalizerF == null) {
            throw new IllegalArgumentException("normalizer [" + normalizerFactory.name() + "] created null normalizer");
        }
        NamedAnalyzer normalizer = new NamedAnalyzer(name, normalizerFactory.scope(), normalizerF);
        if (normalizers.containsKey(name)) {
            throw new IllegalStateException("already registered analyzer with name: " + name);
        }
        normalizers.put(name, normalizer);
    }
}
