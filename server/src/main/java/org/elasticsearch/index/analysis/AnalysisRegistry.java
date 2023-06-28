/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.unmodifiableMap;

/**
 * An internal registry for tokenizer, token filter, char filter and analyzer.
 * This class exists per node and allows to create per-index {@link IndexAnalyzers} via {@link #build(IndexSettings)}
 */
public final class AnalysisRegistry implements Closeable {
    public static final String INDEX_ANALYSIS_CHAR_FILTER = "index.analysis.char_filter";
    public static final String INDEX_ANALYSIS_FILTER = "index.analysis.filter";
    public static final String INDEX_ANALYSIS_ANALYZER = "index.analysis.analyzer";
    public static final String INDEX_ANALYSIS_TOKENIZER = "index.analysis.tokenizer";

    public static final String DEFAULT_ANALYZER_NAME = "default";
    public static final String DEFAULT_SEARCH_ANALYZER_NAME = "default_search";
    public static final String DEFAULT_SEARCH_QUOTED_ANALYZER_NAME = "default_search_quoted";

    private final PrebuiltAnalysis prebuiltAnalysis;
    private final Map<String, Analyzer> cachedAnalyzer = new ConcurrentHashMap<>();

    private final Environment environment;
    private final Map<String, AnalysisProvider<CharFilterFactory>> charFilters;
    private final Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters;
    private final Map<String, AnalysisProvider<TokenizerFactory>> tokenizers;
    private final Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzers;
    private final Map<String, AnalysisProvider<AnalyzerProvider<?>>> normalizers;

    public AnalysisRegistry(
        Environment environment,
        Map<String, AnalysisProvider<CharFilterFactory>> charFilters,
        Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters,
        Map<String, AnalysisProvider<TokenizerFactory>> tokenizers,
        Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzers,
        Map<String, AnalysisProvider<AnalyzerProvider<?>>> normalizers,
        Map<String, PreConfiguredCharFilter> preConfiguredCharFilters,
        Map<String, PreConfiguredTokenFilter> preConfiguredTokenFilters,
        Map<String, PreConfiguredTokenizer> preConfiguredTokenizers,
        Map<String, PreBuiltAnalyzerProviderFactory> preConfiguredAnalyzers
    ) {
        this.environment = environment;
        this.charFilters = unmodifiableMap(charFilters);
        this.tokenFilters = unmodifiableMap(tokenFilters);
        this.tokenizers = unmodifiableMap(tokenizers);
        this.analyzers = unmodifiableMap(analyzers);
        this.normalizers = unmodifiableMap(normalizers);
        prebuiltAnalysis = new PrebuiltAnalysis(
            preConfiguredCharFilters,
            preConfiguredTokenFilters,
            preConfiguredTokenizers,
            preConfiguredAnalyzers
        );
    }

    private static Settings getSettingsFromIndexSettings(IndexSettings indexSettings, String groupName) {
        Settings settings = indexSettings.getSettings().getAsSettings(groupName);
        if (settings.isEmpty()) {
            settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexSettings.getIndexVersionCreated().id()).build();
        }
        return settings;
    }

    private static final IndexSettings NO_INDEX_SETTINGS = new IndexSettings(
        IndexMetadata.builder(IndexMetadata.INDEX_UUID_NA_VALUE)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfReplicas(0)
            .numberOfShards(1)
            .build(),
        Settings.EMPTY
    );

    private <T> T getComponentFactory(
        IndexSettings settings,
        NameOrDefinition nod,
        String componentType,
        Function<String, AnalysisProvider<T>> globalComponentProvider,
        Function<String, AnalysisProvider<T>> prebuiltComponentProvider,
        BiFunction<String, IndexSettings, AnalysisProvider<T>> indexComponentProvider
    ) throws IOException {
        if (nod.definition != null) {
            // custom component, so we build it from scratch
            String type = nod.definition.get("type");
            if (type == null) {
                throw new IllegalArgumentException("Missing [type] setting for anonymous " + componentType + ": " + nod.definition);
            }
            AnalysisProvider<T> factory = globalComponentProvider.apply(type);
            if (factory == null) {
                throw new IllegalArgumentException("failed to find global " + componentType + " under [" + type + "]");
            }
            if (settings == null) {
                settings = NO_INDEX_SETTINGS;
            }
            return factory.get(settings, environment, "__anonymous__" + type, nod.definition);
        }
        if (settings == null) {
            // no index provided, so we use prebuilt analysis components
            AnalysisProvider<T> factory = prebuiltComponentProvider.apply(nod.name);
            if (factory == null) {
                // if there's no prebuilt component, try loading a global one to build with no settings
                factory = globalComponentProvider.apply(nod.name);
                if (factory == null) {
                    throw new IllegalArgumentException("failed to find global " + componentType + " under [" + nod.name + "]");
                }
            }
            return factory.get(environment, nod.name);
        } else {
            // get the component from index settings
            AnalysisProvider<T> factory = indexComponentProvider.apply(nod.name, settings);
            if (factory == null) {
                throw new IllegalArgumentException("failed to find " + componentType + " under [" + nod.name + "]");
            }
            Settings s = getSettingsFromIndexSettings(settings, "index.analysis." + componentType + "." + nod.name);
            return factory.get(settings, environment, nod.name, s);
        }
    }

    /**
     * Returns a registered {@link TokenizerFactory} provider by name or <code>null</code> if the tokenizer was not registered
     */
    private AnalysisModule.AnalysisProvider<TokenizerFactory> getTokenizerProvider(String tokenizer) {
        return tokenizers.getOrDefault(tokenizer, this.prebuiltAnalysis.getTokenizerFactory(tokenizer));
    }

    /**
     * Returns a registered {@link TokenFilterFactory} provider by name or <code>null</code> if the token filter was not registered
     */
    private AnalysisModule.AnalysisProvider<TokenFilterFactory> getTokenFilterProvider(String tokenFilter) {
        return tokenFilters.getOrDefault(tokenFilter, this.prebuiltAnalysis.getTokenFilterFactory(tokenFilter));
    }

    /**
     * Returns a registered {@link CharFilterFactory} provider by name or <code>null</code> if the char filter was not registered
     */
    private AnalysisModule.AnalysisProvider<CharFilterFactory> getCharFilterProvider(String charFilter) {
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
                }
            });
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
        final Map<String, AnalyzerProvider<?>> analyzerFactories = buildAnalyzerFactories(indexSettings);
        final Map<String, AnalyzerProvider<?>> normalizerFactories = buildNormalizerFactories(indexSettings);
        return build(indexSettings, analyzerFactories, normalizerFactories, tokenizerFactories, charFilterFactories, tokenFilterFactories);
    }

    /**
     * Creates a custom analyzer from a collection of {@link NameOrDefinition} specifications for each component
     *
     * Callers are responsible for closing the returned Analyzer
     */
    public NamedAnalyzer buildCustomAnalyzer(
        IndexSettings indexSettings,
        boolean normalizer,
        NameOrDefinition tokenizer,
        List<NameOrDefinition> charFilters,
        List<NameOrDefinition> tokenFilters
    ) throws IOException {
        TokenizerFactory tokenizerFactory = getComponentFactory(
            indexSettings,
            tokenizer,
            "tokenizer",
            this::getTokenizerProvider,
            prebuiltAnalysis::getTokenizerFactory,
            this::getTokenizerProvider
        );

        List<CharFilterFactory> charFilterFactories = new ArrayList<>();
        for (NameOrDefinition nod : charFilters) {
            charFilterFactories.add(
                getComponentFactory(
                    indexSettings,
                    nod,
                    "char_filter",
                    this::getCharFilterProvider,
                    prebuiltAnalysis::getCharFilterFactory,
                    this::getCharFilterProvider
                )
            );
        }

        List<TokenFilterFactory> tokenFilterFactories = new ArrayList<>();
        for (NameOrDefinition nod : tokenFilters) {
            TokenFilterFactory tff = getComponentFactory(
                indexSettings,
                nod,
                "filter",
                this::getTokenFilterProvider,
                prebuiltAnalysis::getTokenFilterFactory,
                this::getTokenFilterProvider
            );
            if (normalizer && tff instanceof NormalizingTokenFilterFactory == false) {
                throw new IllegalArgumentException("Custom normalizer may not use filter [" + tff.name() + "]");
            }
            tff = tff.getChainAwareTokenFilterFactory(tokenizerFactory, charFilterFactories, tokenFilterFactories, name -> {
                try {
                    return getComponentFactory(
                        indexSettings,
                        new NameOrDefinition(name),
                        "filter",
                        this::getTokenFilterProvider,
                        prebuiltAnalysis::getTokenFilterFactory,
                        this::getTokenFilterProvider
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, false);
            tokenFilterFactories.add(tff);
        }

        Analyzer analyzer = new CustomAnalyzer(
            tokenizerFactory,
            charFilterFactories.toArray(new CharFilterFactory[] {}),
            tokenFilterFactories.toArray(new TokenFilterFactory[] {})
        );
        return produceAnalyzer("__custom__", new AnalyzerProvider<>() {
            @Override
            public String name() {
                return "__custom__";
            }

            @Override
            public AnalyzerScope scope() {
                return AnalyzerScope.GLOBAL;
            }

            @Override
            public Analyzer get() {
                return analyzer;
            }
        }, null, null, null);

    }

    public Map<String, TokenFilterFactory> buildTokenFilterFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> tokenFiltersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_FILTER);
        return buildMapping(
            Component.FILTER,
            indexSettings,
            tokenFiltersSettings,
            this.tokenFilters,
            prebuiltAnalysis.preConfiguredTokenFilters
        );
    }

    public Map<String, TokenizerFactory> buildTokenizerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> tokenizersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_TOKENIZER);
        return buildMapping(Component.TOKENIZER, indexSettings, tokenizersSettings, tokenizers, prebuiltAnalysis.preConfiguredTokenizers);
    }

    public Map<String, CharFilterFactory> buildCharFilterFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> charFiltersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_CHAR_FILTER);
        return buildMapping(
            Component.CHAR_FILTER,
            indexSettings,
            charFiltersSettings,
            charFilters,
            prebuiltAnalysis.preConfiguredCharFilterFactories
        );
    }

    private Map<String, AnalyzerProvider<?>> buildAnalyzerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> analyzersSettings = indexSettings.getSettings().getGroups("index.analysis.analyzer");
        return buildMapping(Component.ANALYZER, indexSettings, analyzersSettings, analyzers, prebuiltAnalysis.analyzerProviderFactories);
    }

    private Map<String, AnalyzerProvider<?>> buildNormalizerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> normalizersSettings = indexSettings.getSettings().getGroups("index.analysis.normalizer");
        return buildMapping(Component.NORMALIZER, indexSettings, normalizersSettings, normalizers, Collections.emptyMap());
    }

    /**
     * Returns a registered {@link TokenizerFactory} provider by {@link IndexSettings}
     *  or a registered {@link TokenizerFactory} provider by predefined name
     *  or <code>null</code> if the tokenizer was not registered
     * @param tokenizer global or defined tokenizer name
     * @param indexSettings an index settings
     * @return {@link TokenizerFactory} provider or <code>null</code>
     */
    private AnalysisProvider<TokenizerFactory> getTokenizerProvider(String tokenizer, IndexSettings indexSettings) {
        return getProvider(
            Component.TOKENIZER,
            tokenizer,
            indexSettings,
            "index.analysis.tokenizer",
            tokenizers,
            this::getTokenizerProvider
        );
    }

    /**
     * Returns a registered {@link TokenFilterFactory} provider by {@link IndexSettings}
     *  or a registered {@link TokenFilterFactory} provider by predefined name
     *  or <code>null</code> if the tokenFilter was not registered
     * @param tokenFilter global or defined tokenFilter name
     * @param indexSettings an index settings
     * @return {@link TokenFilterFactory} provider or <code>null</code>
     */
    private AnalysisProvider<TokenFilterFactory> getTokenFilterProvider(String tokenFilter, IndexSettings indexSettings) {
        return getProvider(
            Component.FILTER,
            tokenFilter,
            indexSettings,
            "index.analysis.filter",
            tokenFilters,
            this::getTokenFilterProvider
        );
    }

    /**
     * Returns a registered {@link CharFilterFactory} provider by {@link IndexSettings}
     *  or a registered {@link CharFilterFactory} provider by predefined name
     *  or <code>null</code> if the charFilter was not registered
     * @param charFilter global or defined charFilter name
     * @param indexSettings an index settings
     * @return {@link CharFilterFactory} provider or <code>null</code>
     */
    private AnalysisProvider<CharFilterFactory> getCharFilterProvider(String charFilter, IndexSettings indexSettings) {
        return getProvider(
            Component.CHAR_FILTER,
            charFilter,
            indexSettings,
            "index.analysis.char_filter",
            charFilters,
            this::getCharFilterProvider
        );
    }

    private static <T> AnalysisProvider<T> getProvider(
        Component componentType,
        String componentName,
        IndexSettings indexSettings,
        String componentSettings,
        Map<String, AnalysisProvider<T>> providers,
        Function<String, AnalysisProvider<T>> providerFunction
    ) {
        final Map<String, Settings> subSettings = indexSettings.getSettings().getGroups(componentSettings);
        if (subSettings.containsKey(componentName)) {
            Settings currentSettings = subSettings.get(componentName);
            return getAnalysisProvider(componentType, providers, componentName, currentSettings.get("type"));
        } else {
            return providerFunction.apply(componentName);
        }
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

    @SuppressWarnings("unchecked")
    private <T> Map<String, T> buildMapping(
        Component component,
        IndexSettings settings,
        Map<String, Settings> settingsMap,
        Map<String, ? extends AnalysisModule.AnalysisProvider<T>> providerMap,
        Map<String, ? extends AnalysisModule.AnalysisProvider<T>> defaultInstance
    ) throws IOException {
        Settings defaultSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, settings.getIndexVersionCreated().id())
            .build();
        Map<String, T> factories = new HashMap<>();
        for (Map.Entry<String, Settings> entry : settingsMap.entrySet()) {
            String name = entry.getKey();
            Settings currentSettings = entry.getValue();
            String typeName = currentSettings.get("type");
            if (component == Component.ANALYZER) {
                T factory = null;
                if (typeName == null) {
                    if (currentSettings.get("tokenizer") != null) {
                        factory = (T) new CustomAnalyzerProvider(settings, name, currentSettings);
                    } else {
                        throw new IllegalArgumentException(
                            component + " [" + name + "] " + "must specify either an analyzer type, or a tokenizer"
                        );
                    }
                } else if (typeName.equals("custom")) {
                    factory = (T) new CustomAnalyzerProvider(settings, name, currentSettings);
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
        for (Map.Entry<String, ? extends AnalysisProvider<T>> entry : providerMap.entrySet()) {
            String name = entry.getKey();
            AnalysisProvider<T> provider = entry.getValue();
            // we don't want to re-register one that already exists
            if (settingsMap.containsKey(name)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (provider.requiresAnalysisSettings()) {
                continue;
            }
            AnalysisProvider<T> defaultProvider = defaultInstance.get(name);
            final T instance;
            if (defaultProvider == null) {
                instance = provider.get(settings, environment, name, defaultSettings);
            } else {
                instance = defaultProvider.get(settings, environment, name, defaultSettings);
            }
            factories.put(name, instance);
        }

        for (Map.Entry<String, ? extends AnalysisProvider<T>> entry : defaultInstance.entrySet()) {
            final String name = entry.getKey();
            final AnalysisProvider<T> provider = entry.getValue();
            factories.putIfAbsent(name, provider.get(settings, environment, name, defaultSettings));
        }
        return factories;
    }

    private static <T> AnalysisProvider<T> getAnalysisProvider(
        Component component,
        Map<String, ? extends AnalysisProvider<T>> providerMap,
        String name,
        String typeName
    ) {
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

        final Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzerProviderFactories;
        final Map<String, ? extends AnalysisProvider<TokenFilterFactory>> preConfiguredTokenFilters;
        final Map<String, ? extends AnalysisProvider<TokenizerFactory>> preConfiguredTokenizers;
        final Map<String, ? extends AnalysisProvider<CharFilterFactory>> preConfiguredCharFilterFactories;

        private PrebuiltAnalysis(
            Map<String, PreConfiguredCharFilter> preConfiguredCharFilters,
            Map<String, PreConfiguredTokenFilter> preConfiguredTokenFilters,
            Map<String, PreConfiguredTokenizer> preConfiguredTokenizers,
            Map<String, PreBuiltAnalyzerProviderFactory> preConfiguredAnalyzers
        ) {

            Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories = new HashMap<>();
            analyzerProviderFactories.putAll(preConfiguredAnalyzers);
            // Pre-build analyzers
            for (PreBuiltAnalyzers preBuiltAnalyzerEnum : PreBuiltAnalyzers.values()) {
                String name = preBuiltAnalyzerEnum.name().toLowerCase(Locale.ROOT);
                analyzerProviderFactories.put(name, new PreBuiltAnalyzerProviderFactory(name, preBuiltAnalyzerEnum));
            }

            this.analyzerProviderFactories = Collections.unmodifiableMap(analyzerProviderFactories);
            this.preConfiguredCharFilterFactories = preConfiguredCharFilters;
            this.preConfiguredTokenFilters = preConfiguredTokenFilters;
            this.preConfiguredTokenizers = preConfiguredTokenizers;
        }

        AnalysisProvider<CharFilterFactory> getCharFilterFactory(String name) {
            return preConfiguredCharFilterFactories.get(name);
        }

        AnalysisProvider<TokenFilterFactory> getTokenFilterFactory(String name) {
            return preConfiguredTokenFilters.get(name);
        }

        AnalysisProvider<TokenizerFactory> getTokenizerFactory(String name) {
            return preConfiguredTokenizers.get(name);
        }

        AnalysisProvider<AnalyzerProvider<?>> getAnalyzerProvider(String name) {
            return analyzerProviderFactories.get(name);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(analyzerProviderFactories.values().stream().map((a) -> ((PreBuiltAnalyzerProviderFactory) a)).toList());
        }
    }

    public static IndexAnalyzers build(
        IndexSettings indexSettings,
        Map<String, AnalyzerProvider<?>> analyzerProviders,
        Map<String, AnalyzerProvider<?>> normalizerProviders,
        Map<String, TokenizerFactory> tokenizerFactoryFactories,
        Map<String, CharFilterFactory> charFilterFactoryFactories,
        Map<String, TokenFilterFactory> tokenFilterFactoryFactories
    ) {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        Map<String, NamedAnalyzer> normalizers = new HashMap<>();
        Map<String, NamedAnalyzer> whitespaceNormalizers = new HashMap<>();
        for (Map.Entry<String, AnalyzerProvider<?>> entry : analyzerProviders.entrySet()) {
            analyzers.merge(
                entry.getKey(),
                produceAnalyzer(
                    entry.getKey(),
                    entry.getValue(),
                    tokenFilterFactoryFactories,
                    charFilterFactoryFactories,
                    tokenizerFactoryFactories
                ),
                (k, v) -> {
                    throw new IllegalStateException("already registered analyzer with name: " + entry.getKey());
                }
            );
        }
        for (Map.Entry<String, AnalyzerProvider<?>> entry : normalizerProviders.entrySet()) {
            processNormalizerFactory(
                entry.getKey(),
                entry.getValue(),
                normalizers,
                TokenizerFactory.newFactory("keyword", KeywordTokenizer::new),
                tokenFilterFactoryFactories,
                charFilterFactoryFactories
            );
            processNormalizerFactory(
                entry.getKey(),
                entry.getValue(),
                whitespaceNormalizers,
                TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                tokenFilterFactoryFactories,
                charFilterFactoryFactories
            );
        }

        for (Analyzer analyzer : normalizers.values()) {
            analyzer.normalize("", ""); // check for deprecations
        }

        if (analyzers.containsKey(DEFAULT_ANALYZER_NAME) == false) {
            analyzers.put(
                DEFAULT_ANALYZER_NAME,
                produceAnalyzer(
                    DEFAULT_ANALYZER_NAME,
                    new StandardAnalyzerProvider(indexSettings, null, DEFAULT_ANALYZER_NAME, Settings.EMPTY),
                    tokenFilterFactoryFactories,
                    charFilterFactoryFactories,
                    tokenizerFactoryFactories
                )
            );
        }
        NamedAnalyzer defaultAnalyzer = analyzers.get(DEFAULT_ANALYZER_NAME);
        if (defaultAnalyzer == null) {
            throw new IllegalArgumentException("no default analyzer configured");
        }
        defaultAnalyzer.checkAllowedInMode(AnalysisMode.ALL);
        assert Objects.equals(defaultAnalyzer.name(), DEFAULT_ANALYZER_NAME);
        if (Objects.equals(defaultAnalyzer.name(), DEFAULT_ANALYZER_NAME) == false) {
            throw new IllegalStateException("default analyzer must have the name [default] but was: [" + defaultAnalyzer.name() + "]");
        }

        if (analyzers.containsKey("default_index")) {
            throw new IllegalArgumentException(
                "setting [index.analysis.analyzer.default_index] is not supported anymore, use "
                    + "[index.analysis.analyzer.default] instead for index ["
                    + indexSettings.getIndex().getName()
                    + "]"
            );
        }

        for (Map.Entry<String, NamedAnalyzer> analyzer : analyzers.entrySet()) {
            if (analyzer.getKey().startsWith("_")) {
                throw new IllegalArgumentException("analyzer name must not start with '_'. got \"" + analyzer.getKey() + "\"");
            }
        }
        return IndexAnalyzers.of(analyzers, normalizers, whitespaceNormalizers);
    }

    private static NamedAnalyzer produceAnalyzer(
        String name,
        AnalyzerProvider<?> analyzerFactory,
        Map<String, TokenFilterFactory> tokenFilters,
        Map<String, CharFilterFactory> charFilters,
        Map<String, TokenizerFactory> tokenizers
    ) {
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
        checkVersions(analyzer);
        return analyzer;
    }

    private static void processNormalizerFactory(
        String name,
        AnalyzerProvider<?> normalizerFactory,
        Map<String, NamedAnalyzer> normalizers,
        TokenizerFactory tokenizerFactory,
        Map<String, TokenFilterFactory> tokenFilters,
        Map<String, CharFilterFactory> charFilters
    ) {
        if (tokenizerFactory == null) {
            throw new IllegalStateException("keyword tokenizer factory is null, normalizers require analysis-common module");
        }

        if (normalizerFactory instanceof CustomNormalizerProvider) {
            ((CustomNormalizerProvider) normalizerFactory).build(tokenizerFactory, charFilters, tokenFilters);
        }
        if (normalizers.containsKey(name)) {
            throw new IllegalStateException("already registered analyzer with name: " + name);
        }
        Analyzer normalizerF = normalizerFactory.get();
        if (normalizerF == null) {
            throw new IllegalArgumentException("normalizer [" + normalizerFactory.name() + "] created null normalizer");
        }
        NamedAnalyzer normalizer = new NamedAnalyzer(name, normalizerFactory.scope(), normalizerF);
        normalizers.put(name, normalizer);
    }

    // Some analysis components emit deprecation warnings or throw exceptions when used
    // with the wrong version of elasticsearch. These exceptions and warnings are
    // normally thrown when tokenstreams are constructed, which unless we build a
    // tokenstream up-front does not happen until a document is indexed. In order to
    // surface these warnings or exceptions as early as possible, we build an empty
    // tokenstream and pull it through an Analyzer at construction time.
    private static void checkVersions(Analyzer analyzer) {
        try (TokenStream ts = analyzer.tokenStream("", "")) {
            ts.reset();
            while (ts.incrementToken()) {
            }
            ts.end();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
