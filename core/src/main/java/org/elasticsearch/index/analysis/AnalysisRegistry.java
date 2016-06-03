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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.analysis.compound.HyphenationCompoundWordTokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.HunspellService;
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

/**
 * An internal registry for tokenizer, token filter, char filter and analyzer.
 * This class exists per node and allows to create per-index {@link AnalysisService} via {@link #build(IndexSettings)}
 */
public final class AnalysisRegistry implements Closeable {
    private final Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> charFilters;
    private final Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> tokenFilters;
    private final Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> tokenizers;
    private final Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider>> analyzers;
    private final Map<String, Analyzer> cachedAnalyzer = new ConcurrentHashMap<>();
    private final PrebuiltAnalysis prebuiltAnalysis;
    private final HunspellService hunspellService;
    private final Environment environment;

    public AnalysisRegistry(HunspellService hunspellService, Environment environment) {
        this(hunspellService, environment, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    public AnalysisRegistry(HunspellService hunspellService, Environment environment,
                            Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> charFilters,
                            Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> tokenFilters,
                            Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> tokenizers,
                            Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider>> analyzers) {
        prebuiltAnalysis = new PrebuiltAnalysis();
        this.hunspellService = hunspellService;
        this.environment = environment;
        final Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> charFilterBuilder = new HashMap<>(charFilters);
        final Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> tokenFilterBuilder = new HashMap<>(tokenFilters);
        final Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> tokenizerBuilder = new HashMap<>(tokenizers);
        final Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider>> analyzerBuilder= new HashMap<>(analyzers);
        registerBuiltInAnalyzer(analyzerBuilder);
        registerBuiltInCharFilter(charFilterBuilder);
        registerBuiltInTokenizer(tokenizerBuilder);
        registerBuiltInTokenFilters(tokenFilterBuilder);
        this.tokenFilters = Collections.unmodifiableMap(tokenFilterBuilder);
        this.tokenizers = Collections.unmodifiableMap(tokenizerBuilder);
        this.charFilters = Collections.unmodifiableMap(charFilterBuilder);
        this.analyzers = Collections.unmodifiableMap(analyzerBuilder);
    }

    public HunspellService getHunspellService() {
        return hunspellService;
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
        AnalysisModule.AnalysisProvider<AnalyzerProvider> analyzerProvider = this.prebuiltAnalysis.getAnalyzerProvider(analyzer);
        if (analyzerProvider == null) {
            AnalysisModule.AnalysisProvider<AnalyzerProvider> provider = analyzers.get(analyzer);
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
        final Map<String, Settings> charFiltersSettings = indexSettings.getSettings().getGroups("index.analysis.char_filter");
        final Map<String, Settings> tokenFiltersSettings = indexSettings.getSettings().getGroups("index.analysis.filter");
        final Map<String, Settings> tokenizersSettings = indexSettings.getSettings().getGroups("index.analysis.tokenizer");
        final Map<String, Settings> analyzersSettings = indexSettings.getSettings().getGroups("index.analysis.analyzer");

        final Map<String, CharFilterFactory> charFilterFactories = buildMapping(false, "charfilter", indexSettings, charFiltersSettings, charFilters, prebuiltAnalysis.charFilterFactories);
        final Map<String, TokenizerFactory> tokenizerFactories = buildMapping(false, "tokenizer", indexSettings, tokenizersSettings, tokenizers, prebuiltAnalysis.tokenizerFactories);

        Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> tokenFilters = new HashMap<>(this.tokenFilters);
        /*
         * synonym is different than everything else since it needs access to the tokenizer factories for this index.
         * instead of building the infrastructure for plugins we rather make it a real exception to not pollute the general interface and
         * hide internal data-structures as much as possible.
         */
        tokenFilters.put("synonym", requriesAnalysisSettings((is, env, name, settings) -> new SynonymTokenFilterFactory(is, env, tokenizerFactories, name, settings)));
        final Map<String, TokenFilterFactory> tokenFilterFactories = buildMapping(false, "tokenfilter", indexSettings, tokenFiltersSettings, Collections.unmodifiableMap(tokenFilters), prebuiltAnalysis.tokenFilterFactories);
        final Map<String, AnalyzerProvider> analyzierFactories = buildMapping(true, "analyzer", indexSettings, analyzersSettings, analyzers, prebuiltAnalysis.analyzerProviderFactories);
        return new AnalysisService(indexSettings, analyzierFactories, tokenizerFactories, charFilterFactories, tokenFilterFactories);
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

    private void registerBuiltInCharFilter(Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> charFilters) {
        charFilters.put("html_strip", HtmlStripCharFilterFactory::new);
        charFilters.put("pattern_replace", requriesAnalysisSettings(PatternReplaceCharFilterFactory::new));
        charFilters.put("mapping", requriesAnalysisSettings(MappingCharFilterFactory::new));
    }

    private void registerBuiltInTokenizer(Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> tokenizers) {
        tokenizers.put("standard", StandardTokenizerFactory::new);
        tokenizers.put("uax_url_email", UAX29URLEmailTokenizerFactory::new);
        tokenizers.put("path_hierarchy", PathHierarchyTokenizerFactory::new);
        tokenizers.put("PathHierarchy", PathHierarchyTokenizerFactory::new);
        tokenizers.put("keyword", KeywordTokenizerFactory::new);
        tokenizers.put("letter", LetterTokenizerFactory::new);
        tokenizers.put("lowercase", LowerCaseTokenizerFactory::new);
        tokenizers.put("whitespace", WhitespaceTokenizerFactory::new);
        tokenizers.put("nGram", NGramTokenizerFactory::new);
        tokenizers.put("ngram", NGramTokenizerFactory::new);
        tokenizers.put("edgeNGram", EdgeNGramTokenizerFactory::new);
        tokenizers.put("edge_ngram", EdgeNGramTokenizerFactory::new);
        tokenizers.put("pattern", PatternTokenizerFactory::new);
        tokenizers.put("classic", ClassicTokenizerFactory::new);
        tokenizers.put("thai", ThaiTokenizerFactory::new);
    }

    private void registerBuiltInTokenFilters(Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> tokenFilters) {
        tokenFilters.put("stop", StopTokenFilterFactory::new);
        tokenFilters.put("reverse", ReverseTokenFilterFactory::new);
        tokenFilters.put("asciifolding", ASCIIFoldingTokenFilterFactory::new);
        tokenFilters.put("length", LengthTokenFilterFactory::new);
        tokenFilters.put("lowercase", LowerCaseTokenFilterFactory::new);
        tokenFilters.put("uppercase", UpperCaseTokenFilterFactory::new);
        tokenFilters.put("porter_stem", PorterStemTokenFilterFactory::new);
        tokenFilters.put("kstem", KStemTokenFilterFactory::new);
        tokenFilters.put("standard", StandardTokenFilterFactory::new);
        tokenFilters.put("nGram", NGramTokenFilterFactory::new);
        tokenFilters.put("ngram", NGramTokenFilterFactory::new);
        tokenFilters.put("edgeNGram", EdgeNGramTokenFilterFactory::new);
        tokenFilters.put("edge_ngram", EdgeNGramTokenFilterFactory::new);
        tokenFilters.put("shingle", ShingleTokenFilterFactory::new);
        tokenFilters.put("unique", UniqueTokenFilterFactory::new);
        tokenFilters.put("truncate", requriesAnalysisSettings(TruncateTokenFilterFactory::new));
        tokenFilters.put("trim", TrimTokenFilterFactory::new);
        tokenFilters.put("limit", LimitTokenCountFilterFactory::new);
        tokenFilters.put("common_grams", requriesAnalysisSettings(CommonGramsTokenFilterFactory::new));
        tokenFilters.put("snowball", SnowballTokenFilterFactory::new);
        tokenFilters.put("stemmer", StemmerTokenFilterFactory::new);
        tokenFilters.put("word_delimiter", WordDelimiterTokenFilterFactory::new);
        tokenFilters.put("delimited_payload_filter", DelimitedPayloadTokenFilterFactory::new);
        tokenFilters.put("elision", ElisionTokenFilterFactory::new);
        tokenFilters.put("keep", requriesAnalysisSettings(KeepWordFilterFactory::new));
        tokenFilters.put("keep_types", requriesAnalysisSettings(KeepTypesFilterFactory::new));
        tokenFilters.put("pattern_capture", requriesAnalysisSettings(PatternCaptureGroupTokenFilterFactory::new));
        tokenFilters.put("pattern_replace", requriesAnalysisSettings(PatternReplaceTokenFilterFactory::new));
        tokenFilters.put("dictionary_decompounder", requriesAnalysisSettings(DictionaryCompoundWordTokenFilterFactory::new));
        tokenFilters.put("hyphenation_decompounder", requriesAnalysisSettings(HyphenationCompoundWordTokenFilterFactory::new));
        tokenFilters.put("arabic_stem", ArabicStemTokenFilterFactory::new);
        tokenFilters.put("brazilian_stem", BrazilianStemTokenFilterFactory::new);
        tokenFilters.put("czech_stem", CzechStemTokenFilterFactory::new);
        tokenFilters.put("dutch_stem", DutchStemTokenFilterFactory::new);
        tokenFilters.put("french_stem", FrenchStemTokenFilterFactory::new);
        tokenFilters.put("german_stem", GermanStemTokenFilterFactory::new);
        tokenFilters.put("russian_stem", RussianStemTokenFilterFactory::new);
        tokenFilters.put("keyword_marker", requriesAnalysisSettings(KeywordMarkerTokenFilterFactory::new));
        tokenFilters.put("stemmer_override", requriesAnalysisSettings(StemmerOverrideTokenFilterFactory::new));
        tokenFilters.put("arabic_normalization", ArabicNormalizationFilterFactory::new);
        tokenFilters.put("german_normalization", GermanNormalizationFilterFactory::new);
        tokenFilters.put("hindi_normalization", HindiNormalizationFilterFactory::new);
        tokenFilters.put("indic_normalization", IndicNormalizationFilterFactory::new);
        tokenFilters.put("sorani_normalization", SoraniNormalizationFilterFactory::new);
        tokenFilters.put("persian_normalization", PersianNormalizationFilterFactory::new);
        tokenFilters.put("scandinavian_normalization", ScandinavianNormalizationFilterFactory::new);
        tokenFilters.put("scandinavian_folding", ScandinavianFoldingFilterFactory::new);
        tokenFilters.put("serbian_normalization", SerbianNormalizationFilterFactory::new);

        if (hunspellService != null) {
            tokenFilters.put("hunspell", requriesAnalysisSettings((indexSettings, env, name, settings) -> new HunspellTokenFilterFactory(indexSettings, name, settings, hunspellService)));
        }
        tokenFilters.put("cjk_bigram", CJKBigramFilterFactory::new);
        tokenFilters.put("cjk_width", CJKWidthFilterFactory::new);

        tokenFilters.put("apostrophe", ApostropheFilterFactory::new);
        tokenFilters.put("classic", ClassicFilterFactory::new);
        tokenFilters.put("decimal_digit", DecimalDigitFilterFactory::new);
        tokenFilters.put("fingerprint", FingerprintTokenFilterFactory::new);
    }

    private void registerBuiltInAnalyzer(Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider>> analyzers) {
        analyzers.put("default", StandardAnalyzerProvider::new);
        analyzers.put("standard", StandardAnalyzerProvider::new);
        analyzers.put("standard_html_strip", StandardHtmlStripAnalyzerProvider::new);
        analyzers.put("simple", SimpleAnalyzerProvider::new);
        analyzers.put("stop", StopAnalyzerProvider::new);
        analyzers.put("whitespace", WhitespaceAnalyzerProvider::new);
        analyzers.put("keyword", KeywordAnalyzerProvider::new);
        analyzers.put("pattern", PatternAnalyzerProvider::new);
        analyzers.put("snowball", SnowballAnalyzerProvider::new);
        analyzers.put("arabic", ArabicAnalyzerProvider::new);
        analyzers.put("armenian", ArmenianAnalyzerProvider::new);
        analyzers.put("basque", BasqueAnalyzerProvider::new);
        analyzers.put("brazilian", BrazilianAnalyzerProvider::new);
        analyzers.put("bulgarian", BulgarianAnalyzerProvider::new);
        analyzers.put("catalan", CatalanAnalyzerProvider::new);
        analyzers.put("chinese", ChineseAnalyzerProvider::new);
        analyzers.put("cjk", CjkAnalyzerProvider::new);
        analyzers.put("czech", CzechAnalyzerProvider::new);
        analyzers.put("danish", DanishAnalyzerProvider::new);
        analyzers.put("dutch", DutchAnalyzerProvider::new);
        analyzers.put("english", EnglishAnalyzerProvider::new);
        analyzers.put("finnish", FinnishAnalyzerProvider::new);
        analyzers.put("french", FrenchAnalyzerProvider::new);
        analyzers.put("galician", GalicianAnalyzerProvider::new);
        analyzers.put("german", GermanAnalyzerProvider::new);
        analyzers.put("greek", GreekAnalyzerProvider::new);
        analyzers.put("hindi", HindiAnalyzerProvider::new);
        analyzers.put("hungarian", HungarianAnalyzerProvider::new);
        analyzers.put("indonesian", IndonesianAnalyzerProvider::new);
        analyzers.put("irish", IrishAnalyzerProvider::new);
        analyzers.put("italian", ItalianAnalyzerProvider::new);
        analyzers.put("latvian", LatvianAnalyzerProvider::new);
        analyzers.put("lithuanian", LithuanianAnalyzerProvider::new);
        analyzers.put("norwegian", NorwegianAnalyzerProvider::new);
        analyzers.put("persian", PersianAnalyzerProvider::new);
        analyzers.put("portuguese", PortugueseAnalyzerProvider::new);
        analyzers.put("romanian", RomanianAnalyzerProvider::new);
        analyzers.put("russian", RussianAnalyzerProvider::new);
        analyzers.put("sorani", SoraniAnalyzerProvider::new);
        analyzers.put("spanish", SpanishAnalyzerProvider::new);
        analyzers.put("swedish", SwedishAnalyzerProvider::new);
        analyzers.put("turkish", TurkishAnalyzerProvider::new);
        analyzers.put("thai", ThaiAnalyzerProvider::new);
        analyzers.put("fingerprint", FingerprintAnalyzerProvider::new);
    }

    private <T> Map<String, T> buildMapping(boolean analyzer, String toBuild, IndexSettings settings, Map<String, Settings> settingsMap, Map<String, AnalysisModule.AnalysisProvider<T>> providerMap, Map<String, AnalysisModule.AnalysisProvider<T>> defaultInstance) throws IOException {
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
                if (typeName == null) {
                    throw new IllegalArgumentException(toBuild + " [" + name + "] must specify either an analyzer type, or a tokenizer");
                }
                AnalysisModule.AnalysisProvider<T> type = providerMap.get(typeName);
                if (type == null) {
                    throw new IllegalArgumentException("Unknown " + toBuild + " type [" + typeName + "] for [" + name + "]");
                }
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

    private static class PrebuiltAnalysis implements Closeable {

        final Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider>> analyzerProviderFactories;
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

        public AnalysisModule.AnalysisProvider<AnalyzerProvider> getAnalyzerProvider(String name) {
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
