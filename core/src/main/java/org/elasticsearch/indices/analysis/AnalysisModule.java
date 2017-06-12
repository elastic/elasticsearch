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

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.ApostropheFilterFactory;
import org.elasticsearch.index.analysis.ArabicAnalyzerProvider;
import org.elasticsearch.index.analysis.ArabicNormalizationFilterFactory;
import org.elasticsearch.index.analysis.ArabicStemTokenFilterFactory;
import org.elasticsearch.index.analysis.ArmenianAnalyzerProvider;
import org.elasticsearch.index.analysis.BasqueAnalyzerProvider;
import org.elasticsearch.index.analysis.BrazilianAnalyzerProvider;
import org.elasticsearch.index.analysis.BrazilianStemTokenFilterFactory;
import org.elasticsearch.index.analysis.BulgarianAnalyzerProvider;
import org.elasticsearch.index.analysis.CJKBigramFilterFactory;
import org.elasticsearch.index.analysis.CJKWidthFilterFactory;
import org.elasticsearch.index.analysis.CatalanAnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.ChineseAnalyzerProvider;
import org.elasticsearch.index.analysis.CjkAnalyzerProvider;
import org.elasticsearch.index.analysis.ClassicFilterFactory;
import org.elasticsearch.index.analysis.ClassicTokenizerFactory;
import org.elasticsearch.index.analysis.CommonGramsTokenFilterFactory;
import org.elasticsearch.index.analysis.CzechAnalyzerProvider;
import org.elasticsearch.index.analysis.CzechStemTokenFilterFactory;
import org.elasticsearch.index.analysis.DanishAnalyzerProvider;
import org.elasticsearch.index.analysis.DecimalDigitFilterFactory;
import org.elasticsearch.index.analysis.DelimitedPayloadTokenFilterFactory;
import org.elasticsearch.index.analysis.DutchAnalyzerProvider;
import org.elasticsearch.index.analysis.DutchStemTokenFilterFactory;
import org.elasticsearch.index.analysis.EdgeNGramTokenFilterFactory;
import org.elasticsearch.index.analysis.EdgeNGramTokenizerFactory;
import org.elasticsearch.index.analysis.ElisionTokenFilterFactory;
import org.elasticsearch.index.analysis.EnglishAnalyzerProvider;
import org.elasticsearch.index.analysis.FingerprintAnalyzerProvider;
import org.elasticsearch.index.analysis.FingerprintTokenFilterFactory;
import org.elasticsearch.index.analysis.FinnishAnalyzerProvider;
import org.elasticsearch.index.analysis.FlattenGraphTokenFilterFactory;
import org.elasticsearch.index.analysis.FrenchAnalyzerProvider;
import org.elasticsearch.index.analysis.FrenchStemTokenFilterFactory;
import org.elasticsearch.index.analysis.GalicianAnalyzerProvider;
import org.elasticsearch.index.analysis.GermanAnalyzerProvider;
import org.elasticsearch.index.analysis.GermanNormalizationFilterFactory;
import org.elasticsearch.index.analysis.GermanStemTokenFilterFactory;
import org.elasticsearch.index.analysis.GreekAnalyzerProvider;
import org.elasticsearch.index.analysis.HindiAnalyzerProvider;
import org.elasticsearch.index.analysis.HindiNormalizationFilterFactory;
import org.elasticsearch.index.analysis.HungarianAnalyzerProvider;
import org.elasticsearch.index.analysis.HunspellTokenFilterFactory;
import org.elasticsearch.index.analysis.IndicNormalizationFilterFactory;
import org.elasticsearch.index.analysis.IndonesianAnalyzerProvider;
import org.elasticsearch.index.analysis.IrishAnalyzerProvider;
import org.elasticsearch.index.analysis.ItalianAnalyzerProvider;
import org.elasticsearch.index.analysis.KStemTokenFilterFactory;
import org.elasticsearch.index.analysis.KeepTypesFilterFactory;
import org.elasticsearch.index.analysis.KeepWordFilterFactory;
import org.elasticsearch.index.analysis.KeywordAnalyzerProvider;
import org.elasticsearch.index.analysis.KeywordTokenizerFactory;
import org.elasticsearch.index.analysis.LatvianAnalyzerProvider;
import org.elasticsearch.index.analysis.LengthTokenFilterFactory;
import org.elasticsearch.index.analysis.LetterTokenizerFactory;
import org.elasticsearch.index.analysis.LimitTokenCountFilterFactory;
import org.elasticsearch.index.analysis.LithuanianAnalyzerProvider;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.LowerCaseTokenizerFactory;
import org.elasticsearch.index.analysis.MinHashTokenFilterFactory;
import org.elasticsearch.index.analysis.NGramTokenFilterFactory;
import org.elasticsearch.index.analysis.NGramTokenizerFactory;
import org.elasticsearch.index.analysis.NorwegianAnalyzerProvider;
import org.elasticsearch.index.analysis.PathHierarchyTokenizerFactory;
import org.elasticsearch.index.analysis.PatternAnalyzerProvider;
import org.elasticsearch.index.analysis.PatternCaptureGroupTokenFilterFactory;
import org.elasticsearch.index.analysis.PatternReplaceTokenFilterFactory;
import org.elasticsearch.index.analysis.PatternTokenizerFactory;
import org.elasticsearch.index.analysis.PersianAnalyzerProvider;
import org.elasticsearch.index.analysis.PersianNormalizationFilterFactory;
import org.elasticsearch.index.analysis.PortugueseAnalyzerProvider;
import org.elasticsearch.index.analysis.PreConfiguredCharFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.ReverseTokenFilterFactory;
import org.elasticsearch.index.analysis.RomanianAnalyzerProvider;
import org.elasticsearch.index.analysis.RussianAnalyzerProvider;
import org.elasticsearch.index.analysis.RussianStemTokenFilterFactory;
import org.elasticsearch.index.analysis.ScandinavianFoldingFilterFactory;
import org.elasticsearch.index.analysis.ScandinavianNormalizationFilterFactory;
import org.elasticsearch.index.analysis.SerbianNormalizationFilterFactory;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.SimpleAnalyzerProvider;
import org.elasticsearch.index.analysis.SnowballAnalyzerProvider;
import org.elasticsearch.index.analysis.SoraniAnalyzerProvider;
import org.elasticsearch.index.analysis.SoraniNormalizationFilterFactory;
import org.elasticsearch.index.analysis.SpanishAnalyzerProvider;
import org.elasticsearch.index.analysis.StandardAnalyzerProvider;
import org.elasticsearch.index.analysis.StandardHtmlStripAnalyzerProvider;
import org.elasticsearch.index.analysis.StandardTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StemmerOverrideTokenFilterFactory;
import org.elasticsearch.index.analysis.StemmerTokenFilterFactory;
import org.elasticsearch.index.analysis.StopAnalyzerProvider;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.index.analysis.SwedishAnalyzerProvider;
import org.elasticsearch.index.analysis.ThaiAnalyzerProvider;
import org.elasticsearch.index.analysis.ThaiTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.analysis.TruncateTokenFilterFactory;
import org.elasticsearch.index.analysis.TurkishAnalyzerProvider;
import org.elasticsearch.index.analysis.UAX29URLEmailTokenizerFactory;
import org.elasticsearch.index.analysis.UniqueTokenFilterFactory;
import org.elasticsearch.index.analysis.UpperCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.WhitespaceAnalyzerProvider;
import org.elasticsearch.index.analysis.WhitespaceTokenizerFactory;
import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.analysis.compound.HyphenationCompoundWordTokenFilterFactory;
import org.elasticsearch.plugins.AnalysisPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.plugins.AnalysisPlugin.requriesAnalysisSettings;

/**
 * Sets up {@link AnalysisRegistry}.
 */
public final class AnalysisModule {
    static {
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData
            .SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).build();
        IndexMetaData metaData = IndexMetaData.builder("_na_").settings(build).build();
        NA_INDEX_SETTINGS = new IndexSettings(metaData, Settings.EMPTY);
    }

    private static final IndexSettings NA_INDEX_SETTINGS;

    private final HunspellService hunspellService;
    private final AnalysisRegistry analysisRegistry;

    public AnalysisModule(Environment environment, List<AnalysisPlugin> plugins) throws IOException {
        NamedRegistry<AnalysisProvider<CharFilterFactory>> charFilters = setupCharFilters(plugins);
        NamedRegistry<org.apache.lucene.analysis.hunspell.Dictionary> hunspellDictionaries = setupHunspellDictionaries(plugins);
        hunspellService = new HunspellService(environment.settings(), environment, hunspellDictionaries.getRegistry());
        NamedRegistry<AnalysisProvider<TokenFilterFactory>> tokenFilters = setupTokenFilters(plugins, hunspellService);
        NamedRegistry<AnalysisProvider<TokenizerFactory>> tokenizers = setupTokenizers(plugins);
        NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> analyzers = setupAnalyzers(plugins);
        NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> normalizers = setupNormalizers(plugins);

        Map<String, PreConfiguredCharFilter> preConfiguredCharFilters = setupPreConfiguredCharFilters(plugins);
        Map<String, PreConfiguredTokenFilter> preConfiguredTokenFilters = setupPreConfiguredTokenFilters(plugins);
        Map<String, PreConfiguredTokenizer> preConfiguredTokenizers = setupPreConfiguredTokenizers(plugins);

        analysisRegistry = new AnalysisRegistry(environment,
                charFilters.getRegistry(), tokenFilters.getRegistry(), tokenizers.getRegistry(),
                analyzers.getRegistry(), normalizers.getRegistry(),
                preConfiguredCharFilters, preConfiguredTokenFilters, preConfiguredTokenizers);
    }

    HunspellService getHunspellService() {
        return hunspellService;
    }

    public AnalysisRegistry getAnalysisRegistry() {
        return analysisRegistry;
    }

    private NamedRegistry<AnalysisProvider<CharFilterFactory>> setupCharFilters(List<AnalysisPlugin> plugins) {
        NamedRegistry<AnalysisProvider<CharFilterFactory>> charFilters = new NamedRegistry<>("char_filter");
        charFilters.extractAndRegister(plugins, AnalysisPlugin::getCharFilters);
        return charFilters;
    }

    public NamedRegistry<org.apache.lucene.analysis.hunspell.Dictionary> setupHunspellDictionaries(List<AnalysisPlugin> plugins) {
        NamedRegistry<org.apache.lucene.analysis.hunspell.Dictionary> hunspellDictionaries = new NamedRegistry<>("dictionary");
        hunspellDictionaries.extractAndRegister(plugins, AnalysisPlugin::getHunspellDictionaries);
        return hunspellDictionaries;
    }

    private NamedRegistry<AnalysisProvider<TokenFilterFactory>> setupTokenFilters(List<AnalysisPlugin> plugins, HunspellService
        hunspellService) {
        NamedRegistry<AnalysisProvider<TokenFilterFactory>> tokenFilters = new NamedRegistry<>("token_filter");
        tokenFilters.register("stop", StopTokenFilterFactory::new);
        tokenFilters.register("reverse", ReverseTokenFilterFactory::new);
        tokenFilters.register("length", LengthTokenFilterFactory::new);
        tokenFilters.register("lowercase", LowerCaseTokenFilterFactory::new);
        tokenFilters.register("uppercase", UpperCaseTokenFilterFactory::new);
        tokenFilters.register("kstem", KStemTokenFilterFactory::new);
        tokenFilters.register("standard", StandardTokenFilterFactory::new);
        tokenFilters.register("nGram", NGramTokenFilterFactory::new);
        tokenFilters.register("ngram", NGramTokenFilterFactory::new);
        tokenFilters.register("edgeNGram", EdgeNGramTokenFilterFactory::new);
        tokenFilters.register("edge_ngram", EdgeNGramTokenFilterFactory::new);
        tokenFilters.register("shingle", ShingleTokenFilterFactory::new);
        tokenFilters.register("min_hash", MinHashTokenFilterFactory::new);
        tokenFilters.register("unique", UniqueTokenFilterFactory::new);
        tokenFilters.register("truncate", requriesAnalysisSettings(TruncateTokenFilterFactory::new));
        tokenFilters.register("limit", LimitTokenCountFilterFactory::new);
        tokenFilters.register("common_grams", requriesAnalysisSettings(CommonGramsTokenFilterFactory::new));
        tokenFilters.register("stemmer", StemmerTokenFilterFactory::new);
        tokenFilters.register("delimited_payload_filter", DelimitedPayloadTokenFilterFactory::new);
        tokenFilters.register("elision", ElisionTokenFilterFactory::new);
        tokenFilters.register("flatten_graph", FlattenGraphTokenFilterFactory::new);
        tokenFilters.register("keep", requriesAnalysisSettings(KeepWordFilterFactory::new));
        tokenFilters.register("keep_types", requriesAnalysisSettings(KeepTypesFilterFactory::new));
        tokenFilters.register("pattern_capture", requriesAnalysisSettings(PatternCaptureGroupTokenFilterFactory::new));
        tokenFilters.register("pattern_replace", requriesAnalysisSettings(PatternReplaceTokenFilterFactory::new));
        tokenFilters.register("dictionary_decompounder", requriesAnalysisSettings(DictionaryCompoundWordTokenFilterFactory::new));
        tokenFilters.register("hyphenation_decompounder", requriesAnalysisSettings(HyphenationCompoundWordTokenFilterFactory::new));
        tokenFilters.register("arabic_stem", ArabicStemTokenFilterFactory::new);
        tokenFilters.register("brazilian_stem", BrazilianStemTokenFilterFactory::new);
        tokenFilters.register("czech_stem", CzechStemTokenFilterFactory::new);
        tokenFilters.register("dutch_stem", DutchStemTokenFilterFactory::new);
        tokenFilters.register("french_stem", FrenchStemTokenFilterFactory::new);
        tokenFilters.register("german_stem", GermanStemTokenFilterFactory::new);
        tokenFilters.register("russian_stem", RussianStemTokenFilterFactory::new);
        tokenFilters.register("stemmer_override", requriesAnalysisSettings(StemmerOverrideTokenFilterFactory::new));
        tokenFilters.register("arabic_normalization", ArabicNormalizationFilterFactory::new);
        tokenFilters.register("german_normalization", GermanNormalizationFilterFactory::new);
        tokenFilters.register("hindi_normalization", HindiNormalizationFilterFactory::new);
        tokenFilters.register("indic_normalization", IndicNormalizationFilterFactory::new);
        tokenFilters.register("sorani_normalization", SoraniNormalizationFilterFactory::new);
        tokenFilters.register("persian_normalization", PersianNormalizationFilterFactory::new);
        tokenFilters.register("scandinavian_normalization", ScandinavianNormalizationFilterFactory::new);
        tokenFilters.register("scandinavian_folding", ScandinavianFoldingFilterFactory::new);
        tokenFilters.register("serbian_normalization", SerbianNormalizationFilterFactory::new);

        tokenFilters.register("hunspell", requriesAnalysisSettings((indexSettings, env, name, settings) -> new HunspellTokenFilterFactory
            (indexSettings, name, settings, hunspellService)));
        tokenFilters.register("cjk_bigram", CJKBigramFilterFactory::new);
        tokenFilters.register("cjk_width", CJKWidthFilterFactory::new);

        tokenFilters.register("apostrophe", ApostropheFilterFactory::new);
        tokenFilters.register("classic", ClassicFilterFactory::new);
        tokenFilters.register("decimal_digit", DecimalDigitFilterFactory::new);
        tokenFilters.register("fingerprint", FingerprintTokenFilterFactory::new);
        tokenFilters.extractAndRegister(plugins, AnalysisPlugin::getTokenFilters);
        return tokenFilters;
    }

    static Map<String, PreConfiguredCharFilter> setupPreConfiguredCharFilters(List<AnalysisPlugin> plugins) {
        NamedRegistry<PreConfiguredCharFilter> preConfiguredCharFilters = new NamedRegistry<>("pre-configured char_filter");

        // No char filter are available in lucene-core so none are built in to Elasticsearch core

        for (AnalysisPlugin plugin: plugins) {
            for (PreConfiguredCharFilter filter : plugin.getPreConfiguredCharFilters()) {
                preConfiguredCharFilters.register(filter.getName(), filter);
            }
        }
        return unmodifiableMap(preConfiguredCharFilters.getRegistry());
    }

    static Map<String, PreConfiguredTokenFilter> setupPreConfiguredTokenFilters(List<AnalysisPlugin> plugins) {
        NamedRegistry<PreConfiguredTokenFilter> preConfiguredTokenFilters = new NamedRegistry<>("pre-configured token_filter");

        // Add filters available in lucene-core
        preConfiguredTokenFilters.register("lowercase", PreConfiguredTokenFilter.singleton("lowercase", true, LowerCaseFilter::new));
        preConfiguredTokenFilters.register("standard", PreConfiguredTokenFilter.singleton("standard", false, StandardFilter::new));
        /* Note that "stop" is available in lucene-core but it's pre-built
         * version uses a set of English stop words that are in
         * lucene-analyzers-common so "stop" is defined in the analysis-common
         * module. */

        for (AnalysisPlugin plugin: plugins) {
            for (PreConfiguredTokenFilter filter : plugin.getPreConfiguredTokenFilters()) {
                preConfiguredTokenFilters.register(filter.getName(), filter);
            }
        }
        return unmodifiableMap(preConfiguredTokenFilters.getRegistry());
    }

    static Map<String, PreConfiguredTokenizer> setupPreConfiguredTokenizers(List<AnalysisPlugin> plugins) {
        NamedRegistry<PreConfiguredTokenizer> preConfiguredTokenizers = new NamedRegistry<>("pre-configured tokenizer");

        // Temporary shim to register old style pre-configured tokenizers
        for (PreBuiltTokenizers tokenizer : PreBuiltTokenizers.values()) {
            String name = tokenizer.name().toLowerCase(Locale.ROOT);
            PreConfiguredTokenizer preConfigured;
            switch (tokenizer.getCachingStrategy()) {
            case ONE:
                preConfigured = PreConfiguredTokenizer.singleton(name,
                        () -> tokenizer.create(Version.CURRENT), null);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Caching strategy unsupported by temporary shim [" + tokenizer + "]");
            }
            preConfiguredTokenizers.register(name, preConfigured);
        }
        // Temporary shim for aliases. TODO deprecate after they are moved
        preConfiguredTokenizers.register("nGram", preConfiguredTokenizers.getRegistry().get("ngram"));
        preConfiguredTokenizers.register("edgeNGram", preConfiguredTokenizers.getRegistry().get("edge_ngram"));
        preConfiguredTokenizers.register("PathHierarchy", preConfiguredTokenizers.getRegistry().get("path_hierarchy"));

        for (AnalysisPlugin plugin: plugins) {
            for (PreConfiguredTokenizer tokenizer : plugin.getPreConfiguredTokenizers()) {
                preConfiguredTokenizers.register(tokenizer.getName(), tokenizer);
            }
        }
        return unmodifiableMap(preConfiguredTokenizers.getRegistry());
    }

    private NamedRegistry<AnalysisProvider<TokenizerFactory>> setupTokenizers(List<AnalysisPlugin> plugins) {
        NamedRegistry<AnalysisProvider<TokenizerFactory>> tokenizers = new NamedRegistry<>("tokenizer");
        tokenizers.register("standard", StandardTokenizerFactory::new);
        tokenizers.register("uax_url_email", UAX29URLEmailTokenizerFactory::new);
        tokenizers.register("path_hierarchy", PathHierarchyTokenizerFactory::new);
        tokenizers.register("PathHierarchy", PathHierarchyTokenizerFactory::new);
        tokenizers.register("keyword", KeywordTokenizerFactory::new);
        tokenizers.register("letter", LetterTokenizerFactory::new);
        tokenizers.register("lowercase", LowerCaseTokenizerFactory::new);
        tokenizers.register("whitespace", WhitespaceTokenizerFactory::new);
        tokenizers.register("nGram", NGramTokenizerFactory::new);
        tokenizers.register("ngram", NGramTokenizerFactory::new);
        tokenizers.register("edgeNGram", EdgeNGramTokenizerFactory::new);
        tokenizers.register("edge_ngram", EdgeNGramTokenizerFactory::new);
        tokenizers.register("pattern", PatternTokenizerFactory::new);
        tokenizers.register("classic", ClassicTokenizerFactory::new);
        tokenizers.register("thai", ThaiTokenizerFactory::new);
        tokenizers.extractAndRegister(plugins, AnalysisPlugin::getTokenizers);
        return tokenizers;
    }

    private NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> setupAnalyzers(List<AnalysisPlugin> plugins) {
        NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> analyzers = new NamedRegistry<>("analyzer");
        analyzers.register("default", StandardAnalyzerProvider::new);
        analyzers.register("standard", StandardAnalyzerProvider::new);
        analyzers.register("standard_html_strip", StandardHtmlStripAnalyzerProvider::new);
        analyzers.register("simple", SimpleAnalyzerProvider::new);
        analyzers.register("stop", StopAnalyzerProvider::new);
        analyzers.register("whitespace", WhitespaceAnalyzerProvider::new);
        analyzers.register("keyword", KeywordAnalyzerProvider::new);
        analyzers.register("pattern", PatternAnalyzerProvider::new);
        analyzers.register("snowball", SnowballAnalyzerProvider::new);
        analyzers.register("arabic", ArabicAnalyzerProvider::new);
        analyzers.register("armenian", ArmenianAnalyzerProvider::new);
        analyzers.register("basque", BasqueAnalyzerProvider::new);
        analyzers.register("brazilian", BrazilianAnalyzerProvider::new);
        analyzers.register("bulgarian", BulgarianAnalyzerProvider::new);
        analyzers.register("catalan", CatalanAnalyzerProvider::new);
        analyzers.register("chinese", ChineseAnalyzerProvider::new);
        analyzers.register("cjk", CjkAnalyzerProvider::new);
        analyzers.register("czech", CzechAnalyzerProvider::new);
        analyzers.register("danish", DanishAnalyzerProvider::new);
        analyzers.register("dutch", DutchAnalyzerProvider::new);
        analyzers.register("english", EnglishAnalyzerProvider::new);
        analyzers.register("finnish", FinnishAnalyzerProvider::new);
        analyzers.register("french", FrenchAnalyzerProvider::new);
        analyzers.register("galician", GalicianAnalyzerProvider::new);
        analyzers.register("german", GermanAnalyzerProvider::new);
        analyzers.register("greek", GreekAnalyzerProvider::new);
        analyzers.register("hindi", HindiAnalyzerProvider::new);
        analyzers.register("hungarian", HungarianAnalyzerProvider::new);
        analyzers.register("indonesian", IndonesianAnalyzerProvider::new);
        analyzers.register("irish", IrishAnalyzerProvider::new);
        analyzers.register("italian", ItalianAnalyzerProvider::new);
        analyzers.register("latvian", LatvianAnalyzerProvider::new);
        analyzers.register("lithuanian", LithuanianAnalyzerProvider::new);
        analyzers.register("norwegian", NorwegianAnalyzerProvider::new);
        analyzers.register("persian", PersianAnalyzerProvider::new);
        analyzers.register("portuguese", PortugueseAnalyzerProvider::new);
        analyzers.register("romanian", RomanianAnalyzerProvider::new);
        analyzers.register("russian", RussianAnalyzerProvider::new);
        analyzers.register("sorani", SoraniAnalyzerProvider::new);
        analyzers.register("spanish", SpanishAnalyzerProvider::new);
        analyzers.register("swedish", SwedishAnalyzerProvider::new);
        analyzers.register("turkish", TurkishAnalyzerProvider::new);
        analyzers.register("thai", ThaiAnalyzerProvider::new);
        analyzers.register("fingerprint", FingerprintAnalyzerProvider::new);
        analyzers.extractAndRegister(plugins, AnalysisPlugin::getAnalyzers);
        return analyzers;
    }

    private NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> setupNormalizers(List<AnalysisPlugin> plugins) {
        NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> normalizers = new NamedRegistry<>("normalizer");
        // TODO: provide built-in normalizer providers?
        // TODO: pluggability?
        return normalizers;
    }


    /**
     * The basic factory interface for analysis components.
     */
    public interface AnalysisProvider<T> {

        /**
         * Creates a new analysis provider.
         *
         * @param indexSettings the index settings for the index this provider is created for
         * @param environment   the nodes environment to load resources from persistent storage
         * @param name          the name of the analysis component
         * @param settings      the component specific settings without context prefixes
         * @return a new provider instance
         * @throws IOException if an {@link IOException} occurs
         */
        T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException;

        /**
         * Creates a new global scope analysis provider without index specific settings not settings for the provider itself.
         * This can be used to get a default instance of an analysis factory without binding to an index.
         *
         * @param environment the nodes environment to load resources from persistent storage
         * @param name        the name of the analysis component
         * @return a new provider instance
         * @throws IOException              if an {@link IOException} occurs
         * @throws IllegalArgumentException if the provider requires analysis settings ie. if {@link #requiresAnalysisSettings()} returns
         *                                  <code>true</code>
         */
        default T get(Environment environment, String name) throws IOException {
            if (requiresAnalysisSettings()) {
                throw new IllegalArgumentException("Analysis settings required - can't instantiate analysis factory");
            }
            return get(NA_INDEX_SETTINGS, environment, name, NA_INDEX_SETTINGS.getSettings());
        }

        /**
         * If <code>true</code> the analysis component created by this provider requires certain settings to be instantiated.
         * it can't be created with defaults. The default is <code>false</code>.
         */
        default boolean requiresAnalysisSettings() {
            return false;
        }
    }
}
