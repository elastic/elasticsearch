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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.analysis.compound.HyphenationCompoundWordTokenFilterFactory;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.util.LinkedList;
import java.util.Map;

/**
 *
 */
public class AnalysisModule extends AbstractModule {

    public static class AnalysisBinderProcessor {

        public void processCharFilters(CharFiltersBindings charFiltersBindings) {

        }

        public static class CharFiltersBindings {
            private final Map<String, Class<? extends CharFilterFactory>> charFilters = Maps.newHashMap();

            public CharFiltersBindings() {
            }

            public void processCharFilter(String name, Class<? extends CharFilterFactory> charFilterFactory) {
                charFilters.put(name, charFilterFactory);
            }
        }

        public void processTokenFilters(TokenFiltersBindings tokenFiltersBindings) {

        }

        public static class TokenFiltersBindings {
            private final Map<String, Class<? extends TokenFilterFactory>> tokenFilters = Maps.newHashMap();

            public TokenFiltersBindings() {
            }

            public void processTokenFilter(String name, Class<? extends TokenFilterFactory> tokenFilterFactory) {
                tokenFilters.put(name, tokenFilterFactory);
            }
        }

        public void processTokenizers(TokenizersBindings tokenizersBindings) {

        }

        public static class TokenizersBindings {
            private final Map<String, Class<? extends TokenizerFactory>> tokenizers = Maps.newHashMap();

            public TokenizersBindings() {
            }

            public void processTokenizer(String name, Class<? extends TokenizerFactory> tokenizerFactory) {
                tokenizers.put(name, tokenizerFactory);
            }
        }

        public void processAnalyzers(AnalyzersBindings analyzersBindings) {

        }

        public static class AnalyzersBindings {
            private final Map<String, Class<? extends AnalyzerProvider>> analyzers = Maps.newHashMap();

            public AnalyzersBindings() {
            }

            public void processAnalyzer(String name, Class<? extends AnalyzerProvider> analyzerProvider) {
                analyzers.put(name, analyzerProvider);
            }
        }
    }

    private final Settings settings;

    private final IndicesAnalysisService indicesAnalysisService;

    private final LinkedList<AnalysisBinderProcessor> processors = Lists.newLinkedList();

    private final Map<String, Class<? extends CharFilterFactory>> charFilters = Maps.newHashMap();
    private final Map<String, Class<? extends TokenFilterFactory>> tokenFilters = Maps.newHashMap();
    private final Map<String, Class<? extends TokenizerFactory>> tokenizers = Maps.newHashMap();
    private final Map<String, Class<? extends AnalyzerProvider>> analyzers = Maps.newHashMap();


    public AnalysisModule(Settings settings) {
        this(settings, null);
    }

    public AnalysisModule(Settings settings, IndicesAnalysisService indicesAnalysisService) {
        this.settings = settings;
        this.indicesAnalysisService = indicesAnalysisService;
        processors.add(new DefaultProcessor());
        try {
            processors.add(new ExtendedProcessor());
        } catch (Throwable t) {
            // ignore. no extended ones
        }
    }

    public AnalysisModule addProcessor(AnalysisBinderProcessor processor) {
        processors.addFirst(processor);
        return this;
    }

    public AnalysisModule addCharFilter(String name, Class<? extends CharFilterFactory> charFilter) {
        charFilters.put(name, charFilter);
        return this;
    }

    public AnalysisModule addTokenFilter(String name, Class<? extends TokenFilterFactory> tokenFilter) {
        tokenFilters.put(name, tokenFilter);
        return this;
    }

    public AnalysisModule addTokenizer(String name, Class<? extends TokenizerFactory> tokenizer) {
        tokenizers.put(name, tokenizer);
        return this;
    }

    public AnalysisModule addAnalyzer(String name, Class<? extends AnalyzerProvider> analyzer) {
        analyzers.put(name, analyzer);
        return this;
    }

    @Override
    protected void configure() {
        MapBinder<String, CharFilterFactoryFactory> charFilterBinder
                = MapBinder.newMapBinder(binder(), String.class, CharFilterFactoryFactory.class);

        // CHAR FILTERS

        AnalysisBinderProcessor.CharFiltersBindings charFiltersBindings = new AnalysisBinderProcessor.CharFiltersBindings();
        for (AnalysisBinderProcessor processor : processors) {
            processor.processCharFilters(charFiltersBindings);
        }
        charFiltersBindings.charFilters.putAll(charFilters);

        Map<String, Settings> charFiltersSettings = settings.getGroups("index.analysis.char_filter");
        for (Map.Entry<String, Settings> entry : charFiltersSettings.entrySet()) {
            String charFilterName = entry.getKey();
            Settings charFilterSettings = entry.getValue();

            Class<? extends CharFilterFactory> type = null;
            try {
                type = charFilterSettings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "CharFilterFactory");
            } catch (NoClassSettingsException e) {
                // nothing found, see if its in bindings as a binding name
                if (charFilterSettings.get("type") != null) {
                    type = charFiltersBindings.charFilters.get(Strings.toUnderscoreCase(charFilterSettings.get("type")));
                    if (type == null) {
                        type = charFiltersBindings.charFilters.get(Strings.toCamelCase(charFilterSettings.get("type")));
                    }
                }
                if (type == null) {
                    throw new ElasticsearchIllegalArgumentException("failed to find char filter type [" + charFilterSettings.get("type") + "] for [" + charFilterName + "]", e);
                }
            }
            if (type == null) {
                // nothing found, see if its in bindings as a binding name
                throw new ElasticsearchIllegalArgumentException("Char Filter [" + charFilterName + "] must have a type associated with it");
            }
            charFilterBinder.addBinding(charFilterName).toProvider(FactoryProvider.newFactory(CharFilterFactoryFactory.class, type)).in(Scopes.SINGLETON);
        }
        // go over the char filters in the bindings and register the ones that are not configured
        for (Map.Entry<String, Class<? extends CharFilterFactory>> entry : charFiltersBindings.charFilters.entrySet()) {
            String charFilterName = entry.getKey();
            Class<? extends CharFilterFactory> clazz = entry.getValue();
            // we don't want to re-register one that already exists
            if (charFiltersSettings.containsKey(charFilterName)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (clazz.getAnnotation(AnalysisSettingsRequired.class) != null) {
                continue;
            }
            // register it as default under the name
            if (indicesAnalysisService != null && indicesAnalysisService.hasCharFilter(charFilterName)) {
                // don't register it here, we will use explicitly register it in the AnalysisService
                //charFilterBinder.addBinding(charFilterName).toInstance(indicesAnalysisService.charFilterFactoryFactory(charFilterName));
            } else {
                charFilterBinder.addBinding(charFilterName).toProvider(FactoryProvider.newFactory(CharFilterFactoryFactory.class, clazz)).in(Scopes.SINGLETON);
            }
        }


        // TOKEN FILTERS

        MapBinder<String, TokenFilterFactoryFactory> tokenFilterBinder
                = MapBinder.newMapBinder(binder(), String.class, TokenFilterFactoryFactory.class);

        // initial default bindings
        AnalysisBinderProcessor.TokenFiltersBindings tokenFiltersBindings = new AnalysisBinderProcessor.TokenFiltersBindings();
        for (AnalysisBinderProcessor processor : processors) {
            processor.processTokenFilters(tokenFiltersBindings);
        }
        tokenFiltersBindings.tokenFilters.putAll(tokenFilters);

        Map<String, Settings> tokenFiltersSettings = settings.getGroups("index.analysis.filter");
        for (Map.Entry<String, Settings> entry : tokenFiltersSettings.entrySet()) {
            String tokenFilterName = entry.getKey();
            Settings tokenFilterSettings = entry.getValue();

            Class<? extends TokenFilterFactory> type = null;
            try {
                type = tokenFilterSettings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "TokenFilterFactory");
            } catch (NoClassSettingsException e) {
                // nothing found, see if its in bindings as a binding name
                if (tokenFilterSettings.get("type") != null) {
                    type = tokenFiltersBindings.tokenFilters.get(Strings.toUnderscoreCase(tokenFilterSettings.get("type")));
                    if (type == null) {
                        type = tokenFiltersBindings.tokenFilters.get(Strings.toCamelCase(tokenFilterSettings.get("type")));
                    }
                }
                if (type == null) {
                    throw new ElasticsearchIllegalArgumentException("failed to find token filter type [" + tokenFilterSettings.get("type") + "] for [" + tokenFilterName + "]", e);
                }
            }
            if (type == null) {
                throw new ElasticsearchIllegalArgumentException("token filter [" + tokenFilterName + "] must have a type associated with it");
            }
            tokenFilterBinder.addBinding(tokenFilterName).toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, type)).in(Scopes.SINGLETON);
        }
        // go over the filters in the bindings and register the ones that are not configured
        for (Map.Entry<String, Class<? extends TokenFilterFactory>> entry : tokenFiltersBindings.tokenFilters.entrySet()) {
            String tokenFilterName = entry.getKey();
            Class<? extends TokenFilterFactory> clazz = entry.getValue();
            // we don't want to re-register one that already exists
            if (tokenFiltersSettings.containsKey(tokenFilterName)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (clazz.getAnnotation(AnalysisSettingsRequired.class) != null) {
                continue;
            }
            // register it as default under the name
            if (indicesAnalysisService != null && indicesAnalysisService.hasTokenFilter(tokenFilterName)) {
                // don't register it here, we will use explicitly register it in the AnalysisService
                // tokenFilterBinder.addBinding(tokenFilterName).toInstance(indicesAnalysisService.tokenFilterFactoryFactory(tokenFilterName));
            } else {
                tokenFilterBinder.addBinding(tokenFilterName).toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, clazz)).in(Scopes.SINGLETON);
            }
        }

        // TOKENIZER

        MapBinder<String, TokenizerFactoryFactory> tokenizerBinder
                = MapBinder.newMapBinder(binder(), String.class, TokenizerFactoryFactory.class);

        // initial default bindings
        AnalysisBinderProcessor.TokenizersBindings tokenizersBindings = new AnalysisBinderProcessor.TokenizersBindings();
        for (AnalysisBinderProcessor processor : processors) {
            processor.processTokenizers(tokenizersBindings);
        }
        tokenizersBindings.tokenizers.putAll(tokenizers);

        Map<String, Settings> tokenizersSettings = settings.getGroups("index.analysis.tokenizer");
        for (Map.Entry<String, Settings> entry : tokenizersSettings.entrySet()) {
            String tokenizerName = entry.getKey();
            Settings tokenizerSettings = entry.getValue();


            Class<? extends TokenizerFactory> type = null;
            try {
                type = tokenizerSettings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "TokenizerFactory");
            } catch (NoClassSettingsException e) {
                // nothing found, see if its in bindings as a binding name
                if (tokenizerSettings.get("type") != null) {
                    type = tokenizersBindings.tokenizers.get(Strings.toUnderscoreCase(tokenizerSettings.get("type")));
                    if (type == null) {
                        type = tokenizersBindings.tokenizers.get(Strings.toCamelCase(tokenizerSettings.get("type")));
                    }
                }
                if (type == null) {
                    throw new ElasticsearchIllegalArgumentException("failed to find tokenizer type [" + tokenizerSettings.get("type") + "] for [" + tokenizerName + "]", e);
                }
            }
            if (type == null) {
                throw new ElasticsearchIllegalArgumentException("token filter [" + tokenizerName + "] must have a type associated with it");
            }
            tokenizerBinder.addBinding(tokenizerName).toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, type)).in(Scopes.SINGLETON);
        }
        // go over the tokenizers in the bindings and register the ones that are not configured
        for (Map.Entry<String, Class<? extends TokenizerFactory>> entry : tokenizersBindings.tokenizers.entrySet()) {
            String tokenizerName = entry.getKey();
            Class<? extends TokenizerFactory> clazz = entry.getValue();
            // we don't want to re-register one that already exists
            if (tokenizersSettings.containsKey(tokenizerName)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (clazz.getAnnotation(AnalysisSettingsRequired.class) != null) {
                continue;
            }
            // register it as default under the name
            if (indicesAnalysisService != null && indicesAnalysisService.hasTokenizer(tokenizerName)) {
                // don't register it here, we will use explicitly register it in the AnalysisService
                // tokenizerBinder.addBinding(tokenizerName).toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, clazz)).in(Scopes.SINGLETON);
            } else {
                tokenizerBinder.addBinding(tokenizerName).toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, clazz)).in(Scopes.SINGLETON);
            }
        }

        // ANALYZER

        MapBinder<String, AnalyzerProviderFactory> analyzerBinder
                = MapBinder.newMapBinder(binder(), String.class, AnalyzerProviderFactory.class);

        // initial default bindings
        AnalysisBinderProcessor.AnalyzersBindings analyzersBindings = new AnalysisBinderProcessor.AnalyzersBindings();
        for (AnalysisBinderProcessor processor : processors) {
            processor.processAnalyzers(analyzersBindings);
        }
        analyzersBindings.analyzers.putAll(analyzers);

        Map<String, Settings> analyzersSettings = settings.getGroups("index.analysis.analyzer");
        for (Map.Entry<String, Settings> entry : analyzersSettings.entrySet()) {
            String analyzerName = entry.getKey();
            Settings analyzerSettings = entry.getValue();

            Class<? extends AnalyzerProvider> type = null;
            try {
                type = analyzerSettings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "AnalyzerProvider");
            } catch (NoClassSettingsException e) {
                // nothing found, see if its in bindings as a binding name
                if (analyzerSettings.get("type") != null) {
                    type = analyzersBindings.analyzers.get(Strings.toUnderscoreCase(analyzerSettings.get("type")));
                    if (type == null) {
                        type = analyzersBindings.analyzers.get(Strings.toCamelCase(analyzerSettings.get("type")));
                    }
                }
                if (type == null) {
                    // no specific type, check if it has a tokenizer associated with it
                    String tokenizerName = analyzerSettings.get("tokenizer");
                    if (tokenizerName != null) {
                        // we have a tokenizer, use the CustomAnalyzer
                        type = CustomAnalyzerProvider.class;
                    } else {
                        throw new ElasticsearchIllegalArgumentException("failed to find analyzer type [" + analyzerSettings.get("type") + "] or tokenizer for [" + analyzerName + "]", e);
                    }
                }
            }
            if (type == null) {
                // no specific type, check if it has a tokenizer associated with it
                String tokenizerName = analyzerSettings.get("tokenizer");
                if (tokenizerName != null) {
                    // we have a tokenizer, use the CustomAnalyzer
                    type = CustomAnalyzerProvider.class;
                } else {
                    throw new ElasticsearchIllegalArgumentException("failed to find analyzer type [" + analyzerSettings.get("type") + "] or tokenizer for [" + analyzerName + "]");
                }
            }
            analyzerBinder.addBinding(analyzerName).toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, type)).in(Scopes.SINGLETON);
        }


        // go over the analyzers in the bindings and register the ones that are not configured
        for (Map.Entry<String, Class<? extends AnalyzerProvider>> entry : analyzersBindings.analyzers.entrySet()) {
            String analyzerName = entry.getKey();
            Class<? extends AnalyzerProvider> clazz = entry.getValue();
            // we don't want to re-register one that already exists
            if (analyzersSettings.containsKey(analyzerName)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (clazz.getAnnotation(AnalysisSettingsRequired.class) != null) {
                continue;
            }
            // register it as default under the name
            if (indicesAnalysisService != null && indicesAnalysisService.hasAnalyzer(analyzerName)) {
                // don't register it here, we will use explicitly register it in the AnalysisService
                // analyzerBinder.addBinding(analyzerName).toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, clazz)).in(Scopes.SINGLETON);
            } else {
                analyzerBinder.addBinding(analyzerName).toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, clazz)).in(Scopes.SINGLETON);
            }
        }

        bind(AnalysisService.class).in(Scopes.SINGLETON);
    }

    private static class DefaultProcessor extends AnalysisBinderProcessor {

        @Override
        public void processCharFilters(CharFiltersBindings charFiltersBindings) {
            charFiltersBindings.processCharFilter("html_strip", HtmlStripCharFilterFactory.class);
            charFiltersBindings.processCharFilter("pattern_replace", PatternReplaceCharFilterFactory.class);
        }

        @Override
        public void processTokenFilters(TokenFiltersBindings tokenFiltersBindings) {
            tokenFiltersBindings.processTokenFilter("stop", StopTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("reverse", ReverseTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("asciifolding", ASCIIFoldingTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("length", LengthTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("lowercase", LowerCaseTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("uppercase", UpperCaseTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("porter_stem", PorterStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("kstem", KStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("standard", StandardTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("nGram", NGramTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("ngram", NGramTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("edgeNGram", EdgeNGramTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("edge_ngram", EdgeNGramTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("shingle", ShingleTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("unique", UniqueTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("truncate", TruncateTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("trim", TrimTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("limit", LimitTokenCountFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("common_grams", CommonGramsTokenFilterFactory.class);
        }

        @Override
        public void processTokenizers(TokenizersBindings tokenizersBindings) {
            tokenizersBindings.processTokenizer("standard", StandardTokenizerFactory.class);
            tokenizersBindings.processTokenizer("uax_url_email", UAX29URLEmailTokenizerFactory.class);
            tokenizersBindings.processTokenizer("path_hierarchy", PathHierarchyTokenizerFactory.class);
            tokenizersBindings.processTokenizer("keyword", KeywordTokenizerFactory.class);
            tokenizersBindings.processTokenizer("letter", LetterTokenizerFactory.class);
            tokenizersBindings.processTokenizer("lowercase", LowerCaseTokenizerFactory.class);
            tokenizersBindings.processTokenizer("whitespace", WhitespaceTokenizerFactory.class);

            tokenizersBindings.processTokenizer("nGram", NGramTokenizerFactory.class);
            tokenizersBindings.processTokenizer("ngram", NGramTokenizerFactory.class);
            tokenizersBindings.processTokenizer("edgeNGram", EdgeNGramTokenizerFactory.class);
            tokenizersBindings.processTokenizer("edge_ngram", EdgeNGramTokenizerFactory.class);
        }

        @Override
        public void processAnalyzers(AnalyzersBindings analyzersBindings) {
            analyzersBindings.processAnalyzer("default", StandardAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("standard", StandardAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("standard_html_strip", StandardHtmlStripAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("simple", SimpleAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("stop", StopAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("whitespace", WhitespaceAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("keyword", KeywordAnalyzerProvider.class);
        }
    }

    private static class ExtendedProcessor extends AnalysisBinderProcessor {
        @Override
        public void processCharFilters(CharFiltersBindings charFiltersBindings) {
            charFiltersBindings.processCharFilter("mapping", MappingCharFilterFactory.class);
        }

        @Override
        public void processTokenFilters(TokenFiltersBindings tokenFiltersBindings) {
            tokenFiltersBindings.processTokenFilter("snowball", SnowballTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("stemmer", StemmerTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("word_delimiter", WordDelimiterTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("delimited_payload_filter", DelimitedPayloadTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("synonym", SynonymTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("elision", ElisionTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("keep", KeepWordFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("keep_types", KeepTypesFilterFactory.class);

            tokenFiltersBindings.processTokenFilter("pattern_capture", PatternCaptureGroupTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("pattern_replace", PatternReplaceTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("dictionary_decompounder", DictionaryCompoundWordTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("hyphenation_decompounder", HyphenationCompoundWordTokenFilterFactory.class);

            tokenFiltersBindings.processTokenFilter("arabic_stem", ArabicStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("brazilian_stem", BrazilianStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("czech_stem", CzechStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("dutch_stem", DutchStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("french_stem", FrenchStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("german_stem", GermanStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("russian_stem", RussianStemTokenFilterFactory.class);

            tokenFiltersBindings.processTokenFilter("keyword_marker", KeywordMarkerTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("stemmer_override", StemmerOverrideTokenFilterFactory.class);

            tokenFiltersBindings.processTokenFilter("arabic_normalization", ArabicNormalizationFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("german_normalization", GermanNormalizationFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("hindi_normalization", HindiNormalizationFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("indic_normalization", IndicNormalizationFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("sorani_normalization", SoraniNormalizationFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("persian_normalization", PersianNormalizationFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("scandinavian_normalization", ScandinavianNormalizationFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("scandinavian_folding", ScandinavianFoldingFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("serbian_normalization", SerbianNormalizationFilterFactory.class);

            tokenFiltersBindings.processTokenFilter("hunspell", HunspellTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("cjk_bigram", CJKBigramFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("cjk_width", CJKWidthFilterFactory.class);
            
            tokenFiltersBindings.processTokenFilter("apostrophe", ApostropheFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("classic", ClassicFilterFactory.class);


        }

        @Override
        public void processTokenizers(TokenizersBindings tokenizersBindings) {
            tokenizersBindings.processTokenizer("pattern", PatternTokenizerFactory.class);
            tokenizersBindings.processTokenizer("classic", ClassicTokenizerFactory.class);
            tokenizersBindings.processTokenizer("thai", ThaiTokenizerFactory.class);
        }

        @Override
        public void processAnalyzers(AnalyzersBindings analyzersBindings) {
            analyzersBindings.processAnalyzer("pattern", PatternAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("snowball", SnowballAnalyzerProvider.class);

            analyzersBindings.processAnalyzer("arabic", ArabicAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("armenian", ArmenianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("basque", BasqueAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("brazilian", BrazilianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("bulgarian", BulgarianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("catalan", CatalanAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("chinese", ChineseAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("cjk", CjkAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("czech", CzechAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("danish", DanishAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("dutch", DutchAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("english", EnglishAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("finnish", FinnishAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("french", FrenchAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("galician", GalicianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("german", GermanAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("greek", GreekAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("hindi", HindiAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("hungarian", HungarianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("indonesian", IndonesianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("irish", IrishAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("italian", ItalianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("latvian", LatvianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("norwegian", NorwegianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("persian", PersianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("portuguese", PortugueseAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("romanian", RomanianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("russian", RussianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("sorani", SoraniAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("spanish", SpanishAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("swedish", SwedishAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("turkish", TurkishAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("thai", ThaiAnalyzerProvider.class);
        }
    }
}
