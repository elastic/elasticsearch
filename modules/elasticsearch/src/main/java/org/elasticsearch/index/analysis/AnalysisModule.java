/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.analysis.compound.HyphenationCompoundWordTokenFilterFactory;
import org.elasticsearch.index.analysis.phonetic.PhoneticTokenFilterFactory;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.util.LinkedList;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
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
            private final MapBinder<String, TokenizerFactoryFactory> binder;
            private final Map<String, Settings> groupSettings;
            private final IndicesAnalysisService indicesAnalysisService;

            public TokenizersBindings(MapBinder<String, TokenizerFactoryFactory> binder, Map<String, Settings> groupSettings, IndicesAnalysisService indicesAnalysisService) {
                this.binder = binder;
                this.groupSettings = groupSettings;
                this.indicesAnalysisService = indicesAnalysisService;
            }

            public void processTokenizer(String name, Class<? extends TokenizerFactory> tokenizerFactory) {
                if (!groupSettings.containsKey(name)) {
                    if (indicesAnalysisService != null && indicesAnalysisService.hasTokenizer(name)) {
                        binder.addBinding(name).toInstance(indicesAnalysisService.tokenizerFactoryFactory(name));
                    } else {
                        binder.addBinding(name).toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, tokenizerFactory)).in(Scopes.SINGLETON);
                    }
                }
            }
        }

        public void processAnalyzers(AnalyzersBindings analyzersBindings) {

        }

        public static class AnalyzersBindings {
            private final MapBinder<String, AnalyzerProviderFactory> binder;
            private final Map<String, Settings> groupSettings;
            private final IndicesAnalysisService indicesAnalysisService;

            public AnalyzersBindings(MapBinder<String, AnalyzerProviderFactory> binder, Map<String, Settings> groupSettings, IndicesAnalysisService indicesAnalysisService) {
                this.binder = binder;
                this.groupSettings = groupSettings;
                this.indicesAnalysisService = indicesAnalysisService;
            }

            public void processAnalyzer(String name, Class<? extends AnalyzerProvider> analyzerProvider) {
                if (!groupSettings.containsKey(name)) {
                    if (indicesAnalysisService != null && indicesAnalysisService.hasAnalyzer(name)) {
                        binder.addBinding(name).toInstance(indicesAnalysisService.analyzerProviderFactory(name));
                    } else {
                        binder.addBinding(name).toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, analyzerProvider)).in(Scopes.SINGLETON);
                    }
                }
            }
        }
    }

    private final Settings settings;

    private final IndicesAnalysisService indicesAnalysisService;

    private final LinkedList<AnalysisBinderProcessor> processors = Lists.newLinkedList();

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

    @Override protected void configure() {
        MapBinder<String, CharFilterFactoryFactory> charFilterBinder
                = MapBinder.newMapBinder(binder(), String.class, CharFilterFactoryFactory.class);

        // CHAR FILTERS

        AnalysisBinderProcessor.CharFiltersBindings charFiltersBindings = new AnalysisBinderProcessor.CharFiltersBindings();
        for (AnalysisBinderProcessor processor : processors) {
            processor.processCharFilters(charFiltersBindings);
        }

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
                    type = charFiltersBindings.charFilters.get(charFilterSettings.get("type"));
                }
            }
            if (type == null) {
                // nothing found, see if its in bindings as a binding name
                throw new ElasticSearchIllegalArgumentException("Char Filter [" + charFilterName + "] must have a type associated with it");
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
                charFilterBinder.addBinding(charFilterName).toInstance(indicesAnalysisService.charFilterFactoryFactory(charFilterName));
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
                    type = tokenFiltersBindings.tokenFilters.get(tokenFilterSettings.get("type"));
                }
            }
            if (type == null) {
                throw new ElasticSearchIllegalArgumentException("Token Filter [" + tokenFilterName + "] must have a type associated with it");
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
                tokenFilterBinder.addBinding(tokenFilterName).toInstance(indicesAnalysisService.tokenFilterFactoryFactory(tokenFilterName));
            } else {
                tokenFilterBinder.addBinding(tokenFilterName).toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, clazz)).in(Scopes.SINGLETON);
            }
        }

        // TOKENIZER

        MapBinder<String, TokenizerFactoryFactory> tokenizerBinder
                = MapBinder.newMapBinder(binder(), String.class, TokenizerFactoryFactory.class);

        Map<String, Settings> tokenizersSettings = settings.getGroups("index.analysis.tokenizer");
        for (Map.Entry<String, Settings> entry : tokenizersSettings.entrySet()) {
            String tokenizerName = entry.getKey();
            Settings tokenizerSettings = entry.getValue();

            Class<? extends TokenizerFactory> type = tokenizerSettings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "TokenizerFactory");
            if (type == null) {
                throw new ElasticSearchIllegalArgumentException("Tokenizer [" + tokenizerName + "] must have a type associated with it");
            }
            tokenizerBinder.addBinding(tokenizerName).toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, type)).in(Scopes.SINGLETON);
        }

        AnalysisBinderProcessor.TokenizersBindings tokenizersBindings = new AnalysisBinderProcessor.TokenizersBindings(tokenizerBinder, tokenizersSettings, indicesAnalysisService);
        for (AnalysisBinderProcessor processor : processors) {
            processor.processTokenizers(tokenizersBindings);
        }

        // ANALYZER

        MapBinder<String, AnalyzerProviderFactory> analyzerBinder
                = MapBinder.newMapBinder(binder(), String.class, AnalyzerProviderFactory.class);

        Map<String, Settings> analyzersSettings = settings.getGroups("index.analysis.analyzer");
        for (Map.Entry<String, Settings> entry : analyzersSettings.entrySet()) {
            String analyzerName = entry.getKey();
            Settings analyzerSettings = entry.getValue();
            Class<? extends AnalyzerProvider> type = analyzerSettings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "AnalyzerProvider");
            if (type == null) {
                // no specific type, check if it has a tokenizer associated with it
                String tokenizerName = analyzerSettings.get("tokenizer");
                if (tokenizerName != null) {
                    // we have a tokenizer, use the CustomAnalyzer
                    type = CustomAnalyzerProvider.class;
                } else {
                    throw new ElasticSearchIllegalArgumentException("Analyzer [" + analyzerName + "] must have a type associated with it or a tokenizer");
                }
            }
            analyzerBinder.addBinding(analyzerName).toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, type)).in(Scopes.SINGLETON);
        }

        AnalysisBinderProcessor.AnalyzersBindings analyzersBindings = new AnalysisBinderProcessor.AnalyzersBindings(analyzerBinder, analyzersSettings, indicesAnalysisService);
        for (AnalysisBinderProcessor processor : processors) {
            processor.processAnalyzers(analyzersBindings);
        }

        bind(AnalysisService.class).in(Scopes.SINGLETON);
    }

    private static class DefaultProcessor extends AnalysisBinderProcessor {

        @Override public void processCharFilters(CharFiltersBindings charFiltersBindings) {
            charFiltersBindings.processCharFilter("html_strip", HtmlStripCharFilterFactory.class);
            charFiltersBindings.processCharFilter("htmlStrip", HtmlStripCharFilterFactory.class);
        }

        @Override public void processTokenFilters(TokenFiltersBindings tokenFiltersBindings) {
            tokenFiltersBindings.processTokenFilter("stop", StopTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("reverse", ReverseTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("asciifolding", ASCIIFoldingTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("length", LengthTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("lowercase", LowerCaseTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("porterStem", PorterStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("porter_stem", PorterStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("standard", StandardTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("nGram", NGramTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("ngram", NGramTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("edgeNGram", EdgeNGramTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("edge_ngram", EdgeNGramTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("shingle", ShingleTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("phonetic", PhoneticTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("dictionaryDecompounder", DictionaryCompoundWordTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("dictionary_decompounder", DictionaryCompoundWordTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("hyphenationDecompounder", HyphenationCompoundWordTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("hypennation_decompounder", HyphenationCompoundWordTokenFilterFactory.class);
        }

        @Override public void processTokenizers(TokenizersBindings tokenizersBindings) {
            tokenizersBindings.processTokenizer("standard", StandardTokenizerFactory.class);
            tokenizersBindings.processTokenizer("uax_url_email", UAX29URLEmailTokenizerFactory.class);
            tokenizersBindings.processTokenizer("uaxUrlEmail", UAX29URLEmailTokenizerFactory.class);
            tokenizersBindings.processTokenizer("path_hierarchy", PathHierarchyTokenizerFactory.class);
            tokenizersBindings.processTokenizer("pathHierarchy", PathHierarchyTokenizerFactory.class);
            tokenizersBindings.processTokenizer("keyword", KeywordTokenizerFactory.class);
            tokenizersBindings.processTokenizer("letter", LetterTokenizerFactory.class);
            tokenizersBindings.processTokenizer("lowercase", LowerCaseTokenizerFactory.class);
            tokenizersBindings.processTokenizer("whitespace", WhitespaceTokenizerFactory.class);

            tokenizersBindings.processTokenizer("nGram", NGramTokenizerFactory.class);
            tokenizersBindings.processTokenizer("ngram", NGramTokenizerFactory.class);
            tokenizersBindings.processTokenizer("edgeNGram", EdgeNGramTokenizerFactory.class);
            tokenizersBindings.processTokenizer("edge_ngram", EdgeNGramTokenizerFactory.class);
        }

        @Override public void processAnalyzers(AnalyzersBindings analyzersBindings) {
            analyzersBindings.processAnalyzer("default", StandardAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("standard", StandardAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("standard_html_strip", StandardHtmlStripAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("standardHtmlStrip", StandardHtmlStripAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("simple", SimpleAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("stop", StopAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("whitespace", WhitespaceAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("keyword", KeywordAnalyzerProvider.class);
        }
    }

    private static class ExtendedProcessor extends AnalysisBinderProcessor {
        @Override public void processTokenFilters(TokenFiltersBindings tokenFiltersBindings) {
            tokenFiltersBindings.processTokenFilter("snowball", SnowballTokenFilterFactory.class);

            tokenFiltersBindings.processTokenFilter("arabicStem", ArabicStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("arabic_stem", ArabicStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("brazilianStem", BrazilianStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("brazilian_stem", BrazilianStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("czechStem", CzechStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("czech_stem", CzechStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("dutchStem", DutchStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("dutch_stem", DutchStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("frenchStem", FrenchStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("french_stem", FrenchStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("germanStem", GermanStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("german_stem", GermanStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("russianStem", RussianStemTokenFilterFactory.class);
            tokenFiltersBindings.processTokenFilter("russian_stem", RussianStemTokenFilterFactory.class);
        }

        @Override public void processTokenizers(TokenizersBindings tokenizersBindings) {
        }

        @Override public void processAnalyzers(AnalyzersBindings analyzersBindings) {
            analyzersBindings.processAnalyzer("pattern", PatternAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("snowball", SnowballAnalyzerProvider.class);

            analyzersBindings.processAnalyzer("arabic", ArabicAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("brazilian", BrazilianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("chinese", ChineseAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("cjk", CjkAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("czech", CzechAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("dutch", DutchAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("french", FrenchAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("german", GermanAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("greek", GreekAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("persian", PersianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("russian", RussianAnalyzerProvider.class);
            analyzersBindings.processAnalyzer("thai", ThaiAnalyzerProvider.class);
        }
    }
}
