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

import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.guice.inject.AbstractModule;
import org.elasticsearch.util.guice.inject.Scopes;
import org.elasticsearch.util.guice.inject.assistedinject.FactoryProvider;
import org.elasticsearch.util.guice.inject.multibindings.MapBinder;
import org.elasticsearch.util.settings.Settings;

import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class AnalysisModule extends AbstractModule {

    public static interface AnalysisBinderProcessor {
        void processTokenFilters(MapBinder<String, TokenFilterFactoryFactory> binder, Map<String, Settings> groupSettings);

        void processTokenizers(MapBinder<String, TokenizerFactoryFactory> binder, Map<String, Settings> groupSettings);

        void processAnalyzers(MapBinder<String, AnalyzerProviderFactory> binder, Map<String, Settings> groupSettings);
    }

    private final Settings settings;

    private final List<AnalysisBinderProcessor> processors = Lists.newArrayList();

    public AnalysisModule(Settings settings) {
        this.settings = settings;
        processors.add(new DefaultProcessor());
        try {
            processors.add(new ExtendedProcessor());
        } catch (Throwable t) {
            // ignore. no extended ones
        }
    }

    public AnalysisModule addProcessor(AnalysisBinderProcessor processor) {
        processors.add(processor);
        return this;
    }

    @Override protected void configure() {
        MapBinder<String, TokenFilterFactoryFactory> tokenFilterBinder
                = MapBinder.newMapBinder(binder(), String.class, TokenFilterFactoryFactory.class);

        Map<String, Settings> tokenFiltersSettings = settings.getGroups("index.analysis.filter");
        for (Map.Entry<String, Settings> entry : tokenFiltersSettings.entrySet()) {
            String tokenFilterName = entry.getKey();
            Settings tokenFilterSettings = entry.getValue();

            Class<? extends TokenFilterFactory> type = tokenFilterSettings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "TokenFilterFactory");
            if (type == null) {
                throw new IllegalArgumentException("Token Filter [" + tokenFilterName + "] must have a type associated with it");
            }
            tokenFilterBinder.addBinding(tokenFilterName).toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, type)).in(Scopes.SINGLETON);
        }

        for (AnalysisBinderProcessor processor : processors) {
            processor.processTokenFilters(tokenFilterBinder, tokenFiltersSettings);
        }

        MapBinder<String, TokenizerFactoryFactory> tokenizerBinder
                = MapBinder.newMapBinder(binder(), String.class, TokenizerFactoryFactory.class);

        Map<String, Settings> tokenizersSettings = settings.getGroups("index.analysis.tokenizer");
        for (Map.Entry<String, Settings> entry : tokenizersSettings.entrySet()) {
            String tokenizerName = entry.getKey();
            Settings tokenizerSettings = entry.getValue();

            Class<? extends TokenizerFactory> type = tokenizerSettings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "TokenizerFactory");
            if (type == null) {
                throw new IllegalArgumentException("Tokenizer [" + tokenizerName + "] must have a type associated with it");
            }
            tokenizerBinder.addBinding(tokenizerName).toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, type)).in(Scopes.SINGLETON);
        }

        for (AnalysisBinderProcessor processor : processors) {
            processor.processTokenizers(tokenizerBinder, tokenizersSettings);
        }

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
                    throw new IllegalArgumentException("Analyzer [" + analyzerName + "] must have a type associated with it or a tokenizer");
                }
            }
            analyzerBinder.addBinding(analyzerName).toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, type)).in(Scopes.SINGLETON);
        }

        for (AnalysisBinderProcessor processor : processors) {
            processor.processAnalyzers(analyzerBinder, analyzersSettings);
        }

        bind(AnalysisService.class).in(Scopes.SINGLETON);
    }

    private static class DefaultProcessor implements AnalysisBinderProcessor {
        @Override public void processTokenFilters(MapBinder<String, TokenFilterFactoryFactory> binder, Map<String, Settings> groupSettings) {
            // add defaults
            if (!groupSettings.containsKey("stop")) {
                binder.addBinding("stop").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, StopTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("asciifolding")) {
                binder.addBinding("asciifolding").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, ASCIIFoldingTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("length")) {
                binder.addBinding("length").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, LengthTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("lowercase")) {
                binder.addBinding("lowercase").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, LowerCaseTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("porterStem")) {
                binder.addBinding("porterStem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, PorterStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("porter_stem")) {
                binder.addBinding("porter_stem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, PorterStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("standard")) {
                binder.addBinding("standard").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, StandardTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("nGram")) {
                binder.addBinding("nGram").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, NGramTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("ngram")) {
                binder.addBinding("ngram").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, NGramTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("edgeNGram")) {
                binder.addBinding("edgeNGram").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, EdgeNGramTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("edge_ngram")) {
                binder.addBinding("edge_ngram").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, EdgeNGramTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("shingle")) {
                binder.addBinding("shingle").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, ShingleTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
        }

        @Override public void processTokenizers(MapBinder<String, TokenizerFactoryFactory> binder, Map<String, Settings> groupSettings) {
            // add defaults
            if (!groupSettings.containsKey("standard")) {
                binder.addBinding("standard").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, StandardTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("keyword")) {
                binder.addBinding("keyword").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, KeywordTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("letter")) {
                binder.addBinding("letter").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, LetterTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("lowercase")) {
                binder.addBinding("lowercase").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, LowerCaseTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("whitespace")) {
                binder.addBinding("whitespace").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, WhitespaceTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
        }

        @Override public void processAnalyzers(MapBinder<String, AnalyzerProviderFactory> binder, Map<String, Settings> groupSettings) {
            if (!groupSettings.containsKey("standard")) {
                binder.addBinding("standard").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, StandardAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("simple")) {
                binder.addBinding("simple").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, SimpleAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("stop")) {
                binder.addBinding("stop").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, StopAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("whitespace")) {
                binder.addBinding("whitespace").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, WhitespaceAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("keyword")) {
                binder.addBinding("keyword").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, KeywordAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
        }
    }

    private static class ExtendedProcessor implements AnalysisBinderProcessor {
        @Override public void processTokenFilters(MapBinder<String, TokenFilterFactoryFactory> binder, Map<String, Settings> groupSettings) {
            if (!groupSettings.containsKey("arabicStem")) {
                binder.addBinding("arabicStem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, ArabicStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("arabic_stem")) {
                binder.addBinding("arabic_stem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, ArabicStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("brazilianStem")) {
                binder.addBinding("brazilianStem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, BrazilianStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("brazilian_stem")) {
                binder.addBinding("brazilian_stem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, BrazilianStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("dutchStem")) {
                binder.addBinding("dutchStem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, DutchStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("dutch_stem")) {
                binder.addBinding("dutch_stem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, DutchStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("frenchStem")) {
                binder.addBinding("frenchStem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, FrenchStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("french_stem")) {
                binder.addBinding("french_stem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, FrenchStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("germanStem")) {
                binder.addBinding("germanStem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, GermanStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("german_stem")) {
                binder.addBinding("german_stem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, GermanStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("russianStem")) {
                binder.addBinding("russianStem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, RussianStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("russian_stem")) {
                binder.addBinding("russian_stem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, RussianStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
            }
        }

        @Override public void processTokenizers(MapBinder<String, TokenizerFactoryFactory> binder, Map<String, Settings> groupSettings) {
            if (!groupSettings.containsKey("nGram")) {
                binder.addBinding("nGram").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, NGramTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("ngram")) {
                binder.addBinding("ngram").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, NGramTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("edgeNGram")) {
                binder.addBinding("edgeNGram").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, EdgeNGramTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("edge_ngram")) {
                binder.addBinding("edge_ngram").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, EdgeNGramTokenizerFactory.class)).in(Scopes.SINGLETON);
            }
        }

        @Override public void processAnalyzers(MapBinder<String, AnalyzerProviderFactory> binder, Map<String, Settings> groupSettings) {
            if (!groupSettings.containsKey("arabic")) {
                binder.addBinding("arabic").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, ArabicAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("brazilian")) {
                binder.addBinding("brazilian").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, BrazilianAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("chinese")) {
                binder.addBinding("chinese").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, ChineseAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("cjk")) {
                binder.addBinding("cjk").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, ChineseAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("czech")) {
                binder.addBinding("czech").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, CzechAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("dutch")) {
                binder.addBinding("dutch").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, DutchAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("french")) {
                binder.addBinding("french").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, FrenchAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("german")) {
                binder.addBinding("german").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, GermanAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("greek")) {
                binder.addBinding("greek").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, GreekAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("persian")) {
                binder.addBinding("persian").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, PersianAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("russian")) {
                binder.addBinding("russian").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, RussianAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
            if (!groupSettings.containsKey("thai")) {
                binder.addBinding("thai").toProvider(FactoryProvider.newFactory(AnalyzerProviderFactory.class, ThaiAnalyzerProvider.class)).in(Scopes.SINGLETON);
            }
        }
    }
}
