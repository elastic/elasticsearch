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

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryProvider;
import com.google.inject.multibindings.MapBinder;
import org.elasticsearch.util.settings.Settings;

import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class AnalysisModule extends AbstractModule {

    private final Settings settings;

    public AnalysisModule(Settings settings) {
        this.settings = settings;
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
        // add defaults
        if (!tokenFiltersSettings.containsKey("stop")) {
            tokenFilterBinder.addBinding("stop").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, StopTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenFiltersSettings.containsKey("asciifolding")) {
            tokenFilterBinder.addBinding("asciifolding").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, ASCIIFoldingTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenFiltersSettings.containsKey("length")) {
            tokenFilterBinder.addBinding("length").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, LengthTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenFiltersSettings.containsKey("lowercase")) {
            tokenFilterBinder.addBinding("lowercase").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, LowerCaseTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenFiltersSettings.containsKey("porterStem")) {
            tokenFilterBinder.addBinding("porterStem").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, PorterStemTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenFiltersSettings.containsKey("standard")) {
            tokenFilterBinder.addBinding("standard").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, StandardTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenFiltersSettings.containsKey("nGram")) {
            tokenFilterBinder.addBinding("nGram").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, NGramTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenFiltersSettings.containsKey("edgeNGram")) {
            tokenFilterBinder.addBinding("edgeNGram").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, EdgeNGramTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenFiltersSettings.containsKey("shingle")) {
            tokenFilterBinder.addBinding("shingle").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, ShingleTokenFilterFactory.class)).in(Scopes.SINGLETON);
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
        // add defaults
        if (!tokenizersSettings.containsKey("standard")) {
            tokenizerBinder.addBinding("standard").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, StandardTokenizerFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenizersSettings.containsKey("keyword")) {
            tokenizerBinder.addBinding("keyword").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, KeywordTokenizerFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenizersSettings.containsKey("letter")) {
            tokenizerBinder.addBinding("letter").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, LetterTokenizerFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenizersSettings.containsKey("lowercase")) {
            tokenizerBinder.addBinding("lowercase").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, LowerCaseTokenizerFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenizersSettings.containsKey("whitespace")) {
            tokenizerBinder.addBinding("whitespace").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, WhitespaceTokenizerFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenizersSettings.containsKey("nGram")) {
            tokenizerBinder.addBinding("nGram").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, NGramTokenizerFactory.class)).in(Scopes.SINGLETON);
        }
        if (!tokenizersSettings.containsKey("edgeNGram")) {
            tokenizerBinder.addBinding("edgeNGram").toProvider(FactoryProvider.newFactory(TokenizerFactoryFactory.class, EdgeNGramTokenizerFactory.class)).in(Scopes.SINGLETON);
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

        bind(AnalysisService.class).in(Scopes.SINGLETON);
    }
}
