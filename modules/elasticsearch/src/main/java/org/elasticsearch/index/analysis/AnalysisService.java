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

import org.elasticsearch.util.gcommon.collect.ImmutableMap;
import org.elasticsearch.util.guice.inject.Inject;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.component.CloseableComponent;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;

import javax.annotation.Nullable;
import java.util.Map;

import static org.elasticsearch.util.gcommon.collect.Maps.*;

/**
 * @author kimchy (Shay Banon)
 */
public class AnalysisService extends AbstractIndexComponent implements CloseableComponent {

    private final ImmutableMap<String, AnalyzerProvider> analyzerProviders;

    private final ImmutableMap<String, NamedAnalyzer> analyzers;

    private final ImmutableMap<String, TokenizerFactory> tokenizers;

    private final ImmutableMap<String, TokenFilterFactory> tokenFilters;

    public AnalysisService(Index index) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, null, null, null);
    }

    @Inject public AnalysisService(Index index, @IndexSettings Settings indexSettings,
                                   @Nullable Map<String, AnalyzerProviderFactory> analyzerFactoryFactories,
                                   @Nullable Map<String, TokenizerFactoryFactory> tokenizerFactoryFactories,
                                   @Nullable Map<String, TokenFilterFactoryFactory> tokenFilterFactoryFactories) {
        super(index, indexSettings);

        Map<String, AnalyzerProvider> analyzerProviders = newHashMap();
        if (analyzerFactoryFactories != null) {
            Map<String, Settings> analyzersSettings = indexSettings.getGroups("index.analysis.analyzer");
            for (Map.Entry<String, AnalyzerProviderFactory> entry : analyzerFactoryFactories.entrySet()) {
                String analyzerName = entry.getKey();
                AnalyzerProviderFactory analyzerFactoryFactory = entry.getValue();

                Settings analyzerSettings = analyzersSettings.get(analyzerName);
                if (analyzerSettings == null) {
                    analyzerSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
                }

                AnalyzerProvider analyzerFactory = analyzerFactoryFactory.create(analyzerName, analyzerSettings);
                analyzerProviders.put(analyzerName, analyzerFactory);
            }
        }

        // add some defaults
        if (!analyzerProviders.containsKey("standard")) {
            analyzerProviders.put("standard", new StandardAnalyzerProvider(index, indexSettings, "standard", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("simple")) {
            analyzerProviders.put("simple", new SimpleAnalyzerProvider(index, indexSettings, "simple", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("stop")) {
            analyzerProviders.put("stop", new StopAnalyzerProvider(index, indexSettings, "stop", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("whitespace")) {
            analyzerProviders.put("whitespace", new WhitespaceAnalyzerProvider(index, indexSettings, "whitespace", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("keyword")) {
            analyzerProviders.put("keyword", new KeywordAnalyzerProvider(index, indexSettings, "keyword", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("default")) {
            analyzerProviders.put("default", new StandardAnalyzerProvider(index, indexSettings, "default", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("default_index")) {
            analyzerProviders.put("default_index", analyzerProviders.get("default"));
        }
        if (!analyzerProviders.containsKey("default_search")) {
            analyzerProviders.put("default_search", analyzerProviders.get("default"));
        }

        // extended analyzers defaults
        if (!analyzerProviders.containsKey("arabic")) {
            analyzerProviders.put("arabic", new ArabicAnalyzerProvider(index, indexSettings, "arabic", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("brazilian")) {
            analyzerProviders.put("brazilian", new BrazilianAnalyzerProvider(index, indexSettings, "brazilian", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("chinese")) {
            analyzerProviders.put("chinese", new ChineseAnalyzerProvider(index, indexSettings, "chinese", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("cjk")) {
            analyzerProviders.put("cjk", new ChineseAnalyzerProvider(index, indexSettings, "cjk", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("czech")) {
            analyzerProviders.put("czech", new CzechAnalyzerProvider(index, indexSettings, "czech", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("dutch")) {
            analyzerProviders.put("dutch", new DutchAnalyzerProvider(index, indexSettings, "dutch", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("french")) {
            analyzerProviders.put("french", new FrenchAnalyzerProvider(index, indexSettings, "french", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("german")) {
            analyzerProviders.put("german", new GermanAnalyzerProvider(index, indexSettings, "german", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("greek")) {
            analyzerProviders.put("greek", new GreekAnalyzerProvider(index, indexSettings, "greek", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("persian")) {
            analyzerProviders.put("persian", new PersianAnalyzerProvider(index, indexSettings, "persian", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("russian")) {
            analyzerProviders.put("russian", new RussianAnalyzerProvider(index, indexSettings, "russian", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("thai")) {
            analyzerProviders.put("thai", new ThaiAnalyzerProvider(index, indexSettings, "thai", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }


        this.analyzerProviders = ImmutableMap.copyOf(analyzerProviders);

        Map<String, NamedAnalyzer> analyzers = newHashMap();
        for (AnalyzerProvider analyzerFactory : analyzerProviders.values()) {
            analyzers.put(analyzerFactory.name(), new NamedAnalyzer(analyzerFactory.name(), analyzerFactory.get()));
        }
        this.analyzers = ImmutableMap.copyOf(analyzers);

        Map<String, TokenizerFactory> tokenizers = newHashMap();
        if (tokenizerFactoryFactories != null) {
            Map<String, Settings> tokenizersSettings = indexSettings.getGroups("index.analysis.tokenizer");
            for (Map.Entry<String, TokenizerFactoryFactory> entry : tokenizerFactoryFactories.entrySet()) {
                String tokenizerName = entry.getKey();
                TokenizerFactoryFactory tokenizerFactoryFactory = entry.getValue();

                Settings tokenizerSettings = tokenizersSettings.get(tokenizerName);
                if (tokenizerSettings == null) {
                    tokenizerSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
                }

                TokenizerFactory tokenizerFactory = tokenizerFactoryFactory.create(tokenizerName, tokenizerSettings);
                tokenizers.put(tokenizerName, tokenizerFactory);
            }
        }
        this.tokenizers = ImmutableMap.copyOf(tokenizers);

        Map<String, TokenFilterFactory> tokenFilters = newHashMap();
        if (tokenFilterFactoryFactories != null) {
            Map<String, Settings> tokenFiltersSettings = indexSettings.getGroups("index.analysis.filter");
            for (Map.Entry<String, TokenFilterFactoryFactory> entry : tokenFilterFactoryFactories.entrySet()) {
                String tokenFilterName = entry.getKey();
                TokenFilterFactoryFactory tokenFilterFactoryFactory = entry.getValue();

                Settings tokenFilterSettings = tokenFiltersSettings.get(tokenFilterName);
                if (tokenFilterSettings == null) {
                    tokenFilterSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
                }

                TokenFilterFactory tokenFilterFactory = tokenFilterFactoryFactory.create(tokenFilterName, tokenFilterSettings);
                tokenFilters.put(tokenFilterName, tokenFilterFactory);
            }
        }
        this.tokenFilters = ImmutableMap.copyOf(tokenFilters);
    }

    public void close() {
        for (Analyzer analyzer : analyzers.values()) {
            analyzer.close();
        }
    }

    public NamedAnalyzer analyzer(String name) {
        return analyzers.get(name);
    }

    public NamedAnalyzer defaultAnalyzer() {
        return analyzers.get("default");
    }

    public NamedAnalyzer defaultIndexAnalyzer() {
        return defaultAnalyzer();
    }

    public NamedAnalyzer defaultSearchAnalyzer() {
        return defaultAnalyzer();
    }

    public AnalyzerProvider analyzerProvider(String name) {
        return analyzerProviders.get(name);
    }

    public TokenizerFactory tokenizer(String name) {
        return tokenizers.get(name);
    }

    public TokenFilterFactory tokenFilter(String name) {
        return tokenFilters.get(name);
    }
}
