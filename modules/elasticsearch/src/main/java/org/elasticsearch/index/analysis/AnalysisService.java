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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import javax.annotation.Nullable;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.*;

/**
 * @author kimchy (Shay Banon)
 */
public class AnalysisService extends AbstractIndexComponent implements CloseableComponent {

    private final ImmutableMap<String, NamedAnalyzer> analyzers;

    private final ImmutableMap<String, TokenizerFactory> tokenizers;

    private final ImmutableMap<String, CharFilterFactory> charFilters;

    private final ImmutableMap<String, TokenFilterFactory> tokenFilters;

    public AnalysisService(Index index) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, null, null, null, null);
    }

    @Inject public AnalysisService(Index index, @IndexSettings Settings indexSettings,
                                   @Nullable Map<String, AnalyzerProviderFactory> analyzerFactoryFactories,
                                   @Nullable Map<String, TokenizerFactoryFactory> tokenizerFactoryFactories,
                                   @Nullable Map<String, CharFilterFactoryFactory> charFilterFactoryFactories,
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

        if (!analyzerProviders.containsKey("default")) {
            analyzerProviders.put("default", new StandardAnalyzerProvider(index, indexSettings, "default", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("default_index")) {
            analyzerProviders.put("default_index", analyzerProviders.get("default"));
        }
        if (!analyzerProviders.containsKey("default_search")) {
            analyzerProviders.put("default_search", analyzerProviders.get("default"));
        }

        Map<String, NamedAnalyzer> analyzers = newHashMap();
        for (AnalyzerProvider analyzerFactory : analyzerProviders.values()) {
            NamedAnalyzer analyzer = new NamedAnalyzer(analyzerFactory.name(), analyzerFactory.scope(), analyzerFactory.get());
            analyzers.put(analyzerFactory.name(), analyzer);
            String strAliases = indexSettings.get("index.analysis.analyzer." + analyzerFactory.name() + ".alias");
            if (strAliases != null) {
                for (String alias : Strings.commaDelimitedListToStringArray(strAliases)) {
                    analyzers.put(alias, analyzer);
                }
            }
            String[] aliases = indexSettings.getAsArray("index.analysis.analyzer." + analyzerFactory.name() + ".alias");
            for (String alias : aliases) {
                analyzers.put(alias, analyzer);
            }
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

        Map<String, CharFilterFactory> charFilters = newHashMap();
        if (charFilterFactoryFactories != null) {
            Map<String, Settings> charFiltersSettings = indexSettings.getGroups("index.analysis.char_filter");
            for (Map.Entry<String, CharFilterFactoryFactory> entry : charFilterFactoryFactories.entrySet()) {
                String charFilterName = entry.getKey();
                CharFilterFactoryFactory charFilterFactoryFactory = entry.getValue();

                Settings charFilterSettings = charFiltersSettings.get(charFilterName);
                if (charFilterSettings == null) {
                    charFilterSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
                }

                CharFilterFactory tokenFilterFactory = charFilterFactoryFactory.create(charFilterName, charFilterSettings);
                charFilters.put(charFilterName, tokenFilterFactory);
            }
        }
        this.charFilters = ImmutableMap.copyOf(charFilters);

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
        for (NamedAnalyzer analyzer : analyzers.values()) {
            if (analyzer.scope() == AnalyzerScope.INDEX) {
                try {
                    analyzer.close();
                } catch (NullPointerException e) {
                    // because analyzers are aliased, they might be closed several times
                    // an NPE is thrown in this case, so ignore....
                } catch (Exception e) {
                    logger.debug("failed to close analyzer " + analyzer);
                }
            }
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

    public TokenizerFactory tokenizer(String name) {
        return tokenizers.get(name);
    }

    public CharFilterFactory charFilter(String name) {
        return charFilters.get(name);
    }

    public TokenFilterFactory tokenFilter(String name) {
        return tokenFilters.get(name);
    }
}
