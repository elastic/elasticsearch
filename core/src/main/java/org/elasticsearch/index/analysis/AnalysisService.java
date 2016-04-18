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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.io.Closeable;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Manages configuring and storing analyzers, tokenizers, char filters and token filters.
 */
public class AnalysisService extends AbstractIndexComponent implements Closeable {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(AnalysisService.class));

    private final ImmutableMap<String, NamedAnalyzer> analyzers;
    private final ImmutableMap<String, TokenizerFactory> tokenizers;
    private final ImmutableMap<String, CharFilterFactory> charFilters;
    private final ImmutableMap<String, TokenFilterFactory> tokenFilters;

    private final NamedAnalyzer defaultIndexAnalyzer;
    private final NamedAnalyzer defaultSearchAnalyzer;
    private final NamedAnalyzer defaultSearchQuoteAnalyzer;

    public AnalysisService(Index index, Settings indexSettings) {
        this(index, indexSettings, null, null, null, null, null);
    }


    @Inject
    public AnalysisService(Index index, IndexSettingsService indexSettingsService, @Nullable IndicesAnalysisService indicesAnalysisService,
                           @Nullable Map<String, AnalyzerProviderFactory> analyzerFactoryFactories,
                           @Nullable Map<String, TokenizerFactoryFactory> tokenizerFactoryFactories,
                           @Nullable Map<String, CharFilterFactoryFactory> charFilterFactoryFactories,
                           @Nullable Map<String, TokenFilterFactoryFactory> tokenFilterFactoryFactories) {
        this(index, indexSettingsService.getSettings(), indicesAnalysisService, analyzerFactoryFactories, tokenizerFactoryFactories,
                charFilterFactoryFactories, tokenFilterFactoryFactories);

    }

    //package private for testing
    AnalysisService(Index index, Settings indexSettings, @Nullable IndicesAnalysisService indicesAnalysisService,
                           @Nullable Map<String, AnalyzerProviderFactory> analyzerFactoryFactories,
                           @Nullable Map<String, TokenizerFactoryFactory> tokenizerFactoryFactories,
                           @Nullable Map<String, CharFilterFactoryFactory> charFilterFactoryFactories,
                           @Nullable Map<String, TokenFilterFactoryFactory> tokenFilterFactoryFactories) {
        super(index, indexSettings);
        Settings defaultSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.indexCreated(indexSettings)).build();
        Map<String, TokenizerFactory> tokenizers = newHashMap();
        if (tokenizerFactoryFactories != null) {
            Map<String, Settings> tokenizersSettings = indexSettings.getGroups("index.analysis.tokenizer");
            for (Map.Entry<String, TokenizerFactoryFactory> entry : tokenizerFactoryFactories.entrySet()) {
                String tokenizerName = entry.getKey();
                TokenizerFactoryFactory tokenizerFactoryFactory = entry.getValue();

                Settings tokenizerSettings = tokenizersSettings.get(tokenizerName);
                if (tokenizerSettings == null) {
                    tokenizerSettings = defaultSettings;
                }

                TokenizerFactory tokenizerFactory = tokenizerFactoryFactory.create(tokenizerName, tokenizerSettings);
                tokenizers.put(tokenizerName, tokenizerFactory);
            }
        }

        if (indicesAnalysisService != null) {
            for (Map.Entry<String, PreBuiltTokenizerFactoryFactory> entry : indicesAnalysisService.tokenizerFactories().entrySet()) {
                String name = entry.getKey();
                if (!tokenizers.containsKey(name)) {
                    tokenizers.put(name, entry.getValue().create(name, defaultSettings));
                }
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
                    charFilterSettings = defaultSettings;
                }

                CharFilterFactory tokenFilterFactory = charFilterFactoryFactory.create(charFilterName, charFilterSettings);
                charFilters.put(charFilterName, tokenFilterFactory);
            }
        }

        if (indicesAnalysisService != null) {
            for (Map.Entry<String, PreBuiltCharFilterFactoryFactory> entry : indicesAnalysisService.charFilterFactories().entrySet()) {
                String name = entry.getKey();
                if (!charFilters.containsKey(name)) {
                    charFilters.put(name, entry.getValue().create(name, defaultSettings));
                }
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
                    tokenFilterSettings = defaultSettings;
                }

                TokenFilterFactory tokenFilterFactory = tokenFilterFactoryFactory.create(tokenFilterName, tokenFilterSettings);
                tokenFilters.put(tokenFilterName, tokenFilterFactory);
            }
        }

        // pre initialize the globally registered ones into the map
        if (indicesAnalysisService != null) {
            for (Map.Entry<String, PreBuiltTokenFilterFactoryFactory> entry : indicesAnalysisService.tokenFilterFactories().entrySet()) {
                String name = entry.getKey();
                if (!tokenFilters.containsKey(name)) {
                    tokenFilters.put(name, entry.getValue().create(name, defaultSettings));
                }
            }
        }
        this.tokenFilters = ImmutableMap.copyOf(tokenFilters);

        Map<String, AnalyzerProvider> analyzerProviders = newHashMap();
        if (analyzerFactoryFactories != null) {
            Map<String, Settings> analyzersSettings = indexSettings.getGroups("index.analysis.analyzer");
            for (Map.Entry<String, AnalyzerProviderFactory> entry : analyzerFactoryFactories.entrySet()) {
                String analyzerName = entry.getKey();
                AnalyzerProviderFactory analyzerFactoryFactory = entry.getValue();

                Settings analyzerSettings = analyzersSettings.get(analyzerName);
                if (analyzerSettings == null) {
                    analyzerSettings = defaultSettings;
                }

                AnalyzerProvider analyzerFactory = analyzerFactoryFactory.create(analyzerName, analyzerSettings);
                analyzerProviders.put(analyzerName, analyzerFactory);
            }
        }
        if (indicesAnalysisService != null) {
            for (Map.Entry<String, PreBuiltAnalyzerProviderFactory> entry : indicesAnalysisService.analyzerProviderFactories().entrySet()) {
                String name = entry.getKey();
                Version indexVersion = Version.indexCreated(indexSettings);
                if (!analyzerProviders.containsKey(name)) {
                    analyzerProviders.put(name, entry.getValue().create(name, Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, indexVersion).build()));
                }
            }
        }

        if (!analyzerProviders.containsKey("default")) {
            analyzerProviders.put("default", new StandardAnalyzerProvider(index, indexSettings, null, "default", Settings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("default_index")) {
            analyzerProviders.put("default_index", analyzerProviders.get("default"));
        }
        if (!analyzerProviders.containsKey("default_search")) {
            analyzerProviders.put("default_search", analyzerProviders.get("default"));
        }
        if (!analyzerProviders.containsKey("default_search_quoted")) {
            analyzerProviders.put("default_search_quoted", analyzerProviders.get("default_search"));
        }

        Map<String, NamedAnalyzer> analyzers = newHashMap();
        for (AnalyzerProvider analyzerFactory : analyzerProviders.values()) {
            /*
             * Lucene defaults positionIncrementGap to 0 in all analyzers but
             * Elasticsearch defaults them to 0 only before version 2.0
             * and 100 afterwards so we override the positionIncrementGap if it
             * doesn't match here.
             */
            int overridePositionIncrementGap = StringFieldMapper.Defaults.positionIncrementGap(Version.indexCreated(indexSettings));
            if (analyzerFactory instanceof CustomAnalyzerProvider) {
                ((CustomAnalyzerProvider) analyzerFactory).build(this);
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
                analyzer = new NamedAnalyzer(analyzerFactory.name(), analyzerFactory.scope(), analyzerF, overridePositionIncrementGap);
            }
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

        NamedAnalyzer defaultAnalyzer = analyzers.get("default");
        if (defaultAnalyzer == null) {
            throw new IllegalArgumentException("no default analyzer configured");
        }
        if (analyzers.containsKey("default_index")) {
            deprecationLogger.deprecated("setting [index.analysis.analyzer.default_index] is deprecated, use [index.analysis.analyzer.default] instead for index [{}]", index.getName());
        }
        defaultIndexAnalyzer = analyzers.containsKey("default_index") ? analyzers.get("default_index") : defaultAnalyzer;
        defaultSearchAnalyzer = analyzers.containsKey("default_search") ? analyzers.get("default_search") : defaultAnalyzer;
        defaultSearchQuoteAnalyzer = analyzers.containsKey("default_search_quote") ? analyzers.get("default_search_quote") : defaultSearchAnalyzer;

        for (Map.Entry<String, NamedAnalyzer> analyzer : analyzers.entrySet()) {
            if (analyzer.getKey().startsWith("_")) {
                throw new IllegalArgumentException("analyzer name must not start with '_'. got \"" + analyzer.getKey() + "\"");
            }
        }
        this.analyzers = ImmutableMap.copyOf(analyzers);
    }

    @Override
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
        NamedAnalyzer analyzer = analyzers.get(name);
        if (analyzer != null) {
            return analyzer;
        }
        String underscoreName = Strings.toUnderscoreCase(name);
        analyzer = analyzers.get(underscoreName);
        if (analyzer != null) {
            DEPRECATION_LOGGER.deprecated("Deprecated analyzer name [" + name + "], use [" + underscoreName + "] instead");
        }
        return analyzer;
    }

    public NamedAnalyzer defaultIndexAnalyzer() {
        return defaultIndexAnalyzer;
    }

    public NamedAnalyzer defaultSearchAnalyzer() {
        return defaultSearchAnalyzer;
    }

    public NamedAnalyzer defaultSearchQuoteAnalyzer() {
        return defaultSearchQuoteAnalyzer;
    }

    public TokenizerFactory tokenizer(String name) {
        TokenizerFactory tokenizerFactory = tokenizers.get(name);
        if (tokenizerFactory != null) {
            return tokenizerFactory;
        }
        String underscoreName = Strings.toUnderscoreCase(name);
        tokenizerFactory = tokenizers.get(underscoreName);
        if (tokenizerFactory != null) {
            DEPRECATION_LOGGER.deprecated("Deprecated tokenizer name [" + name + "], use [" + underscoreName + "] instead");
        }
        return tokenizerFactory;
    }

    public CharFilterFactory charFilter(String name) {
        CharFilterFactory charFilterFactory = charFilters.get(name);
        if (charFilterFactory != null) {
            return charFilterFactory;
        }
        String underscoreName = Strings.toUnderscoreCase(name);
        charFilterFactory = charFilters.get(underscoreName);
        if (charFilterFactory != null) {
            DEPRECATION_LOGGER.deprecated("Deprecated char_filter name [" + name + "], use [" + underscoreName + "] instead");
        }
        return charFilterFactory;
    }

    public TokenFilterFactory tokenFilter(String name) {
        TokenFilterFactory tokenFilterFactory = tokenFilters.get(name);
        if (tokenFilterFactory != null) {
            return tokenFilterFactory;
        }
        String underscoreName = Strings.toUnderscoreCase(name);
        tokenFilterFactory = tokenFilters.get(underscoreName);
        if (tokenFilterFactory != null) {
            DEPRECATION_LOGGER.deprecated("Deprecated token_filter name [" + name + "], use [" + underscoreName + "] instead");
        }
        return tokenFilterFactory;
    }
}
