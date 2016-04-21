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
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.core.TextFieldMapper;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 *
 */
public class AnalysisService extends AbstractIndexComponent implements Closeable {

    private final Map<String, NamedAnalyzer> analyzers;
    private final Map<String, TokenizerFactory> tokenizers;
    private final Map<String, CharFilterFactory> charFilters;
    private final Map<String, TokenFilterFactory> tokenFilters;

    private final NamedAnalyzer defaultIndexAnalyzer;
    private final NamedAnalyzer defaultSearchAnalyzer;
    private final NamedAnalyzer defaultSearchQuoteAnalyzer;

    public AnalysisService(IndexSettings indexSettings,
                           Map<String, AnalyzerProvider> analyzerProviders,
                           Map<String, TokenizerFactory> tokenizerFactoryFactories,
                           Map<String, CharFilterFactory> charFilterFactoryFactories,
                           Map<String, TokenFilterFactory> tokenFilterFactoryFactories) {
        super(indexSettings);
        this.tokenizers = unmodifiableMap(tokenizerFactoryFactories);
        this.charFilters = unmodifiableMap(charFilterFactoryFactories);
        this.tokenFilters = unmodifiableMap(tokenFilterFactoryFactories);
        analyzerProviders = new HashMap<>(analyzerProviders);

        if (!analyzerProviders.containsKey("default")) {
            analyzerProviders.put("default", new StandardAnalyzerProvider(indexSettings, null, "default", Settings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerProviders.containsKey("default_search")) {
            analyzerProviders.put("default_search", analyzerProviders.get("default"));
        }
        if (!analyzerProviders.containsKey("default_search_quoted")) {
            analyzerProviders.put("default_search_quoted", analyzerProviders.get("default_search"));
        }

        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        for (Map.Entry<String, AnalyzerProvider> entry : analyzerProviders.entrySet()) {
            AnalyzerProvider analyzerFactory = entry.getValue();
            String name = entry.getKey();
            /*
             * Lucene defaults positionIncrementGap to 0 in all analyzers but
             * Elasticsearch defaults them to 0 only before version 2.0
             * and 100 afterwards so we override the positionIncrementGap if it
             * doesn't match here.
             */
            int overridePositionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
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
                analyzer = new NamedAnalyzer(name, analyzerFactory.scope(), analyzerF, overridePositionIncrementGap);
            }
            if (analyzers.containsKey(name)) {
                throw new IllegalStateException("already registered analyzer with name: " + name);
            }
            analyzers.put(name, analyzer);
            String strAliases = this.indexSettings.getSettings().get("index.analysis.analyzer." + analyzerFactory.name() + ".alias");
            if (strAliases != null) {
                for (String alias : Strings.commaDelimitedListToStringArray(strAliases)) {
                    analyzers.put(alias, analyzer);
                }
            }
            String[] aliases = this.indexSettings.getSettings().getAsArray("index.analysis.analyzer." + analyzerFactory.name() + ".alias");
            for (String alias : aliases) {
                analyzers.put(alias, analyzer);
            }
        }

        NamedAnalyzer defaultAnalyzer = analyzers.get("default");
        if (defaultAnalyzer == null) {
            throw new IllegalArgumentException("no default analyzer configured");
        }
        if (analyzers.containsKey("default_index")) {
            final Version createdVersion = indexSettings.getIndexVersionCreated();
            if (createdVersion.onOrAfter(Version.V_5_0_0_alpha1)) {
                throw new IllegalArgumentException("setting [index.analysis.analyzer.default_index] is not supported anymore, use [index.analysis.analyzer.default] instead for index [" + index().getName() + "]");
            } else {
                deprecationLogger.deprecated("setting [index.analysis.analyzer.default_index] is deprecated, use [index.analysis.analyzer.default] instead for index [{}]", index().getName());
            }
        }
        defaultIndexAnalyzer = analyzers.containsKey("default_index") ? analyzers.get("default_index") : defaultAnalyzer;
        defaultSearchAnalyzer = analyzers.containsKey("default_search") ? analyzers.get("default_search") : defaultAnalyzer;
        defaultSearchQuoteAnalyzer = analyzers.containsKey("default_search_quote") ? analyzers.get("default_search_quote") : defaultSearchAnalyzer;

        for (Map.Entry<String, NamedAnalyzer> analyzer : analyzers.entrySet()) {
            if (analyzer.getKey().startsWith("_")) {
                throw new IllegalArgumentException("analyzer name must not start with '_'. got \"" + analyzer.getKey() + "\"");
            }
        }
        this.analyzers = unmodifiableMap(analyzers);
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
                    logger.debug("failed to close analyzer {}", analyzer);
                }
            }
        }
    }

    public NamedAnalyzer analyzer(String name) {
        return analyzers.get(name);
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
        return tokenizers.get(name);
    }

    public CharFilterFactory charFilter(String name) {
        return charFilters.get(name);
    }

    public TokenFilterFactory tokenFilter(String name) {
        return tokenFilters.get(name);
    }
}
