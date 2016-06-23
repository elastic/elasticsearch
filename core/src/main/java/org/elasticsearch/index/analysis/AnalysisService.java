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
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.core.TextFieldMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;

/**
 *
 */
public class AnalysisService extends AbstractIndexComponent implements Closeable {

    private static NamedAnalyzer buildNamedAnalyzer(String name, AnalyzerProvider analyzerFactory,
            Analyzer analyzerF, int overridePositionIncrementGap) {
        if (analyzerF instanceof NamedAnalyzer) {
            // if we got a named analyzer back, use it...
            NamedAnalyzer analyzer = (NamedAnalyzer) analyzerF;
            if (overridePositionIncrementGap >= 0 && analyzer.getPositionIncrementGap(analyzer.name()) != overridePositionIncrementGap) {
                // unless the positionIncrementGap needs to be overridden
                analyzer = new NamedAnalyzer(analyzer, overridePositionIncrementGap);
            }
            return analyzer;
        } else {
            return new NamedAnalyzer(name, analyzerFactory.scope(), analyzerF, overridePositionIncrementGap);
        }
    }

    private final Map<String, NamedAnalyzer> analyzers;
    private final Map<String, NamedAnalyzer> multiTermAnalyzers;
    private final Map<String, TokenizerFactory> tokenizers;
    private final Map<String, CharFilterFactory> charFilters;
    private final Map<String, TokenFilterFactory> tokenFilters;

    private final NamedAnalyzer defaultIndexAnalyzer;
    private final NamedAnalyzer defaultSearchAnalyzer;
    private final NamedAnalyzer defaultSearchMultiTermAnalyzer;
    private final NamedAnalyzer defaultSearchQuoteAnalyzer;

    public AnalysisService(IndexSettings indexSettings,
                           Map<String, AnalyzerProvider> analyzerFactories,
                           Map<String, TokenizerFactory> tokenizerFactoryFactories,
                           Map<String, CharFilterFactory> charFilterFactoryFactories,
                           Map<String, TokenFilterFactory> tokenFilterFactoryFactories) {
        super(indexSettings);
        this.tokenizers = unmodifiableMap(tokenizerFactoryFactories);
        this.charFilters = unmodifiableMap(charFilterFactoryFactories);
        this.tokenFilters = unmodifiableMap(tokenFilterFactoryFactories);
        analyzerFactories = new HashMap<>(analyzerFactories);

        if (!analyzerFactories.containsKey("default")) {
            analyzerFactories.put("default", new StandardAnalyzerProvider(indexSettings, null, "default", Settings.Builder.EMPTY_SETTINGS));
        }
        if (!analyzerFactories.containsKey("default_search")) {
            analyzerFactories.put("default_search", analyzerFactories.get("default"));
        }
        if (!analyzerFactories.containsKey("default_search_quoted")) {
            analyzerFactories.put("default_search_quoted", analyzerFactories.get("default_search"));
        }

        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        Map<String, NamedAnalyzer> multiTermAnalyzers = new HashMap<>();
        for (Map.Entry<String, AnalyzerProvider> entry : analyzerFactories.entrySet()) {
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
            NamedAnalyzer analyzer = buildNamedAnalyzer(name, analyzerFactory, analyzerF, overridePositionIncrementGap);
            Analyzer multiTermAnalyzerF = analyzerFactory.getMultiTerm();
            NamedAnalyzer multiTermAnalyzer = buildNamedAnalyzer(name, analyzerFactory, multiTermAnalyzerF, overridePositionIncrementGap);
            if (analyzers.containsKey(name)) {
                throw new IllegalStateException("already registered analyzer with name: " + name);
            }
            analyzers.put(name, analyzer);
            multiTermAnalyzers.put(name, multiTermAnalyzer);
            String strAliases = this.indexSettings.getSettings().get("index.analysis.analyzer." + analyzerFactory.name() + ".alias");
            if (strAliases != null) {
                for (String alias : Strings.commaDelimitedListToStringArray(strAliases)) {
                    analyzers.put(alias, analyzer);
                    multiTermAnalyzers.put(alias, multiTermAnalyzer);
                }
            }
            String[] aliases = this.indexSettings.getSettings().getAsArray("index.analysis.analyzer." + analyzerFactory.name() + ".alias");
            for (String alias : aliases) {
                analyzers.put(alias, analyzer);
                multiTermAnalyzers.put(alias, multiTermAnalyzer);
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
        NamedAnalyzer defaultIndexAnalyzer = analyzers.get("default_index");
        if (defaultIndexAnalyzer == null) {
            defaultIndexAnalyzer = defaultAnalyzer;
        }
        NamedAnalyzer defaultSearchAnalyzer = analyzers.get("default_search");
        if (defaultSearchAnalyzer == null) {
            defaultSearchAnalyzer = defaultAnalyzer;
        }
        NamedAnalyzer defaultSearchQuoteAnalyzer = analyzers.get("default_search_quote");
        if (defaultSearchQuoteAnalyzer == null) {
            defaultSearchQuoteAnalyzer = defaultSearchAnalyzer;
        }
        NamedAnalyzer defaultSearchMultiTermAnalyzer = multiTermAnalyzers.get("default_search");
        if (defaultSearchMultiTermAnalyzer == null) {
            defaultSearchMultiTermAnalyzer = multiTermAnalyzers.get("default");
        }
        this.defaultIndexAnalyzer = defaultIndexAnalyzer;
        this.defaultSearchAnalyzer = defaultSearchAnalyzer;
        this.defaultSearchQuoteAnalyzer = defaultSearchQuoteAnalyzer;
        this.defaultSearchMultiTermAnalyzer = defaultSearchMultiTermAnalyzer;
        for (Map.Entry<String, NamedAnalyzer> analyzer : analyzers.entrySet()) {
            if (analyzer.getKey().startsWith("_")) {
                throw new IllegalArgumentException("analyzer name must not start with '_'. got \"" + analyzer.getKey() + "\"");
            }
        }
        assert analyzers.keySet().equals(multiTermAnalyzers.keySet());
        this.analyzers = unmodifiableMap(analyzers);
        this.multiTermAnalyzers = unmodifiableMap(multiTermAnalyzers);
    }

    @Override
    public void close() throws IOException {
        List<NamedAnalyzer> indexAnalyzers = Stream.concat(
                analyzers.values().stream(),
                multiTermAnalyzers.values().stream())
                .filter(analyzer -> analyzer.scope() == AnalyzerScope.INDEX)
                .collect(Collectors.toList());
        IOUtils.close(indexAnalyzers);
    }

    public NamedAnalyzer analyzer(String name) {
        return analyzers.get(name);
    }

    public NamedAnalyzer multiTermAnalyzer(String name) {
        return multiTermAnalyzers.get(name);
    }

    public NamedAnalyzer defaultIndexAnalyzer() {
        return defaultIndexAnalyzer;
    }

    public NamedAnalyzer defaultSearchAnalyzer() {
        return defaultSearchAnalyzer;
    }

    public NamedAnalyzer defaultSearchMultiTermAnalyzer() {
        return defaultSearchMultiTermAnalyzer;
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
