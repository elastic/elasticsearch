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

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.Version;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.analysis.*;

import java.io.Closeable;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 * A node level registry of analyzers, to be reused by different indices which use default analyzers.
 */
public class IndicesAnalysisService extends AbstractComponent implements Closeable {

    private final Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PreBuiltTokenizerFactoryFactory> tokenizerFactories = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PreBuiltTokenFilterFactoryFactory> tokenFilterFactories = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PreBuiltCharFilterFactoryFactory> charFilterFactories = ConcurrentCollections.newConcurrentMap();

    public IndicesAnalysisService() {
        super(EMPTY_SETTINGS);
    }

    @Inject
    public IndicesAnalysisService(Settings settings) {
        super(settings);

        // Analyzers
        for (PreBuiltAnalyzers preBuiltAnalyzerEnum : PreBuiltAnalyzers.values()) {
            String name = preBuiltAnalyzerEnum.name().toLowerCase(Locale.ROOT);
            analyzerProviderFactories.put(name, new PreBuiltAnalyzerProviderFactory(name, AnalyzerScope.INDICES, preBuiltAnalyzerEnum.getAnalyzer(Version.CURRENT)));
        }

        // Tokenizers
        for (PreBuiltTokenizers preBuiltTokenizer : PreBuiltTokenizers.values()) {
            String name = preBuiltTokenizer.name().toLowerCase(Locale.ROOT);
            tokenizerFactories.put(name, new PreBuiltTokenizerFactoryFactory(preBuiltTokenizer.getTokenizerFactory(Version.CURRENT)));
        }

        // Tokenizer aliases
        tokenizerFactories.put("nGram", new PreBuiltTokenizerFactoryFactory(PreBuiltTokenizers.NGRAM.getTokenizerFactory(Version.CURRENT)));
        tokenizerFactories.put("edgeNGram", new PreBuiltTokenizerFactoryFactory(PreBuiltTokenizers.EDGE_NGRAM.getTokenizerFactory(Version.CURRENT)));


        // Token filters
        for (PreBuiltTokenFilters preBuiltTokenFilter : PreBuiltTokenFilters.values()) {
            String name = preBuiltTokenFilter.name().toLowerCase(Locale.ROOT);
            tokenFilterFactories.put(name, new PreBuiltTokenFilterFactoryFactory(preBuiltTokenFilter.getTokenFilterFactory(Version.CURRENT)));
        }
        // Token filter aliases
        tokenFilterFactories.put("nGram", new PreBuiltTokenFilterFactoryFactory(PreBuiltTokenFilters.NGRAM.getTokenFilterFactory(Version.CURRENT)));
        tokenFilterFactories.put("edgeNGram", new PreBuiltTokenFilterFactoryFactory(PreBuiltTokenFilters.EDGE_NGRAM.getTokenFilterFactory(Version.CURRENT)));


        // Char Filters
        for (PreBuiltCharFilters preBuiltCharFilter : PreBuiltCharFilters.values()) {
            String name = preBuiltCharFilter.name().toLowerCase(Locale.ROOT);
            charFilterFactories.put(name, new PreBuiltCharFilterFactoryFactory(preBuiltCharFilter.getCharFilterFactory(Version.CURRENT)));
        }
        // Char filter aliases
        charFilterFactories.put("htmlStrip", new PreBuiltCharFilterFactoryFactory(PreBuiltCharFilters.HTML_STRIP.getCharFilterFactory(Version.CURRENT)));
    }

    public boolean hasCharFilter(String name) {
        return charFilterFactoryFactory(name) != null;
    }

    public Map<String, PreBuiltCharFilterFactoryFactory> charFilterFactories() {
        return charFilterFactories;
    }

    public CharFilterFactoryFactory charFilterFactoryFactory(String name) {
        return charFilterFactories.get(name);
    }

    public boolean hasTokenFilter(String name) {
        return tokenFilterFactoryFactory(name) != null;
    }

    public Map<String, PreBuiltTokenFilterFactoryFactory> tokenFilterFactories() {
        return tokenFilterFactories;
    }

    public TokenFilterFactoryFactory tokenFilterFactoryFactory(String name) {
        return tokenFilterFactories.get(name);
    }

    public boolean hasTokenizer(String name) {
        return tokenizerFactoryFactory(name) != null;
    }

    public Map<String, PreBuiltTokenizerFactoryFactory> tokenizerFactories() {
        return tokenizerFactories;
    }

    public TokenizerFactoryFactory tokenizerFactoryFactory(String name) {
        return tokenizerFactories.get(name);
    }

    public Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories() {
        return analyzerProviderFactories;
    }

    public PreBuiltAnalyzerProviderFactory analyzerProviderFactory(String name) {
        return analyzerProviderFactories.get(name);
    }

    public boolean hasAnalyzer(String name) {
        return analyzerProviderFactories.containsKey(name);
    }

    public Analyzer analyzer(String name) {
        PreBuiltAnalyzerProviderFactory analyzerProviderFactory = analyzerProviderFactory(name);
        if (analyzerProviderFactory == null) {
            return null;
        }
        return analyzerProviderFactory.analyzer();
    }

    @Override
    public void close() {
        for (PreBuiltAnalyzerProviderFactory analyzerProviderFactory : analyzerProviderFactories.values()) {
            try {
                analyzerProviderFactory.analyzer().close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
