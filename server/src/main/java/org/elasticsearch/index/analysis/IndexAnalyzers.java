/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME;

/**
 * IndexAnalyzers contains a name to analyzer mapping for a specific index.
 * This class only holds analyzers that are explicitly configured for an index and doesn't allow
 * access to individual tokenizers, char or token filter.
 *
 * @see AnalysisRegistry
 */
public interface IndexAnalyzers extends Closeable {

    enum AnalyzerType {
        ANALYZER,
        NORMALIZER,
        WHITESPACE
    }

    /**
     * Returns an analyzer of the given type mapped to the given name, or {@code null} if
     * no such analyzer exists.
     */
    NamedAnalyzer getAnalyzer(AnalyzerType type, String name);

    /**
     * Returns an analyzer mapped to the given name or {@code null} if not present
     */
    default NamedAnalyzer get(String name) {
        return getAnalyzer(AnalyzerType.ANALYZER, name);
    }

    /**
     * Returns a normalizer mapped to the given name or {@code null} if not present
     */
    default NamedAnalyzer getNormalizer(String name) {
        return getAnalyzer(AnalyzerType.NORMALIZER, name);
    }

    /**
     * Returns a normalizer that splits on whitespace mapped to the given name or {@code null} if not present
     */
    default NamedAnalyzer getWhitespaceNormalizer(String name) {
        return getAnalyzer(AnalyzerType.WHITESPACE, name);
    }

    /**
     * Returns the default index analyzer for this index
     */
    default NamedAnalyzer getDefaultIndexAnalyzer() {
        return getAnalyzer(AnalyzerType.ANALYZER, DEFAULT_ANALYZER_NAME);
    }

    /**
     * Returns the default search analyzer for this index. If not set, this will return the default analyzer
     */
    default NamedAnalyzer getDefaultSearchAnalyzer() {
        NamedAnalyzer analyzer = getAnalyzer(AnalyzerType.ANALYZER, DEFAULT_SEARCH_ANALYZER_NAME);
        if (analyzer != null) {
            return analyzer;
        }
        return getDefaultIndexAnalyzer();
    }

    /**
     * Returns the default search quote analyzer for this index. If not set, this will return the default
     * search analyzer
     */
    default NamedAnalyzer getDefaultSearchQuoteAnalyzer() {
        NamedAnalyzer analyzer = getAnalyzer(AnalyzerType.ANALYZER, AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME);
        if (analyzer != null) {
            return analyzer;
        }
        return getDefaultSearchAnalyzer();
    }

    /**
     * Reload any analyzers that have reloadable components
     */
    default List<String> reload(AnalysisRegistry analysisRegistry, IndexSettings indexSettings, String resource, boolean preview)
        throws IOException {
        return List.of();
    }

    default void close() throws IOException {}

    static IndexAnalyzers of(Map<String, NamedAnalyzer> analyzers) {
        return of(analyzers, Map.of(), Map.of());
    }

    static IndexAnalyzers of(Map<String, NamedAnalyzer> analyzers, Map<String, NamedAnalyzer> tokenizers) {
        return of(analyzers, tokenizers, Map.of());
    }

    static IndexAnalyzers of(
        Map<String, NamedAnalyzer> analyzers,
        Map<String, NamedAnalyzer> normalizers,
        Map<String, NamedAnalyzer> whitespaceNormalizers
    ) {
        return new IndexAnalyzers() {
            @Override
            public NamedAnalyzer getAnalyzer(AnalyzerType type, String name) {
                return switch (type) {
                    case ANALYZER -> analyzers.get(name);
                    case NORMALIZER -> normalizers.get(name);
                    case WHITESPACE -> whitespaceNormalizers.get(name);
                };
            }

            @Override
            public void close() throws IOException {
                IOUtils.close(
                    Stream.of(analyzers.values().stream(), normalizers.values().stream(), whitespaceNormalizers.values().stream())
                        .flatMap(s -> s)
                        .filter(a -> a.scope() == AnalyzerScope.INDEX)
                        .toList()
                );
            }

            @Override
            public List<String> reload(AnalysisRegistry registry, IndexSettings indexSettings, String resource, boolean preview)
                throws IOException {

                List<NamedAnalyzer> reloadableAnalyzers = analyzers.values()
                    .stream()
                    .filter(a -> a.analyzer() instanceof ReloadableCustomAnalyzer ra && ra.usesResource(resource))
                    .toList();

                if (reloadableAnalyzers.isEmpty()) {
                    return List.of();
                }

                if (preview == false) {
                    final Map<String, TokenizerFactory> tokenizerFactories = registry.buildTokenizerFactories(indexSettings);
                    final Map<String, CharFilterFactory> charFilterFactories = registry.buildCharFilterFactories(indexSettings);
                    final Map<String, TokenFilterFactory> tokenFilterFactories = registry.buildTokenFilterFactories(indexSettings);
                    final Map<String, Settings> settings = indexSettings.getSettings().getGroups("index.analysis.analyzer");

                    for (NamedAnalyzer analyzer : reloadableAnalyzers) {
                        String name = analyzer.name();
                        Settings analyzerSettings = settings.get(name);
                        ReloadableCustomAnalyzer reloadableAnalyzer = (ReloadableCustomAnalyzer) analyzer.analyzer();
                        reloadableAnalyzer.reload(name, analyzerSettings, tokenizerFactories, charFilterFactories, tokenFilterFactories);
                    }
                }

                return reloadableAnalyzers.stream().map(NamedAnalyzer::name).toList();
            }
        };
    }

}
