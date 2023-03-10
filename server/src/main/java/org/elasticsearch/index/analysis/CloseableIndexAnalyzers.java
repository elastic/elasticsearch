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
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;

public final class CloseableIndexAnalyzers implements Closeable, IndexAnalyzers {

    private final Map<String, NamedAnalyzer> analyzers;
    private final Map<String, NamedAnalyzer> normalizers;
    private final Map<String, NamedAnalyzer> whitespaceNormalizers;

    public CloseableIndexAnalyzers(
        Map<String, NamedAnalyzer> analyzers,
        Map<String, NamedAnalyzer> normalizers,
        Map<String, NamedAnalyzer> whitespaceNormalizers
    ) {
        Objects.requireNonNull(analyzers.get(DEFAULT_ANALYZER_NAME), "the default analyzer must be set");
        if (analyzers.get(DEFAULT_ANALYZER_NAME).name().equals(DEFAULT_ANALYZER_NAME) == false) {
            throw new IllegalStateException(
                "default analyzer must have the name [default] but was: [" + analyzers.get(DEFAULT_ANALYZER_NAME).name() + "]"
            );
        }
        this.analyzers = unmodifiableMap(analyzers);
        this.normalizers = unmodifiableMap(normalizers);
        this.whitespaceNormalizers = unmodifiableMap(whitespaceNormalizers);
    }

    @Override
    public NamedAnalyzer getAnalyzer(AnalyzerType type, String name) {
        return switch (type) {
            case ANALYZER -> analyzers.get(name);
            case NORMALIZER -> normalizers.get(name);
            case WHITESPACE -> whitespaceNormalizers.get(name);
        };
    }

    /**
     * Returns an (unmodifiable) map of containing the index analyzers
     */
    public Map<String, NamedAnalyzer> getAnalyzers() {
        return analyzers;
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
    public List<String> reload(AnalysisRegistry registry, IndexSettings indexSettings) throws IOException {

        List<NamedAnalyzer> reloadableAnalyzers = analyzers.values()
            .stream()
            .filter(a -> a.analyzer() instanceof ReloadableCustomAnalyzer)
            .toList();
        if (reloadableAnalyzers.isEmpty()) {
            return List.of();
        }

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

        return reloadableAnalyzers.stream().map(NamedAnalyzer::name).toList();
    }
}
