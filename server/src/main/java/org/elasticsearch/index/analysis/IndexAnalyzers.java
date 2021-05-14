/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.analysis;

import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME;

/**
 * IndexAnalyzers contains a name to analyzer mapping for a specific index.
 * This class only holds analyzers that are explicitly configured for an index and doesn't allow
 * access to individual tokenizers, char or token filter.
 *
 * @see AnalysisRegistry
 */
public final class IndexAnalyzers implements Closeable {
    private final Map<String, NamedAnalyzer> analyzers;
    private final Map<String, NamedAnalyzer> normalizers;
    private final Map<String, NamedAnalyzer> whitespaceNormalizers;

    public IndexAnalyzers(Map<String, NamedAnalyzer> analyzers, Map<String, NamedAnalyzer> normalizers,
            Map<String, NamedAnalyzer> whitespaceNormalizers) {
        Objects.requireNonNull(analyzers.get(DEFAULT_ANALYZER_NAME), "the default analyzer must be set");
        if (analyzers.get(DEFAULT_ANALYZER_NAME).name().equals(DEFAULT_ANALYZER_NAME) == false) {
            throw new IllegalStateException(
                    "default analyzer must have the name [default] but was: [" + analyzers.get(DEFAULT_ANALYZER_NAME).name() + "]");
        }
        this.analyzers = unmodifiableMap(analyzers);
        this.normalizers = unmodifiableMap(normalizers);
        this.whitespaceNormalizers = unmodifiableMap(whitespaceNormalizers);
    }

    /**
     * Returns an analyzer mapped to the given name or <code>null</code> if not present
     */
    public NamedAnalyzer get(String name) {
        return analyzers.get(name);
    }

    /**
     * Returns an (unmodifiable) map of containing the index analyzers
     */
    public Map<String, NamedAnalyzer> getAnalyzers() {
        return analyzers;
    }

    /**
     * Returns a normalizer mapped to the given name or <code>null</code> if not present
     */
    public NamedAnalyzer getNormalizer(String name) {
        return normalizers.get(name);
    }

    /**
     * Returns a normalizer that splits on whitespace mapped to the given name or <code>null</code> if not present
     */
    public NamedAnalyzer getWhitespaceNormalizer(String name) {
        return whitespaceNormalizers.get(name);
    }

    /**
     * Returns the default index analyzer for this index
     */
    public NamedAnalyzer getDefaultIndexAnalyzer() {
        return analyzers.get(DEFAULT_ANALYZER_NAME);
    }

    /**
     * Returns the default search analyzer for this index. If not set, this will return the default analyzer
     */
    public NamedAnalyzer getDefaultSearchAnalyzer() {
        return analyzers.getOrDefault(DEFAULT_SEARCH_ANALYZER_NAME, getDefaultIndexAnalyzer());
    }

    /**
     * Returns the default search quote analyzer for this index
     */
    public NamedAnalyzer getDefaultSearchQuoteAnalyzer() {
        return analyzers.getOrDefault(DEFAULT_SEARCH_QUOTED_ANALYZER_NAME, getDefaultSearchAnalyzer());
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(Stream.of(analyzers.values().stream(), normalizers.values().stream(), whitespaceNormalizers.values().stream())
            .flatMap(s -> s)
            .filter(a -> a.scope() == AnalyzerScope.INDEX)
            .collect(Collectors.toList()));
    }
}
