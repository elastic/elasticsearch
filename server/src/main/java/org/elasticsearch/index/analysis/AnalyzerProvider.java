/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.injection.guice.Provider;

/**
 * Provides instances of Lucene {@link Analyzer} implementations for index analysis.
 * This interface defines the contract for creating and managing analyzers used in text analysis operations.
 *
 * @param <T> the specific type of Analyzer this provider creates
 */
public interface AnalyzerProvider<T extends Analyzer> extends Provider<T> {

    /**
     * Retrieves the name of this analyzer provider.
     *
     * @return the analyzer name
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnalyzerProvider<StandardAnalyzer> provider = ...;
     * String name = provider.name(); // e.g., "standard"
     * }</pre>
     */
    String name();

    /**
     * Retrieves the scope of this analyzer, indicating whether it is index-specific or global.
     *
     * @return the {@link AnalyzerScope} defining the analyzer's visibility
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnalyzerProvider<StandardAnalyzer> provider = ...;
     * AnalyzerScope scope = provider.scope();
     * }</pre>
     */
    AnalyzerScope scope();

    /**
     * Retrieves an instance of the analyzer.
     * This method may return a new instance or a cached instance depending on the implementation.
     *
     * @return the Analyzer instance
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnalyzerProvider<StandardAnalyzer> provider = ...;
     * StandardAnalyzer analyzer = provider.get();
     * TokenStream tokenStream = analyzer.tokenStream("field", "text to analyze");
     * }</pre>
     */
    @Override
    T get();
}
