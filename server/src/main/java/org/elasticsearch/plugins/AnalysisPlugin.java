/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.index.analysis.PreConfiguredCharFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * An additional extension point for {@link Plugin}s that extends Elasticsearch's analysis functionality. To add an additional
 * {@link TokenFilter} just implement the interface and implement the {@link #getTokenFilters()} method:
 *
 * <pre>{@code
 * public class AnalysisPhoneticPlugin extends Plugin implements AnalysisPlugin {
 *     &#64;Override
 *     public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
 *         return singletonMap("phonetic", PhoneticTokenFilterFactory::new);
 *     }
 * }
 * }</pre>
 *
 * Elasticsearch doesn't have any automatic mechanism to share these components between indexes. If any component is heavy enough to warrant
 * such sharing then it is the Plugin's responsibility to do it in their {@link AnalysisProvider} implementation. We recommend against doing
 * this unless absolutely necessary because it can be difficult to get the caching right given things like behavior changes across versions.
 */
public interface AnalysisPlugin {
    /**
     * Override to add additional {@link CharFilter}s. See {@link #requiresAnalysisSettings(AnalysisProvider)}
     * how to on get the configuration from the index.
     */
    default Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link TokenFilter}s. See {@link #requiresAnalysisSettings(AnalysisProvider)}
     * how to on get the configuration from the index.
     */
    default Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link Tokenizer}s. See {@link #requiresAnalysisSettings(AnalysisProvider)}
     * how to on get the configuration from the index.
     */
    default Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link Analyzer}s. See {@link #requiresAnalysisSettings(AnalysisProvider)}
     * how to on get the configuration from the index.
     */
    default Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return emptyMap();
    }

    /**
     * Override to add additional pre-configured {@link Analyzer}s.
     */
    default List<PreBuiltAnalyzerProviderFactory> getPreBuiltAnalyzerProviderFactories() {
        return emptyList();
    }

    /**
     * Override to add additional pre-configured {@link CharFilter}s.
     */
    default List<PreConfiguredCharFilter> getPreConfiguredCharFilters() {
        return emptyList();
    }

    /**
     * Override to add additional pre-configured {@link TokenFilter}s.
     */
    default List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
        return emptyList();
    }

    /**
     * Override to add additional pre-configured {@link Tokenizer}.
     */
    default List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
        return emptyList();
    }

    /**
     * Override to add additional hunspell {@link org.apache.lucene.analysis.hunspell.Dictionary}s.
     */
    default Map<String, org.apache.lucene.analysis.hunspell.Dictionary> getHunspellDictionaries() {
        return emptyMap();
    }

    /**
     * Mark an {@link AnalysisProvider} as requiring the index's settings.
     */
    static <T> AnalysisProvider<T> requiresAnalysisSettings(AnalysisProvider<T> provider) {
        return new AnalysisProvider<T>() {
            @Override
            public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
                return provider.get(indexSettings, environment, name, settings);
            }

            @Override
            public boolean requiresAnalysisSettings() {
                return true;
            }
        };
    }
}
