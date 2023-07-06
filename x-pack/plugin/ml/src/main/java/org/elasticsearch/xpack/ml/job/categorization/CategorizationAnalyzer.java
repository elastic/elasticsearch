/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The categorization analyzer.
 *
 * Converts messages to lists of tokens that will be fed to the ML categorization algorithm.
 *
 */
public class CategorizationAnalyzer implements Releasable {

    private final Analyzer analyzer;
    private final boolean closeAnalyzer;

    public CategorizationAnalyzer(AnalysisRegistry analysisRegistry, CategorizationAnalyzerConfig categorizationAnalyzerConfig)
        throws IOException {

        Tuple<Analyzer, Boolean> tuple = makeAnalyzer(categorizationAnalyzerConfig, analysisRegistry);
        analyzer = tuple.v1();
        closeAnalyzer = tuple.v2();
    }

    public CategorizationAnalyzer(Analyzer analyzer, boolean closeAnalyzer) {
        this.analyzer = analyzer;
        this.closeAnalyzer = closeAnalyzer;
    }

    public final TokenStream tokenStream(final String fieldName, final String text) {
        return analyzer.tokenStream(fieldName, text);
    }

    /**
     * Release resources held by the analyzer (unless it's global).
     */
    @Override
    public void close() {
        if (closeAnalyzer) {
            analyzer.close();
        }
    }

    /**
     * Given a field value, convert it to a list of tokens using the configured analyzer.
     */
    public List<String> tokenizeField(String fieldName, String fieldValue) {
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(fieldName, fieldValue)) {
            stream.reset();
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            while (stream.incrementToken()) {
                String token = term.toString();
                // Ignore empty tokens for categorization
                if (token.isEmpty() == false) {
                    tokens.add(term.toString());
                }
            }
            stream.end();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to analyze value [" + fieldValue + "] of field [" + fieldName + "]", e);
        }
        return tokens;
    }

    /**
     * Verify that the config builder will build a valid config.  This is not done as part of the basic build
     * because it verifies that the names of analyzers/tokenizers/filters referenced by the config are
     * known, and the validity of these names could change over time.  Additionally, it has to be done
     * server-side rather than client-side, as the client will not have loaded the appropriate analysis
     * modules/plugins.
     */
    public static void verifyConfigBuilder(CategorizationAnalyzerConfig.Builder configBuilder, AnalysisRegistry analysisRegistry)
        throws IOException {
        Tuple<Analyzer, Boolean> tuple = makeAnalyzer(configBuilder.build(), analysisRegistry);
        if (tuple.v2()) {
            tuple.v1().close();
        }
    }

    /**
     * Convert a config to an {@link Analyzer}.  This may be a global analyzer or a newly created custom analyzer.
     * In the case of a global analyzer the caller must NOT close it when they have finished with it.  In the case of
     * a newly created custom analyzer the caller is responsible for closing it.
     * @return The first tuple member is the {@link Analyzer}; the second indicates whether the caller is responsible
     *         for closing it.
     */
    private static Tuple<Analyzer, Boolean> makeAnalyzer(CategorizationAnalyzerConfig config, AnalysisRegistry analysisRegistry)
        throws IOException {
        String analyzer = config.getAnalyzer();
        if (analyzer != null) {
            Analyzer globalAnalyzer = analysisRegistry.getAnalyzer(analyzer);
            if (globalAnalyzer == null) {
                throw new IllegalArgumentException("Failed to find global analyzer [" + analyzer + "]");
            }
            return new Tuple<>(globalAnalyzer, Boolean.FALSE);
        } else {
            return new Tuple<>(
                analysisRegistry.buildCustomAnalyzer(IndexCreationContext.RELOAD_ANALYZERS,
                        null, false, config.getTokenizer(), config.getCharFilters(), config.getTokenFilters()),
                Boolean.TRUE
            );
        }
    }

}
