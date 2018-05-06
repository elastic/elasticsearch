/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * The categorization analyzer.
 *
 * Converts messages to lists of tokens that will be fed to the ML categorization algorithm.
 */
public class CategorizationAnalyzer implements Closeable {

    private final Analyzer analyzer;
    private final boolean closeAnalyzer;

    public CategorizationAnalyzer(AnalysisRegistry analysisRegistry, Environment environment,
                                  CategorizationAnalyzerConfig categorizationAnalyzerConfig) throws IOException {

        Tuple<Analyzer, Boolean> tuple = categorizationAnalyzerConfig.toAnalyzer(analysisRegistry, environment);
        analyzer = tuple.v1();
        closeAnalyzer = tuple.v2();
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
}
