/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import com.ibm.icu.text.Normalizer2;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;

import java.io.Reader;

/**
 * Provides an ICU-based analyzer that combines tokenization, normalization, and folding.
 * This analyzer uses the ICU tokenizer for language-aware segmentation and applies
 * normalization and folding for case-insensitive matching.
 */
public class IcuAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final Normalizer2 normalizer;

    /**
     * Constructs an ICU analyzer provider with configurable normalization.
     *
     * @param indexSettings the index settings
     * @param environment the environment
     * @param name the analyzer name
     * @param settings the analyzer settings containing:
     *        <ul>
     *        <li>method: normalization method (default: "nfkc_cf")</li>
     *        <li>mode: normalization mode - "compose" or "decompose" (default: "compose")</li>
     *        </ul>
     * @throws IllegalArgumentException if mode is not "compose" or "decompose"
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "analyzer": {
     *   "my_icu_analyzer": {
     *     "type": "icu_analyzer",
     *     "method": "nfkc_cf",
     *     "mode": "compose"
     *   }
     * }
     * }</pre>
     */
    public IcuAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        String method = settings.get("method", "nfkc_cf");
        String mode = settings.get("mode", "compose");
        if ("compose".equals(mode) == false && "decompose".equals(mode) == false) {
            throw new IllegalArgumentException(
                "Unknown mode [" + mode + "] in analyzer [" + name + "], expected one of [compose, decompose]"
            );
        }
        Normalizer2 normalizerInstance = Normalizer2.getInstance(
            null,
            method,
            "compose".equals(mode) ? Normalizer2.Mode.COMPOSE : Normalizer2.Mode.DECOMPOSE
        );
        this.normalizer = IcuNormalizerTokenFilterFactory.wrapWithUnicodeSetFilter(normalizerInstance, settings);
    }

    /**
     * Creates and returns the configured ICU analyzer instance.
     * The analyzer performs ICU normalization on the input, then tokenizes using the ICU tokenizer,
     * and finally applies ICU folding for case-insensitive matching.
     *
     * @return a new {@link Analyzer} instance configured with ICU components
     */
    @Override
    public Analyzer get() {
        return new Analyzer() {

            @Override
            protected Reader initReader(String fieldName, Reader reader) {
                return new ICUNormalizer2CharFilter(reader, normalizer);
            }

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new ICUTokenizer();
                return new TokenStreamComponents(source, new ICUFoldingFilter(source));
            }
        };
    }
}
