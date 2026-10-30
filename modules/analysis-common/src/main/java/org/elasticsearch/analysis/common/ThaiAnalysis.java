/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.th.ThaiTokenizer;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;

/** Defines the two behaviorally distinct generations of Thai analysis and the preconfigured components cached by generation. */
final class ThaiAnalysis {

    enum Generation {
        LEGACY,
        CURRENT;

        Analyzer newAnalyzer() {
            return switch (this) {
                case LEGACY -> new LegacyThaiAnalyzer();
                case CURRENT -> new ThaiAnalyzer();
            };
        }

        Analyzer newAnalyzer(CharArraySet stopWords) {
            return switch (this) {
                case LEGACY -> new LegacyThaiAnalyzer(stopWords);
                case CURRENT -> new ThaiAnalyzer(stopWords);
            };
        }

        Tokenizer newTokenizer() {
            return switch (this) {
                case LEGACY -> new LegacyThaiTokenizer();
                case CURRENT -> new ThaiTokenizer();
            };
        }

        CharArraySet defaultStopSet() {
            return switch (this) {
                case LEGACY -> LegacyThaiAnalyzer.getDefaultStopSet();
                case CURRENT -> ThaiAnalyzer.getDefaultStopSet();
            };
        }
    }

    static Generation generation(Version luceneVersion) {
        return luceneVersion.onOrAfter(Version.LUCENE_10_6_0) ? Generation.CURRENT : Generation.LEGACY;
    }

    static PreBuiltAnalyzerProviderFactory preBuiltAnalyzerFactory() {
        return PreBuiltAnalyzerProviderFactory.versioned(
            "thai",
            indexVersion -> generation(indexVersion.luceneVersion()),
            Generation::newAnalyzer
        );
    }

    static PreConfiguredTokenizer preConfiguredTokenizer() {
        return PreConfiguredTokenizer.versioned("thai", indexVersion -> generation(indexVersion.luceneVersion()), Generation::newTokenizer);
    }

    private ThaiAnalysis() {}
}
