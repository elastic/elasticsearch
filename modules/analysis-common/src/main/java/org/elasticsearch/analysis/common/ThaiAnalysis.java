/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.ThaiLucene105StopWords;

/** Defines the two behaviorally distinct generations of Thai analysis and the preconfigured components that select between them. */
final class ThaiAnalysis {

    enum Generation {
        /** Lucene 10.5 behavior, kept for indices created before Lucene 10.6. */
        LUCENE_10_5,
        /** Whatever Lucene currently ships; not pinned to a Lucene version. */
        CURRENT;

        Analyzer newAnalyzer() {
            return switch (this) {
                case LUCENE_10_5 -> new ThaiLucene105Analyzer();
                case CURRENT -> new ThaiAnalyzer();
            };
        }

        Analyzer newAnalyzer(CharArraySet stopWords) {
            return switch (this) {
                case LUCENE_10_5 -> new ThaiLucene105Analyzer(stopWords);
                case CURRENT -> new ThaiAnalyzer(stopWords);
            };
        }

        Tokenizer newTokenizer() {
            return switch (this) {
                case LUCENE_10_5 -> new ThaiLucene105Tokenizer();
                case CURRENT -> new ThaiTokenizer();
            };
        }

        CharArraySet defaultStopSet() {
            return switch (this) {
                case LUCENE_10_5 -> ThaiLucene105Analyzer.getDefaultStopSet();
                case CURRENT -> ThaiAnalyzer.getDefaultStopSet();
            };
        }
    }

    static Generation generation(IndexVersion indexVersion) {
        return generation(indexVersion.luceneVersion());
    }

    static Generation generation(Version luceneVersion) {
        return ThaiLucene105StopWords.appliesTo(luceneVersion) ? Generation.LUCENE_10_5 : Generation.CURRENT;
    }

    static PreBuiltAnalyzerProviderFactory preBuiltAnalyzerFactory() {
        return PreBuiltAnalyzerProviderFactory.luceneVersion("thai", luceneVersion -> generation(luceneVersion).newAnalyzer());
    }

    static PreConfiguredTokenizer preConfiguredTokenizer() {
        return PreConfiguredTokenizer.luceneVersion("thai", luceneVersion -> generation(luceneVersion).newTokenizer());
    }

    private ThaiAnalysis() {}
}
