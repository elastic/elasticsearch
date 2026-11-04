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
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.analysis.LegacyThaiStopWords;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.checkRandomData;

/**
 * {@link LegacyThaiAnalyzer} must reproduce Lucene 10.5's {@code ThaiAnalyzer}: no char filter, no normalization or repeat filter,
 * and the 115-word stop list. Fixtures are the inputs the Lucene 10.6 migration guide lists as changed.
 */
public class LegacyThaiAnalyzerTests extends ESTokenStreamTestCase {

    /** Guards against accidental edits to the vendored list or an unnoticed change to Lucene's. */
    public void testDefaultStopSetSizes() {
        assertEquals(115, LegacyThaiAnalyzer.getDefaultStopSet().size());
        assertSame(LegacyThaiStopWords.DEFAULT_STOP_SET, LegacyThaiAnalyzer.getDefaultStopSet());
        assertEquals(87, ThaiAnalyzer.getDefaultStopSet().size());
        try (LegacyThaiAnalyzer analyzer = new LegacyThaiAnalyzer()) {
            assertEquals(LegacyThaiStopWords.DEFAULT_STOP_SET, analyzer.getStopwordSet());
        }
    }

    public void testMigrationGuideInputsKeepLucene105Output() throws IOException {
        try (Analyzer legacy = new LegacyThaiAnalyzer(); Analyzer current = new ThaiAnalyzer()) {
            ThaiAnalyzerBwcTests.assertPreLucene106ThaiTokens(legacy);
            ThaiAnalyzerBwcTests.assertLucene106ThaiTokens(current);
        }
    }

    /**
     * Lucene 10.6 adds {@code ThaiCharFilter}, which strips zero-width characters before tokenization. Without it the zero-width
     * space survives into the indexed term.
     */
    public void testZeroWidthCharactersAreNotStripped() throws IOException {
        String text = "แม\u200Bว";
        try (Analyzer legacy = new LegacyThaiAnalyzer(); Analyzer current = new ThaiAnalyzer()) {
            assertAnalyzesTo(legacy, text, new String[] { "แม\u200Bว" });
            assertAnalyzesTo(current, text, new String[] { "แมว" });
        }
    }

    /** The normalization path (used for e.g. query-time normalization of wildcard terms) has no Thai char filter or normalizer. */
    public void testNormalizeHasNoThaiNormalization() {
        try (Analyzer legacy = new LegacyThaiAnalyzer(); Analyzer current = new ThaiAnalyzer()) {
            assertEquals(new BytesRef("เเมว"), legacy.normalize("f", "เเมว"));
            assertEquals(new BytesRef("แมว"), current.normalize("f", "เเมว"));
            // lowercase + decimal digit still apply
            assertEquals(new BytesRef("abc123"), legacy.normalize("f", "ABC๑๒๓"));
        }
    }

    public void testCustomStopWords() throws IOException {
        try (Analyzer analyzer = new LegacyThaiAnalyzer(new CharArraySet(List.of("แมว"), false))) {
            assertAnalyzesTo(analyzer, "ไป แมว", new String[] { "ไป" });
        }
    }

    public void testRandomStrings() throws IOException {
        try (Analyzer analyzer = new LegacyThaiAnalyzer()) {
            checkRandomData(random(), analyzer, 200);
        }
    }
}
