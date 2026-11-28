/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.io.StringReader;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class SimplePatternTokenizerFactoryTests extends ESTokenStreamTestCase {

    public void testNonDeterministicPatterns() throws IOException {
        assertTokens("ab|ac", "ab ac ad", "ab", "ac");
        assertTokens("[0-9]{2}/[0-9]{2}/[0-9]{4}|[0-9]{4}-[0-9]{2}-[0-9]{2}", "on 05/11/2026 or 2026-11-05", "05/11/2026", "2026-11-05");
    }

    public void testLongestMatchWins() throws IOException {
        assertTokens("[a-z]+|[a-z]+\\.[a-z]+", "foo.bar baz", "foo.bar", "baz");
    }

    /**
     * A pattern that is too complex to determinize must only fail when a tokenizer is actually
     * created, so that defining such a tokenizer without using it in an analyzer stays harmless.
     */
    public void testTooComplexPatternFailsOnlyOnCreate() {
        SimplePatternTokenizerFactory factory = newFactory("(a|b)*a(a|b){20}");
        expectThrows(TooComplexToDeterminizeException.class, factory::create);
    }

    public void testTildeIsLiteral() throws IOException {
        assertTokens(IndexVersion.current(), "x~a", "xyz x~a", "x~a");
        assertTokens(IndexVersion.current(), "~a", "a ~a", "~a");
    }

    /** Indices created before Lucene 10 keep {@code ~} as complement: {@code x~a} is "x then anything but a". */
    public void testTildeIsComplementOnLegacyIndices() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
        assumeTrue("no read-compatible version predates Lucene 10", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));
        assertTokens(legacy, "x~a", "a b xyz", "xyz");
        assertTokens(legacy, "x~a", "xa", "x");
    }

    public void testSplitTildeIsLiteral() throws IOException {
        assertSplitTokens(IndexVersion.current(), "~x", "foo-x-bar~xbaz", "foo-x-bar", "baz");
    }

    /** Indices created before Lucene 10 keep {@code ~} as complement: {@code ~x} matches the whole input as a separator. */
    public void testSplitTildeIsComplementOnLegacyIndices() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
        assumeTrue("no read-compatible version predates Lucene 10", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));
        assertSplitTokens(legacy, "~x", "foo-x-bar~xbaz");
    }

    private void assertTokens(String pattern, String text, String... expected) throws IOException {
        assertTokens(IndexVersion.current(), pattern, text, expected);
    }

    private void assertTokens(IndexVersion version, String pattern, String text, String... expected) throws IOException {
        Tokenizer tokenizer = new SimplePatternTokenizerFactory(indexSettings(version), null, "simple_pattern", patternSettings(pattern))
            .create();
        tokenizer.setReader(new StringReader(text));
        assertTokenStreamContents(tokenizer, expected);
    }

    private void assertSplitTokens(IndexVersion version, String pattern, String text, String... expected) throws IOException {
        Tokenizer tokenizer = new SimplePatternSplitTokenizerFactory(
            indexSettings(version),
            null,
            "simple_pattern_split",
            patternSettings(pattern)
        ).create();
        tokenizer.setReader(new StringReader(text));
        assertTokenStreamContents(tokenizer, expected);
    }

    private SimplePatternTokenizerFactory newFactory(String pattern) {
        return new SimplePatternTokenizerFactory(indexSettings(IndexVersion.current()), null, "simple_pattern", patternSettings(pattern));
    }

    private IndexSettings indexSettings(IndexVersion version) {
        Settings settings = newAnalysisSettingsBuilder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        return IndexSettingsModule.newIndexSettings(new Index("test", "_na_"), settings);
    }

    private static Settings patternSettings(String pattern) {
        return Settings.builder().put("pattern", pattern).build();
    }
}
