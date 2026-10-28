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
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Pins every Elasticsearch entry point to Thai analysis to the token stream its index was created with. Lucene 10.6 changed what
 * Lucene's Thai analysis produces; indices created before {@link IndexVersions#THAI_ANALYSIS_LUCENE_10_6} must keep the Lucene 10.5
 * tokens (otherwise documents indexed after an upgrade, and queries, would be analyzed differently from the documents already in the
 * index), while newer indices get Lucene's current output. The entry points are:
 * <ul>
 *   <li>{@code type: thai} analyzer ({@link ThaiAnalyzerProvider})</li>
 *   <li>prebuilt {@code thai} analyzer ({@link PreBuiltAnalyzerProviderFactory})</li>
 *   <li>{@code type: thai} tokenizer ({@link ThaiTokenizerFactory})</li>
 *   <li>preconfigured {@code thai} tokenizer ({@link PreConfiguredTokenizer})</li>
 *   <li>{@code stopwords: _thai_} on any stop-aware component</li>
 * </ul>
 * The Lucene changes are:
 * <ul>
 *   <li>{@code ThaiCharFilter} + {@code ThaiNormalizationFilter}: double Sara E ({@code เเ} → {@code แ}) and other orthographic
 *   cleanup. <a href="https://github.com/apache/lucene/pull/16717">apache/lucene#16717</a></li>
 *   <li>{@code ThaiRepeatFilter}: Maiyamok {@code ๆ} becomes a real repeat ({@code เร็วๆ} → two {@code เร็ว} tokens instead of
 *   {@code เร็ว} + {@code ๆ}). <a href="https://github.com/apache/lucene/pull/16720">apache/lucene#16720</a></li>
 *   <li>Curated {@code stopwords.txt}: content verbs/nouns such as {@code ไป}, {@code ผ่าน}, and {@code มา} are no longer stopped,
 *   so compounds like {@code ที่ผ่านมา} emit tokens instead of disappearing.
 *   <a href="https://github.com/apache/lucene/pull/16718">apache/lucene#16718</a></li>
 *   <li>{@code ThaiTokenizer#isSafeEnd} accepts any whitespace as a safe 1024-char buffer cut, so a word straddling the buffer
 *   after a space is no longer sliced. <a href="https://github.com/apache/lucene/pull/16727">apache/lucene#16727</a></li>
 * </ul>
 */
public class ThaiAnalyzerBwcTests extends ESTokenStreamTestCase {

    public void testNamedThaiAnalyzer() throws IOException {
        assertPreLucene106ThaiTokens(namedThaiAnalyzer(legacyVersion()));
        assertLucene106ThaiTokens(namedThaiAnalyzer(gatedVersion()));
    }

    /** Mappings that only set {@code "analyzer": "thai"} use the prebuilt instance, so it must follow the same gate. */
    public void testPrebuiltThaiAnalyzer() throws IOException {
        NamedAnalyzer legacy = analysis(legacyVersion(), Settings.EMPTY).indexAnalyzers.get("thai");
        assertThat(legacy.analyzer(), instanceOf(LegacyThaiAnalyzer.class));
        assertPreLucene106ThaiTokens(legacy);

        NamedAnalyzer current = analysis(gatedVersion(), Settings.EMPTY).indexAnalyzers.get("thai");
        assertThat(current.analyzer(), instanceOf(ThaiAnalyzer.class));
        assertLucene106ThaiTokens(current);
    }

    /**
     * {@code stopwords: _thai_} with only {@code tokenizer: thai} isolates the named stop set from the analyzer filters. Pre-10.6,
     * {@code ไป} and {@code ที่ผ่านมา} ({@code ที่}+{@code ผ่าน}+{@code มา}) produced no tokens.
     */
    public void testThaiStopwordName() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.thai_stops.type", "stop")
            .put("index.analysis.filter.thai_stops.stopwords", "_thai_")
            .put("index.analysis.analyzer.thai_stops.tokenizer", "thai")
            .put("index.analysis.analyzer.thai_stops.filter", "thai_stops")
            .build();
        Analyzer legacy = analysis(legacyVersion(), settings).indexAnalyzers.get("thai_stops");
        assertAnalyzesTo(legacy, "ไป", new String[] {});
        assertAnalyzesTo(legacy, "ที่ผ่านมา", new String[] {});

        Analyzer current = analysis(gatedVersion(), settings).indexAnalyzers.get("thai_stops");
        assertAnalyzesTo(current, "ไป", new String[] { "ไป" });
        assertAnalyzesTo(current, "ที่ผ่านมา", new String[] { "ผ่าน", "มา" });
    }

    /** An explicit {@code stopwords: _thai_} on the {@code thai} analyzer resolves through the same gate as its default. */
    public void testThaiAnalyzerWithExplicitThaiStopwords() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_thai.type", "thai")
            .put("index.analysis.analyzer.my_thai.stopwords", "_thai_")
            .build();
        assertPreLucene106ThaiTokens(analysis(legacyVersion(), settings).indexAnalyzers.get("my_thai"));
        assertLucene106ThaiTokens(analysis(gatedVersion(), settings).indexAnalyzers.get("my_thai"));
    }

    public void testNamedThaiTokenizer() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.tokenizer.my_thai.type", "thai")
            .put("index.analysis.analyzer.thai_only.tokenizer", "my_thai")
            .build();
        ESTestCase.TestAnalysis legacy = analysis(legacyVersion(), settings);
        assertThat(legacy.tokenizer.get("my_thai").create(), instanceOf(LegacyThaiTokenizer.class));
        assertPreLucene106ThaiTokenizer(legacy.indexAnalyzers.get("thai_only"));

        ESTestCase.TestAnalysis current = analysis(gatedVersion(), settings);
        assertThat(current.tokenizer.get("my_thai").create(), not(instanceOf(LegacyThaiTokenizer.class)));
        assertLucene106ThaiTokenizer(current.indexAnalyzers.get("thai_only"));
    }

    /** {@code _analyze} and mappings that name the tokenizer without declaring a factory use the preconfigured registration. */
    public void testPreconfiguredThaiTokenizer() throws IOException {
        Settings settings = Settings.builder().put("index.analysis.analyzer.thai_only.tokenizer", "thai").build();
        assertPreLucene106ThaiTokenizer(analysis(legacyVersion(), settings).indexAnalyzers.get("thai_only"));
        assertLucene106ThaiTokenizer(analysis(gatedVersion(), settings).indexAnalyzers.get("thai_only"));
    }

    /** The version immediately before the gate is legacy and the gate version itself is not. */
    public void testGateBoundary() throws IOException {
        IndexVersion before = IndexVersionUtils.getPreviousVersion(IndexVersions.THAI_ANALYSIS_LUCENE_10_6);
        IndexVersion at = IndexVersions.THAI_ANALYSIS_LUCENE_10_6;

        assertThat(new ThaiAnalyzerProvider(indexSettings(before), env(), "a", Settings.EMPTY).get(), instanceOf(LegacyThaiAnalyzer.class));
        assertThat(new ThaiAnalyzerProvider(indexSettings(at), env(), "a", Settings.EMPTY).get(), instanceOf(ThaiAnalyzer.class));
        assertThat(
            new ThaiTokenizerFactory(indexSettings(before), env(), "t", Settings.EMPTY).create(),
            instanceOf(LegacyThaiTokenizer.class)
        );
        assertThat(
            new ThaiTokenizerFactory(indexSettings(at), env(), "t", Settings.EMPTY).create(),
            not(instanceOf(LegacyThaiTokenizer.class))
        );

        PreConfiguredTokenizer preconfigured = new CommonAnalysisPlugin().getPreConfiguredTokenizers()
            .stream()
            .filter(t -> t.getName().equals("thai"))
            .findFirst()
            .orElseThrow();
        assertThat(preconfigured.get(indexSettings(before), env(), "thai", Settings.EMPTY).create(), instanceOf(LegacyThaiTokenizer.class));
        assertThat(
            preconfigured.get(indexSettings(at), env(), "thai", Settings.EMPTY).create(),
            not(instanceOf(LegacyThaiTokenizer.class))
        );
    }

    /**
     * Two indices with identical Thai settings on opposite sides of the gate must never share a component: the gate side is part of
     * every gated factory's sharing key. Identical settings on the same side still share.
     */
    public void testSharingKeysIncludeGateSide() throws IOException {
        IndexSettings legacy = indexSettings(legacyVersion());
        IndexSettings otherLegacy = indexSettings(legacyVersion());
        IndexSettings current = indexSettings(gatedVersion());
        Environment env = env();

        Object legacyTokenizer = new ThaiTokenizerFactory(legacy, env, "a", Settings.EMPTY).sharingKey();
        assertEquals(legacyTokenizer, new ThaiTokenizerFactory(otherLegacy, env, "b", Settings.EMPTY).sharingKey());
        assertNotEquals(legacyTokenizer, new ThaiTokenizerFactory(current, env, "c", Settings.EMPTY).sharingKey());

        Object legacyAnalyzer = new ThaiAnalyzerProvider(legacy, env, "a", Settings.EMPTY).sharingKey();
        assertEquals(legacyAnalyzer, new ThaiAnalyzerProvider(otherLegacy, env, "b", Settings.EMPTY).sharingKey());
        assertNotEquals(legacyAnalyzer, new ThaiAnalyzerProvider(current, env, "c", Settings.EMPTY).sharingKey());

        // The same explicit list on both sides: only the gate side tells the two chains apart.
        Settings explicit = Settings.builder().putList("stopwords", "foo").build();
        assertNotEquals(
            new ThaiAnalyzerProvider(legacy, env, "a", explicit).sharingKey(),
            new ThaiAnalyzerProvider(current, env, "b", explicit).sharingKey()
        );
    }

    private static IndexVersion legacyVersion() {
        return IndexVersionUtils.randomPreviousCompatibleWriteVersion(IndexVersions.THAI_ANALYSIS_LUCENE_10_6);
    }

    private static IndexVersion gatedVersion() {
        return IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.THAI_ANALYSIS_LUCENE_10_6);
    }

    private IndexSettings indexSettings(IndexVersion version) {
        return IndexSettingsModule.newIndexSettings(
            "index",
            ESTestCase.indexSettings(1, 1).put(IndexMetadata.SETTING_VERSION_CREATED, version).build()
        );
    }

    private Environment env() {
        return new Environment(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(), null);
    }

    private ESTestCase.TestAnalysis analysis(IndexVersion version, Settings analysisSettings) throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(analysisSettings)
            .build();
        return AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
    }

    private Analyzer namedThaiAnalyzer(IndexVersion version) {
        return new ThaiAnalyzerProvider(indexSettings(version), env(), "my-analyzer", Settings.EMPTY).get();
    }

    /** Lucene 10.5 {@code ThaiAnalyzer} tokens. */
    static void assertPreLucene106ThaiTokens(Analyzer analyzer) throws IOException {
        // #16717: เเ stays เเ without ThaiCharFilter / ThaiNormalizationFilter
        assertAnalyzesTo(analyzer, "เเปลก", new String[] { "เเปลก" });
        assertAnalyzesTo(analyzer, "เเมว", new String[] { "เเมว" });
        // #16720: ๆ is a leftover token, not a repeated เร็ว
        assertAnalyzesTo(analyzer, "เร็วๆ", new String[] { "เร็ว", "ๆ" });
        // #16718: ไป / ที่+ผ่าน+มา were all default stops
        assertAnalyzesTo(analyzer, "ไป", new String[] {});
        assertAnalyzesTo(analyzer, "ที่ผ่านมา", new String[] {});
    }

    /** Lucene {@code ThaiAnalyzer} tokens since 10.6. */
    static void assertLucene106ThaiTokens(Analyzer analyzer) throws IOException {
        assertAnalyzesTo(analyzer, "เเปลก", new String[] { "แปลก" });
        assertAnalyzesTo(analyzer, "เเมว", new String[] { "แมว" });
        assertAnalyzesTo(analyzer, "เร็วๆ", new String[] { "เร็ว", "เร็ว" });
        assertAnalyzesTo(analyzer, "ไป", new String[] { "ไป" });
        assertAnalyzesTo(analyzer, "ที่ผ่านมา", new String[] { "ผ่าน", "มา" });
    }

    /** 1021 {@code ก} characters, a space, then {@code แมว} sitting across the default 1024-char buffer. */
    static final String LONG_BUFFER_TEXT = "ก".repeat(1021) + " แมว";

    /** Pre-10.6 {@code ThaiTokenizer} long-buffer tokens: only line terminators are safe cuts, so {@code แมว} is sliced. */
    static void assertPreLucene106ThaiTokenizer(Analyzer analyzer) throws IOException {
        assertAnalyzesTo(analyzer, LONG_BUFFER_TEXT, longBufferTokens("แม", "ว"));
    }

    /** {@code ThaiTokenizer} long-buffer tokens since 10.6: the space before {@code แมว} is a safe cut, so it stays whole. */
    static void assertLucene106ThaiTokenizer(Analyzer analyzer) throws IOException {
        assertAnalyzesTo(analyzer, LONG_BUFFER_TEXT, longBufferTokens("แมว"));
    }

    private static String[] longBufferTokens(String... tail) {
        String[] expected = new String[511 + tail.length];
        Arrays.fill(expected, 0, 510, "กก");
        expected[510] = "ก";
        System.arraycopy(tail, 0, expected, 511, tail.length);
        return expected;
    }
}
