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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
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
 * Lucene's Thai analysis produces; indices created on a Lucene version before {@link Version#LUCENE_10_6_0} must keep the Lucene 10.5
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
        assertThat(legacy.analyzer(), instanceOf(ThaiLucene105Analyzer.class));
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

    public void testNamedThaiTokenizer() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.tokenizer.my_thai.type", "thai")
            .put("index.analysis.analyzer.thai_only.tokenizer", "my_thai")
            .build();
        ESTestCase.TestAnalysis legacy = analysis(legacyVersion(), settings);
        assertThat(legacy.tokenizer.get("my_thai").create(), instanceOf(ThaiLucene105Tokenizer.class));
        assertPreLucene106ThaiTokenizer(legacy.indexAnalyzers.get("thai_only"));

        ESTestCase.TestAnalysis current = analysis(gatedVersion(), settings);
        assertThat(current.tokenizer.get("my_thai").create(), not(instanceOf(ThaiLucene105Tokenizer.class)));
        assertLucene106ThaiTokenizer(current.indexAnalyzers.get("thai_only"));
    }

    /** {@code _analyze} and mappings that name the tokenizer without declaring a factory use the preconfigured registration. */
    public void testPreconfiguredThaiTokenizer() throws IOException {
        Settings settings = Settings.builder().put("index.analysis.analyzer.thai_only.tokenizer", "thai").build();
        assertPreLucene106ThaiTokenizer(analysis(legacyVersion(), settings).indexAnalyzers.get("thai_only"));
        assertLucene106ThaiTokenizer(analysis(gatedVersion(), settings).indexAnalyzers.get("thai_only"));
    }

    /** The gate is the index's Lucene version, so versions are chosen by Lucene version and not by index version ordering. */
    private static IndexVersion legacyVersion() {
        return IndexVersionUtils.randomWriteVersionBeforeLucene(Version.LUCENE_10_6_0);
    }

    private static IndexVersion gatedVersion() {
        return IndexVersionUtils.randomWriteVersionOnOrAfterLucene(Version.LUCENE_10_6_0);
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
        // #16717: no ThaiCharFilter, so a zero-width space survives into the term
        assertAnalyzesTo(analyzer, "แม​ว", new String[] { "แม​ว" });
        // the normalization path (e.g. wildcard terms) has no Thai normalization either
        assertEquals(new BytesRef("เเมว"), analyzer.normalize("f", "เเมว"));
    }

    /** Lucene {@code ThaiAnalyzer} tokens since 10.6. */
    static void assertLucene106ThaiTokens(Analyzer analyzer) throws IOException {
        assertAnalyzesTo(analyzer, "เเปลก", new String[] { "แปลก" });
        assertAnalyzesTo(analyzer, "เเมว", new String[] { "แมว" });
        assertAnalyzesTo(analyzer, "เร็วๆ", new String[] { "เร็ว", "เร็ว" });
        assertAnalyzesTo(analyzer, "ไป", new String[] { "ไป" });
        assertAnalyzesTo(analyzer, "ที่ผ่านมา", new String[] { "ผ่าน", "มา" });
        assertAnalyzesTo(analyzer, "แม​ว", new String[] { "แมว" });
        assertEquals(new BytesRef("แมว"), analyzer.normalize("f", "เเมว"));
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
