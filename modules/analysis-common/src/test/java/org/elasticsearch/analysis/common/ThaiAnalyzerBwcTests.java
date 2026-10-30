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
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.security.CodeSource;
import java.util.Arrays;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;

/**
 * Pins existing Thai names to their pre-Lucene-10.6 token streams once Lucene is no longer a
 * snapshot artifact.
 *
 * <p>{@code lucene_snapshot} may wrap Lucene's current {@code ThaiAnalyzer}, stop list, and
 * {@code ThaiTokenizer} while those defaults are still under discussion. {@link Version#LATEST}
 * does not carry the snapshot qualifier, so this suite looks at the Lucene jar version string. A
 * released Lucene (no {@code -snapshot} in that string) must keep the tokens below until
 * Elasticsearch pins the old recipe.
 */
public class ThaiAnalyzerBwcTests extends ESTokenStreamTestCase {

    /**
     * Named {@code analyzer: thai} ({@link ThaiAnalyzerProvider}) wraps Lucene's
     * {@code ThaiAnalyzer}. Through 10.5 that was tokenizer → lowercase → decimal_digit →
     * stop. Lucene 10.6 changed the default stream in three ways that would force a reindex
     * for anyone already using this name:
     * <ul>
     *   <li>{@code ThaiCharFilter} + {@code ThaiNormalizationFilter} — double Sara E
     *   ({@code เเ} → {@code แ}) and other orthographic cleanup.
     *   <a href="https://github.com/apache/lucene/pull/16717">apache/lucene#16717</a></li>
     *   <li>{@code ThaiRepeatFilter} — Maiyamok {@code ๆ} becomes a real repeat
     *   ({@code เร็วๆ} → two {@code เร็ว} tokens instead of {@code เร็ว} + {@code ๆ}).
     *   <a href="https://github.com/apache/lucene/pull/16720">apache/lucene#16720</a></li>
     *   <li>Curated {@code stopwords.txt} — content verbs/nouns such as {@code ไป},
     *   {@code ผ่าน}, and {@code มา} are no longer stopped, so compounds like
     *   {@code ที่ผ่านมา} emit tokens instead of disappearing.
     *   <a href="https://github.com/apache/lucene/pull/16718">apache/lucene#16718</a></li>
     * </ul>
     * Fixtures below are the 10.5 tokens. Current Lucene emits {@code แปลก}, {@code แมว},
     * two {@code เร็ว}, {@code ไป}, and {@code ผ่าน}/{@code มา}.
     */
    public void testNamedThaiAnalyzerMatchesPreLucene106Tokens() throws IOException {
        assumeNotLuceneSnapshot();
        assertPreLucene106ThaiTokens(namedThaiAnalyzer());
    }

    /**
     * Prebuilt {@code thai} is a second registration of the same Lucene {@code ThaiAnalyzer}
     * ({@code PreBuiltAnalyzerProviderFactory}, no index settings). It picks up the same 10.6
     * default-chain breakages as the named provider: char filter and normalization
     * (<a href="https://github.com/apache/lucene/pull/16717">apache/lucene#16717</a>),
     * Maiyamok expansion
     * (<a href="https://github.com/apache/lucene/pull/16720">apache/lucene#16720</a>),
     * and the curated stop list
     * (<a href="https://github.com/apache/lucene/pull/16718">apache/lucene#16718</a>).
     * Mappings that only set {@code "analyzer": "thai"} use this instance, so it must stay
     * token-identical to the named analyzer pin.
     */
    public void testPrebuiltThaiAnalyzerMatchesPreLucene106Tokens() throws IOException {
        assumeNotLuceneSnapshot();
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        assertPreLucene106ThaiTokens(analysis.indexAnalyzers.get("thai"));
    }

    /**
     * {@code stopwords: _thai_} loads {@code ThaiAnalyzer.getDefaultStopSet()} — the same
     * {@code stopwords.txt} Lucene curated in
     * <a href="https://github.com/apache/lucene/pull/16718">apache/lucene#16718</a>.
     * That PR dropped content verbs/nouns that were over-filtering compounds (including
     * {@code ไป}, {@code ผ่าน}, and {@code มา}) and added composed spellings
     * ({@code สำหรับ}, {@code ทำให้}).
     *
     * <p>This test uses only {@code tokenizer: thai} plus a stop filter, so it isolates the
     * named stop set from the new analyzer filters. Pre-10.6, {@code ไป} and
     * {@code ที่ผ่านมา} ({@code ที่}+{@code ผ่าน}+{@code มา}) produced no tokens. After
     * #16718 they become {@code ไป} and {@code ผ่าน}/{@code มา}. Any custom analyzer that
     * already referenced {@code _thai_} would change tokens on upgrade without a pin.
     */
    public void testThaiStopwordNameMatchesPreLucene106Stops() throws IOException {
        assumeNotLuceneSnapshot();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.thai_stops.type", "stop")
            .put("index.analysis.filter.thai_stops.stopwords", "_thai_")
            .put("index.analysis.analyzer.thai_stops.tokenizer", "thai")
            .put("index.analysis.analyzer.thai_stops.filter", "thai_stops")
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        Analyzer analyzer = analysis.indexAnalyzers.get("thai_stops");
        assertAnalyzesTo(analyzer, "ไป", new String[] {});
        assertAnalyzesTo(analyzer, "ที่ผ่านมา", new String[] {});
    }

    /**
     * Named {@code tokenizer: thai} ({@link ThaiTokenizerFactory}) is Lucene's
     * {@code ThaiTokenizer}. Through 10.5, {@code SegmentingTokenizerBase} only treated
     * newlines as a safe 1024-char buffer cut. Thai often uses spaces instead of newlines,
     * so a word that straddled offset 1024 after a space was sliced into fragments.
     * <a href="https://github.com/apache/lucene/pull/16727">apache/lucene#16727</a>
     * overrides {@code isSafeEnd} to include whitespace, so that word stays intact.
     *
     * <p>Fixture: 1021 {@code ก} characters, a space, then {@code แมว} sitting across the
     * default buffer. Pre-10.6 tokens end {@code แม} + {@code ว}. Current Lucene emits one
     * {@code แมว}. Short Thai strings do not show this; only the long-buffer case does.
     */
    public void testNamedThaiTokenizerMatchesPreLucene106BufferCut() throws IOException {
        assumeNotLuceneSnapshot();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.tokenizer.my_thai.type", "thai")
            .put("index.analysis.analyzer.thai_only.tokenizer", "my_thai")
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        assertPreLucene106ThaiTokenizer(analysis.indexAnalyzers.get("thai_only"));
    }

    /**
     * Preconfigured {@code thai} tokenizer ({@code PreConfiguredTokenizer.singleton}) is the
     * no-settings twin of {@link ThaiTokenizerFactory}. It constructs {@code new ThaiTokenizer()}
     * the same way, so
     * <a href="https://github.com/apache/lucene/pull/16727">apache/lucene#16727</a>
     * changes its long-buffer tokens as well: whitespace becomes a safe 1024-char cut and
     * {@code แมว} spanning that cut is no longer split into {@code แม} + {@code ว}.
     * {@code _analyze} / mappings that name the tokenizer without declaring a factory use
     * this registration.
     */
    public void testPreconfiguredThaiTokenizerMatchesPreLucene106BufferCut() throws IOException {
        assumeNotLuceneSnapshot();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.analyzer.thai_only.tokenizer", "thai")
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        assertPreLucene106ThaiTokenizer(analysis.indexAnalyzers.get("thai_only"));
    }

    private Analyzer namedThaiAnalyzer() {
        Settings settings = ESTestCase.indexSettings(1, 1)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        Environment environment = new Environment(settings, null);
        return new ThaiAnalyzerProvider(idxSettings, environment, "my-analyzer", Settings.EMPTY).get();
    }

    /**
     * Pre-10.6 {@code ThaiAnalyzer} tokens. Current Lucene changes every fixture: #16717
     * ({@code แปลก}, {@code แมว}), #16720 (two {@code เร็ว}), #16718 ({@code ไป},
     * {@code ผ่าน}/{@code มา}).
     */
    private static void assertPreLucene106ThaiTokens(Analyzer analyzer) throws IOException {
        // #16717: เเ stays เเ without ThaiCharFilter / ThaiNormalizationFilter
        assertAnalyzesTo(analyzer, "เเปลก", new String[] { "เเปลก" });
        assertAnalyzesTo(analyzer, "เเมว", new String[] { "เเมว" });
        // #16720: ๆ is a leftover token, not a repeated เร็ว
        assertAnalyzesTo(analyzer, "เร็วๆ", new String[] { "เร็ว", "ๆ" });
        // #16718: ไป / ที่+ผ่าน+มา were all default stops
        assertAnalyzesTo(analyzer, "ไป", new String[] {});
        assertAnalyzesTo(analyzer, "ที่ผ่านมา", new String[] {});
    }

    /**
     * Pre-10.6 {@code ThaiTokenizer} long-buffer tokens. #16727 would keep {@code แมว} as
     * one token because the space before it is now a safe 1024-char cut.
     */
    private static void assertPreLucene106ThaiTokenizer(Analyzer analyzer) throws IOException {
        String text = "ก".repeat(1021) + " แมว";
        String[] expected = new String[513];
        Arrays.fill(expected, 0, 510, "กก");
        expected[510] = "ก";
        expected[511] = "แม";
        expected[512] = "ว";
        assertAnalyzesTo(analyzer, text, expected);
    }

    private static void assumeNotLuceneSnapshot() {
        assumeFalse("Pin named Thai analysis to the pre-10.6 recipe before shipping a non-snapshot Lucene", isLuceneSnapshotArtifact());
    }

    /**
     * True when the Lucene dependency is a snapshot jar ({@code 10.6.0-snapshot-…}), not when
     * Elasticsearch itself is a snapshot. {@link Version#LATEST} is typically unqualified.
     */
    private static boolean isLuceneSnapshotArtifact() {
        Package lucenePackage = Version.class.getPackage();
        if (containsSnapshot(lucenePackage == null ? null : lucenePackage.getImplementationVersion())) {
            return true;
        }
        CodeSource codeSource = Version.class.getProtectionDomain().getCodeSource();
        return codeSource != null && containsSnapshot(String.valueOf(codeSource.getLocation()));
    }

    private static boolean containsSnapshot(String value) {
        return value != null && value.contains("-snapshot");
    }
}
