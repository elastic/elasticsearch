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
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.NameOrDefinition;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertStreamHasNumberOfTokens;
import static org.hamcrest.Matchers.containsString;

public class MinHashFilterFactoryTests extends ESTokenStreamTestCase {
    public void testDefault() throws IOException {
        int default_hash_count = 1;
        int default_bucket_size = 512;
        int default_hash_set_size = 1;
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("min_hash");
        String source = "the quick brown fox";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        // with_rotation is true by default, and hash_set_size is 1, so even though the source doesn't
        // have enough tokens to fill all the buckets, we still expect 512 tokens.
        assertStreamHasNumberOfTokens(tokenFilter.create(tokenizer), default_hash_count * default_bucket_size * default_hash_set_size);
    }

    public void testRotationSettings() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "1")
            .put("index.analysis.filter.test_min_hash.bucket_count", "2")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "1")
            .put("index.analysis.filter.test_min_hash.with_rotation", false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("test_min_hash");
        String source = "sushi";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        // despite the fact that bucket_count is 2 and hash_set_size is 1,
        // because with_rotation is false, we only expect 1 token here.
        assertStreamHasNumberOfTokens(tokenFilter.create(tokenizer), 1);
    }

    public void testHashCountSettings() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "1")
            .put("index.analysis.filter.test_min_hash.bucket_count", "1")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "1")
            .put("index.analysis.filter.test_min_hash.with_rotation", true)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("test_min_hash");
        String source = "sushi";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        // Single bucket should result in a single output token
        assertStreamHasNumberOfTokens(tokenFilter.create(tokenizer), 1);
    }

    public void testHashSetSize() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "1")
            .put("index.analysis.filter.test_min_hash.bucket_count", "1")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "2")
            .put("index.analysis.filter.test_min_hash.with_rotation", false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("test_min_hash");
        String source = "another, longer test";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        // hash_set_size = 2 should give us two buckets
        assertStreamHasNumberOfTokens(tokenFilter.create(tokenizer), 2);
    }

    // ---- Filters defined inline in a request (e.g. _analyze) ----

    public void testInlineOversizedParametersRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildInline(null, Map.of("hash_count", 2147483639, "bucket_count", 2147483639, "hash_set_size", 1))
        );
        assertThat(e.getMessage(), containsString("must not exceed"));
    }

    public void testInlineIndividualParameterCapEnforced() {
        Map<String, Integer> overLimit = Map.of(
            "hash_count",
            MinHashTokenFilterFactory.MAX_HASH_COUNT + 1,
            "bucket_count",
            MinHashTokenFilterFactory.MAX_BUCKET_COUNT + 1,
            "hash_set_size",
            MinHashTokenFilterFactory.MAX_HASH_SET_SIZE + 1
        );
        for (Map.Entry<String, Integer> entry : overLimit.entrySet()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> buildInline(null, Map.of(entry.getKey(), entry.getValue()))
            );
            assertThat(e.getMessage(), containsString("[" + entry.getKey() + "] must not exceed"));
        }
    }

    public void testInlineOverflowBypassRejected() {
        // Three values whose product wraps to zero in long arithmetic must still be rejected by the per-parameter cap.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildInline(null, Map.of("hash_count", 2097152, "bucket_count", 2097152, "hash_set_size", 4194304))
        );
        assertThat(e.getMessage(), containsString("must not exceed"));
    }

    public void testInlineProductIncludesHashSetSize() {
        // hash_count * bucket_count is within 10,000, but the filter can hold up to 10^8 hashes while reading its input.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildInline(null, Map.of("hash_count", 10000, "bucket_count", 1, "hash_set_size", 10000))
        );
        assertThat(e.getMessage(), containsString("exceeds the maximum token count limit [10000]"));
    }

    public void testInlineProductUsesIndexMaxTokenCount() throws IOException {
        IndexSettings indexSettings = indexSettings(IndexVersion.current(), Settings.builder().put("index.analyze.max_token_count", 20000));
        assertNotNull(buildInline(indexSettings, Map.of("hash_count", 1, "bucket_count", 10000, "hash_set_size", 2)));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildInline(indexSettings, Map.of("hash_count", 1, "bucket_count", 10000, "hash_set_size", 3))
        );
        assertThat(e.getMessage(), containsString("exceeds the maximum token count limit [20000]"));
    }

    public void testInlineCheckedAgainstLegacyIndex() {
        // Inline definitions are checked even when the request targets an index created before the index bounds were introduced.
        IndexSettings indexSettings = indexSettings(
            IndexVersionUtils.randomPreviousCompatibleWriteVersion(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS),
            Settings.builder()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildInline(indexSettings, Map.of("hash_count", 10000, "bucket_count", 1, "hash_set_size", 10000))
        );
        assertThat(e.getMessage(), containsString("exceeds the maximum token count limit"));
    }

    public void testInlineMaxParamValueAllowed() throws IOException {
        int maxBucketCount = MinHashTokenFilterFactory.MAX_BUCKET_COUNT;
        // Product is 1 * 10000 * 1 = 10000, which equals the default max_token_count.
        assertNotNull(buildInline(null, Map.of("hash_count", 1, "bucket_count", maxBucketCount, "hash_set_size", 1)));
    }

    // ---- Filters defined in the settings of indices created on or after MIN_HASH_INDEX_PARAM_BOUNDS ----

    public void testIndexHashSetsBound() throws IOException {
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS);
        assertNotNull(buildFromIndexSettings(version, Settings.builder(), 100, 100, 1));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildFromIndexSettings(version, Settings.builder(), 10001, 1, 1)
        );
        assertThat(e.getMessage(), containsString("is [10001], which exceeds the limit [10000]"));
    }

    public void testIndexHashEntriesBound() throws IOException {
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS);
        // 1 * 512 * 195 = 99,840
        assertNotNull(buildFromIndexSettings(version, Settings.builder(), 1, 512, 195));
        // 1 * 512 * 196 = 100,352
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildFromIndexSettings(version, Settings.builder(), 1, 512, 196)
        );
        assertThat(e.getMessage(), containsString("is [100352], which exceeds the limit [100000]"));
    }

    public void testIndexBoundsDoNotOverflow() {
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildFromIndexSettings(version, Settings.builder(), 2097152, 2097152, 4194304)
        );
        assertThat(e.getMessage(), containsString("which exceeds the limit [10000]"));
    }

    public void testIndexBoundsIgnoreMaxTokenCount() throws IOException {
        // index.analyze.max_token_count is dynamic; lowering it must not stop an index with a min_hash filter from opening.
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS);
        Settings.Builder lowMaxTokenCount = Settings.builder().put("index.analyze.max_token_count", 1);
        assertNotNull(buildFromIndexSettings(version, lowMaxTokenCount, 1, 512, 20));
    }

    // ---- Filters defined in the settings of indices created before MIN_HASH_INDEX_PARAM_BOUNDS ----

    public void testLegacyIndexAboveBoundsStillBuilds() throws IOException {
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleWriteVersion(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS);
        assertNotNull(buildFromIndexSettings(version, Settings.builder(), 1, 512, 200));
        assertWarnings(
            "[min_hash] filter [test_min_hash] on index [test]: The product of hash_count [1], bucket_count [512], and hash_set_size "
                + "[200] is [102400], which exceeds the limit [100000]. This configuration is allowed on this index, but can't be used "
                + "on new indices. Lower these parameters in the settings of any new index, for example a reindex destination; note "
                + "that this changes the hashes the filter produces."
        );
    }

    public void testLegacyIndexAboveParamCapStillBuilds() throws IOException {
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleWriteVersion(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS);
        Settings.Builder lowMaxTokenCount = Settings.builder().put("index.analyze.max_token_count", 1);
        assertNotNull(buildFromIndexSettings(version, lowMaxTokenCount, 1, MinHashTokenFilterFactory.MAX_BUCKET_COUNT + 1, 1));
        assertWarnings(
            "[min_hash] filter [test_min_hash] on index [test]: The product of hash_count [1] and bucket_count [10001] is [10001], "
                + "which exceeds the limit [10000]. This configuration is allowed on this index, but can't be used on new indices. "
                + "Lower these parameters in the settings of any new index, for example a reindex destination; note that this "
                + "changes the hashes the filter produces."
        );
    }

    public void testLegacyIndexWithinBoundsNoWarning() throws IOException {
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleWriteVersion(IndexVersions.MIN_HASH_INDEX_PARAM_BOUNDS);
        assertNotNull(buildFromIndexSettings(version, Settings.builder(), 1, 512, 20));
        // ESTestCase fails the test on any unexpected deprecation warning.
    }

    private TokenFilterFactory buildFromIndexSettings(
        IndexVersion version,
        Settings.Builder extraIndexSettings,
        int hashCount,
        int bucketCount,
        int hashSetSize
    ) throws IOException {
        Settings settings = extraIndexSettings.put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", hashCount)
            .put("index.analysis.filter.test_min_hash.bucket_count", bucketCount)
            .put("index.analysis.filter.test_min_hash.hash_set_size", hashSetSize)
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        return analysis.tokenFilter.get("test_min_hash");
    }

    /**
     * Builds an analyzer with a {@code min_hash} filter defined inline, the way {@code _analyze} does for a request whose
     * filter is given as a definition rather than a name.
     */
    private NamedAnalyzer buildInline(IndexSettings indexSettings, Map<String, Integer> params) throws IOException {
        Map<String, Object> definition = new HashMap<>(params);
        definition.put("type", "min_hash");
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(nodeSettings),
            List.of(new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
        return registry.buildCustomAnalyzer(
            IndexService.IndexCreationContext.RELOAD_ANALYZERS,
            indexSettings,
            false,
            new NameOrDefinition("whitespace"),
            List.of(),
            List.of(new NameOrDefinition(definition))
        );
    }

    private static IndexSettings indexSettings(IndexVersion version, Settings.Builder settings) {
        return IndexSettingsModule.newIndexSettings("test", settings.put(IndexMetadata.SETTING_VERSION_CREATED, version).build());
    }
}
