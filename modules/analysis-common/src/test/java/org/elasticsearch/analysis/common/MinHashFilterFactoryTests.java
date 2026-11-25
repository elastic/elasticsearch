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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

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

    public void testOversizedParametersRejected() {
        // hash_count * bucket_count * hash_set_size far exceeds the default max_token_count (10000)
        Settings settings = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "2147483639")
            .put("index.analysis.filter.test_min_hash.bucket_count", "2147483639")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "1")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin())
        );
        assertThat(e.getMessage(), containsString("must not exceed"));
    }

    public void testIndividualParameterCapEnforced() {
        // Each parameter should be independently rejected when it exceeds its cap
        String[][] paramsAndLimits = {
            { "hash_count", Integer.toString(MinHashTokenFilterFactory.MAX_HASH_COUNT + 1) },
            { "bucket_count", Integer.toString(MinHashTokenFilterFactory.MAX_BUCKET_COUNT + 1) },
            { "hash_set_size", Integer.toString(MinHashTokenFilterFactory.MAX_HASH_SET_SIZE + 1) } };
        for (String[] entry : paramsAndLimits) {
            String param = entry[0];
            String overLimit = entry[1];
            Settings settings = Settings.builder()
                .put("index.analysis.filter.test_min_hash.type", "min_hash")
                .put("index.analysis.filter.test_min_hash." + param, overLimit)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin())
            );
            assertThat(e.getMessage(), containsString("[" + param + "] must not exceed"));
        }
    }

    public void testOverflowBypassRejected() {
        // Regression test: three values whose mathematical product wraps to zero in long arithmetic
        // should still be rejected by the per-parameter cap. This is the bypass described in
        // elastic/security#12881 where hash_set_size was raised to force a long overflow.
        Settings settings = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "2097152")
            .put("index.analysis.filter.test_min_hash.bucket_count", "2097152")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "4194304")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin())
        );
        assertThat(e.getMessage(), containsString("must not exceed"));
    }

    public void testMaxParamValueAllowed() throws IOException {
        // Boundary test: exactly the maximum should pass the per-parameter cap (but may still
        // be rejected by the product check against max_token_count).
        int maxBucketCount = MinHashTokenFilterFactory.MAX_BUCKET_COUNT;
        Settings settings = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "1")
            .put("index.analysis.filter.test_min_hash.bucket_count", maxBucketCount)
            .put("index.analysis.filter.test_min_hash.hash_set_size", "1")
            .put("index.analyze.max_token_count", maxBucketCount)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        // Should not throw — product is 1 * 10000 * 1 = 10000, which equals max_token_count
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        assertNotNull(analysis.tokenFilter.get("test_min_hash"));
    }

}
