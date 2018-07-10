/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.MlParserType;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class CategorizationAnalyzerConfigTests extends AbstractSerializingTestCase<CategorizationAnalyzerConfig> {

    private AnalysisRegistry analysisRegistry;
    private Environment environment;

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        environment = TestEnvironment.newEnvironment(settings);
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(environment);
    }

    public void testVerify_GivenNoConfig() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("categorization_analyzer that is not a global analyzer must specify a [tokenizer] field", e.getMessage());
    }

    public void testVerify_GivenDefault() throws IOException {
        CategorizationAnalyzerConfig defaultConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null);
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder(defaultConfig);
        builder.verify(analysisRegistry, environment);
    }

    public void testVerify_GivenValidAnalyzer() throws IOException {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().setAnalyzer("standard");
        builder.verify(analysisRegistry, environment);
    }

    public void testVerify_GivenInvalidAnalyzer() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().setAnalyzer("does not exist");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("Failed to find global analyzer [does not exist]", e.getMessage());
    }

    public void testVerify_GivenValidCustomConfig() throws IOException {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        Map<String, Object> ignoreStuffThatBeginsWithADigit = new HashMap<>();
        ignoreStuffThatBeginsWithADigit.put("type", "pattern_replace");
        ignoreStuffThatBeginsWithADigit.put("pattern", "^[0-9].*");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .addCharFilter(ignoreStuffInSqaureBrackets)
                .setTokenizer("classic")
                .addTokenFilter("lowercase")
                .addTokenFilter(ignoreStuffThatBeginsWithADigit)
                .addTokenFilter("snowball");
        builder.verify(analysisRegistry, environment);
    }

    public void testVerify_GivenCustomConfigWithInvalidCharFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .addCharFilter("wrong!")
                .setTokenizer("classic")
                .addTokenFilter("lowercase")
                .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("Failed to find global char filter under [wrong!]", e.getMessage());
    }

    public void testVerify_GivenCustomConfigWithMisconfiguredCharFilter() {
        Map<String, Object> noPattern = new HashMap<>();
        noPattern.put("type", "pattern_replace");
        noPattern.put("attern", "should have been pattern");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .addCharFilter(noPattern)
                .setTokenizer("classic")
                .addTokenFilter("lowercase")
                .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("pattern is missing for [_anonymous_charfilter] char filter of type 'pattern_replace'", e.getMessage());
    }

    public void testVerify_GivenCustomConfigWithInvalidTokenizer() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .addCharFilter(ignoreStuffInSqaureBrackets)
                .setTokenizer("oops!")
                .addTokenFilter("lowercase")
                .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("Failed to find global tokenizer under [oops!]", e.getMessage());
    }

    public void testVerify_GivenNoTokenizer() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        Map<String, Object> ignoreStuffThatBeginsWithADigit = new HashMap<>();
        ignoreStuffThatBeginsWithADigit.put("type", "pattern_replace");
        ignoreStuffThatBeginsWithADigit.put("pattern", "^[0-9].*");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .addCharFilter(ignoreStuffInSqaureBrackets)
                .addTokenFilter("lowercase")
                .addTokenFilter(ignoreStuffThatBeginsWithADigit)
                .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("categorization_analyzer that is not a global analyzer must specify a [tokenizer] field", e.getMessage());
    }

    public void testVerify_GivenCustomConfigWithInvalidTokenFilter() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .addCharFilter(ignoreStuffInSqaureBrackets)
                .setTokenizer("classic")
                .addTokenFilter("lowercase")
                .addTokenFilter("oh dear!");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("Failed to find global token filter under [oh dear!]", e.getMessage());
    }

    public void testVerify_GivenCustomConfigWithMisconfiguredTokenFilter() {
        Map<String, Object> noPattern = new HashMap<>();
        noPattern.put("type", "pattern_replace");
        noPattern.put("attern", "should have been pattern");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .addCharFilter("html_strip")
                .setTokenizer("classic")
                .addTokenFilter("lowercase")
                .addTokenFilter(noPattern);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("pattern is missing for [_anonymous_tokenfilter] token filter of type 'pattern_replace'", e.getMessage());
    }

    public void testVerify_GivenAnalyzerAndCharFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .setAnalyzer("standard")
                .addCharFilter("html_strip");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [char_filter] field", e.getMessage());
    }

    public void testVerify_GivenAnalyzerAndTokenizer() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .setAnalyzer("standard")
                .setTokenizer("classic");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [tokenizer] field", e.getMessage());
    }

    public void testVerify_GivenAnalyzerAndTokenFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
                .setAnalyzer("standard")
                .addTokenFilter("lowercase");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.verify(analysisRegistry, environment));
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [filter] field", e.getMessage());
    }

    @Override
    protected CategorizationAnalyzerConfig createTestInstance() {
        return createRandomized().build();
    }

    public static CategorizationAnalyzerConfig.Builder createRandomized() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder();
        if (rarely()) {
            builder.setAnalyzer(randomAlphaOfLength(10));
        } else {
            if (randomBoolean()) {
                for (String pattern : generateRandomStringArray(3, 40, false)) {
                    if (rarely()) {
                        builder.addCharFilter(randomAlphaOfLength(10));
                    } else {
                        Map<String, Object> charFilter = new HashMap<>();
                        charFilter.put("type", "pattern_replace");
                        charFilter.put("pattern", pattern);
                        builder.addCharFilter(charFilter);
                    }
                }
            }

            if (rarely()) {
                builder.setTokenizer(randomAlphaOfLength(10));
            } else {
                Map<String, Object> tokenizer = new HashMap<>();
                tokenizer.put("type", "pattern");
                tokenizer.put("pattern", randomAlphaOfLength(10));
                builder.setTokenizer(tokenizer);
            }

            if (randomBoolean()) {
                for (String pattern : generateRandomStringArray(4, 40, false)) {
                    if (rarely()) {
                        builder.addTokenFilter(randomAlphaOfLength(10));
                    } else {
                        Map<String, Object> tokenFilter = new HashMap<>();
                        tokenFilter.put("type", "pattern_replace");
                        tokenFilter.put("pattern", pattern);
                        builder.addTokenFilter(tokenFilter);
                    }
                }
            }
        }
        return builder;
    }

    @Override
    protected Writeable.Reader<CategorizationAnalyzerConfig> instanceReader() {
        return CategorizationAnalyzerConfig::new;
    }

    @Override
    protected CategorizationAnalyzerConfig doParseInstance(XContentParser parser) throws IOException {
        return CategorizationAnalyzerConfig.buildFromXContentObject(parser, MlParserType.CONFIG);
    }

    @Override
    protected CategorizationAnalyzerConfig mutateInstance(CategorizationAnalyzerConfig instance) {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder(instance);

        if (instance.getAnalyzer() != null) {
            builder.setAnalyzer(instance.getAnalyzer() + "mutated");
        } else {
            if (randomBoolean()) {
                builder.addCharFilter(randomAlphaOfLengthBetween(1, 20));
            } else {
                builder.addTokenFilter(randomAlphaOfLengthBetween(1, 20));
            }
        }
        return builder.build();
    }
}
