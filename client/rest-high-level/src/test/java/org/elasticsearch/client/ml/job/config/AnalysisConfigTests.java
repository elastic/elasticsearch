/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalysisConfigTests extends AbstractXContentTestCase<AnalysisConfig> {

    public static AnalysisConfig.Builder createRandomized() {
        boolean isCategorization = randomBoolean();
        List<Detector> detectors = new ArrayList<>();
        int numDetectors = randomIntBetween(1, 10);
        for (int i = 0; i < numDetectors; i++) {
            Detector.Builder builder = new Detector.Builder("count", null);
            if (isCategorization) {
                builder.setByFieldName("mlcategory");
            }
            builder.setPartitionFieldName("part");
            detectors.add(builder.build());
        }
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(detectors);
        if (randomBoolean()) {
            TimeValue bucketSpan = TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000));
            builder.setBucketSpan(bucketSpan);
        }
        if (isCategorization) {
            builder.setCategorizationFieldName(randomAlphaOfLength(10));
            if (randomBoolean()) {
                builder.setCategorizationFilters(Arrays.asList(generateRandomStringArray(10, 10, false)));
            } else {
                CategorizationAnalyzerConfig.Builder analyzerBuilder = new CategorizationAnalyzerConfig.Builder();
                if (rarely()) {
                    analyzerBuilder.setAnalyzer(randomAlphaOfLength(10));
                } else {
                    if (randomBoolean()) {
                        for (String pattern : generateRandomStringArray(3, 40, false)) {
                            Map<String, Object> charFilter = new HashMap<>();
                            charFilter.put("type", "pattern_replace");
                            charFilter.put("pattern", pattern);
                            analyzerBuilder.addCharFilter(charFilter);
                        }
                    }

                    Map<String, Object> tokenizer = new HashMap<>();
                    tokenizer.put("type", "pattern");
                    tokenizer.put("pattern", randomAlphaOfLength(10));
                    analyzerBuilder.setTokenizer(tokenizer);

                    if (randomBoolean()) {
                        for (String pattern : generateRandomStringArray(4, 40, false)) {
                            Map<String, Object> tokenFilter = new HashMap<>();
                            tokenFilter.put("type", "pattern_replace");
                            tokenFilter.put("pattern", pattern);
                            analyzerBuilder.addTokenFilter(tokenFilter);
                        }
                    }
                }
                builder.setCategorizationAnalyzerConfig(analyzerBuilder.build());
            }
            if (randomBoolean()) {
                boolean enabled = randomBoolean();
                builder.setPerPartitionCategorizationConfig(
                    new PerPartitionCategorizationConfig(enabled, enabled && randomBoolean()));
            }
        }
        if (randomBoolean()) {
            builder.setLatency(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            builder.setMultivariateByFields(randomBoolean());
        }

        builder.setInfluencers(Arrays.asList(generateRandomStringArray(10, 10, false)));
        return builder;
    }

    @Override
    protected AnalysisConfig createTestInstance() {
        return createRandomized().build();
    }

    @Override
    protected AnalysisConfig doParseInstance(XContentParser parser) {
        return AnalysisConfig.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testBuilder_WithNullDetectors() {
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(new ArrayList<>());
        NullPointerException ex = expectThrows(NullPointerException.class, () ->  builder.setDetectors(null));
        assertEquals("[detectors] must not be null", ex.getMessage());
    }

    public void testEquals_GivenSameReference() {
        AnalysisConfig config = createRandomized().build();
        assertTrue(config.equals(config));
    }

    public void testEquals_GivenDifferentClass() {
        assertFalse(createRandomized().build().equals("a string"));
    }

    public void testEquals_GivenNull() {
        assertFalse(createRandomized().build().equals(null));
    }

    public void testEquals_GivenEqualConfig() {
        AnalysisConfig config1 = createValidCategorizationConfig().build();
        AnalysisConfig config2 = createValidCategorizationConfig().build();

        assertTrue(config1.equals(config2));
        assertTrue(config2.equals(config1));
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testEquals_GivenDifferentBucketSpan() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setBucketSpan(TimeValue.timeValueSeconds(1800));
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setBucketSpan(TimeValue.timeValueHours(1));
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenCategorizationField() {
        AnalysisConfig.Builder builder = createValidCategorizationConfig();
        builder.setCategorizationFieldName("foo");
        AnalysisConfig config1 = builder.build();

        builder = createValidCategorizationConfig();
        builder.setCategorizationFieldName("bar");
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentDetector() {
        AnalysisConfig config1 = createConfigWithDetectors(Collections.singletonList(new Detector.Builder("min", "low_count").build()));

        AnalysisConfig config2 = createConfigWithDetectors(Collections.singletonList(new Detector.Builder("min", "high_count").build()));

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentInfluencers() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setInfluencers(Collections.singletonList("foo"));
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setInfluencers(Collections.singletonList("bar"));
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentLatency() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setLatency(TimeValue.timeValueSeconds(1800));
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setLatency(TimeValue.timeValueSeconds(1801));
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenSummaryCountField() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setSummaryCountFieldName("foo");
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setSummaryCountFieldName("bar");
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenMultivariateByField() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setMultivariateByFields(true);
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setMultivariateByFields(false);
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentCategorizationFilters() {
        AnalysisConfig.Builder configBuilder1 = createValidCategorizationConfig();
        AnalysisConfig.Builder configBuilder2 = createValidCategorizationConfig();
        configBuilder1.setCategorizationFilters(Arrays.asList("foo", "bar"));
        configBuilder2.setCategorizationFilters(Arrays.asList("foo", "foobar"));
        AnalysisConfig config1 = configBuilder1.build();
        AnalysisConfig config2 = configBuilder2.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    private static AnalysisConfig createConfigWithDetectors(List<Detector> detectors) {
        return new AnalysisConfig.Builder(detectors).build();
    }

    private static AnalysisConfig.Builder createConfigBuilder() {
        return new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("min", "count").build()));
    }

    private static AnalysisConfig.Builder createValidCategorizationConfig() {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("mlcategory");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        analysisConfig.setLatency(TimeValue.ZERO);
        analysisConfig.setCategorizationFieldName("msg");
        return analysisConfig;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }
}
