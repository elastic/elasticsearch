/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class CategorizationAnalyzerConfigTests extends AbstractSerializingTestCase<CategorizationAnalyzerConfig> {

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

    public void testAsMap() throws IOException {
        Map<String, Object> map = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(Collections.emptyList())
            .asMap(NamedXContentRegistry.EMPTY);
        @SuppressWarnings("unchecked")
        Map<String, Object> firstLevel =
            (Map<String, Object>) map.get(CategorizationAnalyzerConfig.CATEGORIZATION_ANALYZER.getPreferredName());
        assertThat(firstLevel, not(nullValue()));
        String tokenizer = (String) firstLevel.get(CategorizationAnalyzerConfig.TOKENIZER.getPreferredName());
        assertThat(tokenizer, is("ml_classic"));
    }

    @Override
    protected Writeable.Reader<CategorizationAnalyzerConfig> instanceReader() {
        return CategorizationAnalyzerConfig::new;
    }

    @Override
    protected CategorizationAnalyzerConfig doParseInstance(XContentParser parser) throws IOException {
        return CategorizationAnalyzerConfig.buildFromXContentObject(parser, false);
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
