/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CategorizationAnalyzerConfigTests extends AbstractXContentTestCase<CategorizationAnalyzerConfig> {

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
    protected CategorizationAnalyzerConfig doParseInstance(XContentParser parser) throws IOException {
        return CategorizationAnalyzerConfig.buildFromXContentObject(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
