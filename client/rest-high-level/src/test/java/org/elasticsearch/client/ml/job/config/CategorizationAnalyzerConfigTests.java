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
