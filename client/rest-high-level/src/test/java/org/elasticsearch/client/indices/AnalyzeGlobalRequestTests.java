/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AnalyzeGlobalRequestTests extends AnalyzeRequestTests {

    private static final Map<String, Object> charFilterConfig = new HashMap<>();
    static {
        charFilterConfig.put("type", "html_strip");
    }

    private static final Map<String, Object> tokenFilterConfig = new HashMap<>();
    static {
        tokenFilterConfig.put("type", "synonym");
    }

    @Override
    protected AnalyzeRequest createClientTestInstance() {
        int option = random().nextInt(3);
        switch (option) {
            case 0:
                return AnalyzeRequest.withGlobalAnalyzer("my_analyzer", "some text", "some more text");
            case 1:
                return AnalyzeRequest.buildCustomAnalyzer("my_tokenizer")
                    .addCharFilter("my_char_filter")
                    .addCharFilter(charFilterConfig)
                    .addTokenFilter("my_token_filter")
                    .addTokenFilter(tokenFilterConfig)
                    .build("some text", "some more text");
            case 2:
                return AnalyzeRequest.buildCustomNormalizer()
                    .addCharFilter("my_char_filter")
                    .addCharFilter(charFilterConfig)
                    .addTokenFilter("my_token_filter")
                    .addTokenFilter(tokenFilterConfig)
                    .build("some text", "some more text");
        }
        throw new IllegalStateException("nextInt(3) has returned a value greater than 2");
    }

    @Override
    protected AnalyzeAction.Request doParseToServerInstance(XContentParser parser) throws IOException {
        return AnalyzeAction.Request.fromXContent(parser, null);
    }
}
