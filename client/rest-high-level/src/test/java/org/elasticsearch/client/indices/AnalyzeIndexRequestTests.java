/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class AnalyzeIndexRequestTests extends AnalyzeRequestTests {

    @Override
    protected AnalyzeRequest createClientTestInstance() {
        int option = random().nextInt(5);
        switch (option) {
            case 0:
                return AnalyzeRequest.withField("index", "field", "some text", "some more text");
            case 1:
                return AnalyzeRequest.withIndexAnalyzer("index", "my_analyzer", "some text", "some more text");
            case 2:
                return AnalyzeRequest.withNormalizer("index", "my_normalizer", "text", "more text");
            case 3:
                return AnalyzeRequest.buildCustomAnalyzer("index", "my_tokenizer")
                    .addCharFilter("my_char_filter")
                    .addCharFilter(Map.of("type", "html_strip"))
                    .addTokenFilter("my_token_filter")
                    .addTokenFilter(Map.of("type", "synonym"))
                    .build("some text", "some more text");
            case 4:
                return AnalyzeRequest.buildCustomNormalizer("index")
                    .addCharFilter("my_char_filter")
                    .addCharFilter(Map.of("type", "html_strip"))
                    .addTokenFilter("my_token_filter")
                    .addTokenFilter(Map.of("type", "synonym"))
                    .build("some text", "some more text");
        }
        throw new IllegalStateException("nextInt(5) has returned a value greater than 4");
    }

    @Override
    protected AnalyzeAction.Request doParseToServerInstance(XContentParser parser) throws IOException {
        return AnalyzeAction.Request.fromXContent(parser, "index");
    }
}
