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

package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class AnalyzeGlobalRequestTests extends AnalyzeRequestTests {

    @Override
    protected AnalyzeRequest createClientTestInstance() {
        int option = random().nextInt(3);
        switch (option) {
            case 0:
                return AnalyzeRequest.withGlobalAnalyzer("my_analyzer", "some text", "some more text");
            case 1:
                return AnalyzeRequest.buildCustomAnalyzer("my_tokenizer")
                    .addCharFilter("my_char_filter")
                    .addCharFilter(Map.of("type", "html_strip"))
                    .addTokenFilter("my_token_filter")
                    .addTokenFilter(Map.of("type", "synonym"))
                    .build("some text", "some more text");
            case 2:
                return AnalyzeRequest.buildCustomNormalizer()
                    .addCharFilter("my_char_filter")
                    .addCharFilter(Map.of("type", "html_strip"))
                    .addTokenFilter("my_token_filter")
                    .addTokenFilter(Map.of("type", "synonym"))
                    .build("some text", "some more text");
        }
        throw new IllegalStateException("nextInt(3) has returned a value greater than 2");
    }

    @Override
    protected AnalyzeAction.Request doParseToServerInstance(XContentParser parser) throws IOException {
        return AnalyzeAction.Request.fromXContent(parser, null);
    }
}
