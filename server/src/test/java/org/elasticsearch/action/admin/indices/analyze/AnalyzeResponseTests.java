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

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class AnalyzeResponseTests extends AbstractStreamableXContentTestCase<AnalyzeResponse> {

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return s -> s.contains("tokens.");
    }

    @Override
    protected AnalyzeResponse doParseInstance(XContentParser parser) throws IOException {
        return AnalyzeResponse.fromXContent(parser);
    }

    @Override
    protected AnalyzeResponse createBlankInstance() {
        return new AnalyzeResponse();
    }

    @Override
    protected AnalyzeResponse createTestInstance() {
        int tokenCount = randomIntBetween(1, 30);
        AnalyzeResponse.AnalyzeToken[] tokens = new AnalyzeResponse.AnalyzeToken[tokenCount];
        for (int i = 0; i < tokenCount; i++) {
            tokens[i] = randomToken();
        }
        DetailAnalyzeResponse dar = null;
        if (randomBoolean()) {
            dar = new DetailAnalyzeResponse();
            if (randomBoolean()) {
                dar.charfilters(new DetailAnalyzeResponse.CharFilteredText[]{
                    new DetailAnalyzeResponse.CharFilteredText("my_charfilter", new String[]{"one two"})
                });
            }
            dar.tokenizer(new DetailAnalyzeResponse.AnalyzeTokenList("my_tokenizer", tokens));
            if (randomBoolean()) {
                dar.tokenfilters(new DetailAnalyzeResponse.AnalyzeTokenList[]{
                    new DetailAnalyzeResponse.AnalyzeTokenList("my_tokenfilter_1", tokens),
                    new DetailAnalyzeResponse.AnalyzeTokenList("my_tokenfilter_2", tokens)
                });
            }
            return new AnalyzeResponse(null, dar);
        }
        return new AnalyzeResponse(Arrays.asList(tokens), null);
    }

    private AnalyzeResponse.AnalyzeToken randomToken() {
        String token = randomAlphaOfLengthBetween(1, 20);
        int position = randomIntBetween(0, 1000);
        int startOffset = randomIntBetween(0, 1000);
        int endOffset = randomIntBetween(0, 1000);
        int posLength = randomIntBetween(1, 5);
        String type = randomAlphaOfLengthBetween(1, 20);
        Map<String, Object> extras = new HashMap<>();
        if (randomBoolean()) {
            int entryCount = randomInt(6);
            for (int i = 0; i < entryCount; i++) {
                switch (randomInt(6)) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                        String key = randomAlphaOfLength(5);
                        String value = randomAlphaOfLength(10);
                        extras.put(key, value);
                        break;
                    case 4:
                        String objkey = randomAlphaOfLength(5);
                        Map<String, String> obj = new HashMap<>();
                        obj.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
                        extras.put(objkey, obj);
                        break;
                    case 5:
                        String listkey = randomAlphaOfLength(5);
                        List<String> list = new ArrayList<>();
                        list.add(randomAlphaOfLength(4));
                        list.add(randomAlphaOfLength(6));
                        extras.put(listkey, list);
                        break;
                }
            }
        }
        return new AnalyzeResponse.AnalyzeToken(token, position, startOffset, endOffset, posLength, type, extras);
    }
}
