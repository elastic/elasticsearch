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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class AnalyzeResponseTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testNullResponseToXContent() throws IOException {
        AnalyzeAction.CharFilteredText[] charfilters = null;

        String name = "test_tokens_null";
        AnalyzeAction.AnalyzeToken[] tokens = null;
        AnalyzeAction.AnalyzeTokenList tokenizer = null;


        AnalyzeAction.AnalyzeTokenList tokenfiltersItem = new AnalyzeAction.AnalyzeTokenList(name, tokens);
        AnalyzeAction.AnalyzeTokenList[] tokenfilters = {tokenfiltersItem};

        AnalyzeAction.DetailAnalyzeResponse detail = new AnalyzeAction.DetailAnalyzeResponse(charfilters, tokenizer, tokenfilters);

        AnalyzeAction.Response response = new AnalyzeAction.Response(null, detail);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Map<String, Object> converted = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            List<Map<String, Object>> tokenfiltersValue = (List<Map<String, Object>>) ((Map<String, Object>)
                converted.get("detail")).get("tokenfilters");
            List<Map<String, Object>> nullTokens = (List<Map<String, Object>>) tokenfiltersValue.get(0).get("tokens");
            String nameValue = (String) tokenfiltersValue.get(0).get("name");
            assertThat(nullTokens.size(), equalTo(0));
            assertThat(name, equalTo(nameValue));
        }

    }
}
