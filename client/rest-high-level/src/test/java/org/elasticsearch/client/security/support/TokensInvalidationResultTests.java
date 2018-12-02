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
package org.elasticsearch.client.security.support;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TokensInvalidationResultTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String json = "{\"invalidated_tokens\":2," +
            "\"previously_invalidated_tokens\":2," +
            "\"error_size\":0" +
            "}";
        final TokensInvalidationResult result = TokensInvalidationResult.fromXContent(XContentType.JSON.xContent().createParser(
            new NamedXContentRegistry(Collections.emptyList()), new DeprecationHandler() {
                @Override
                public void usedDeprecatedName(String usedName, String modernName) {
                }

                @Override
                public void usedDeprecatedField(String usedName, String replacedWith) {
                }
            }, json));
        final TokensInvalidationResult expectedResult = new TokensInvalidationResult(2, 2, null);
        assertThat(result, equalTo(expectedResult));
    }

    public void testFromXContentWithErrors() throws IOException {
        final String json =
            "   {\"invalidated_tokens\":2," +
                "\"previously_invalidated_tokens\":2," +
                "\"error_size\":2," +
                "\"error_details\":[" +
                "  {\"type\":\"exception\"," +
                "    \"reason\":\"foo\"," +
                "    \"caused_by\":{" +
                "      \"type\":\"illegal_state_exception\"," +
                "      \"reason\":\"bar\"" +
                "    }" +
                "  }," +
                "  {\"type\":\"exception\"," +
                "    \"reason\":\"boo\"," +
                "    \"caused_by\":{" +
                "      \"type\":\"illegal_state_exception\"," +
                "      \"reason\":\"far\"" +
                "    }" +
                "  }" +
                "]" +
                "}";
        final TokensInvalidationResult result = TokensInvalidationResult.fromXContent(XContentType.JSON.xContent().createParser(
            new NamedXContentRegistry(Collections.emptyList()), new DeprecationHandler() {
                @Override
                public void usedDeprecatedName(String usedName, String modernName) {
                }

                @Override
                public void usedDeprecatedField(String usedName, String replacedWith) {
                }
            }, json));
        assertThat(result.getInvalidatedTokens(), equalTo(2));
        assertThat(result.getPreviouslyInvalidatedTokens(), equalTo(2));
        assertThat(result.getErrors().size(), equalTo(2));
        assertThat(result.getErrors().get(0).toString(), containsString("type=exception, reason=foo"));
        assertThat(result.getErrors().get(0).toString(), containsString("type=illegal_state_exception, reason=bar"));
        assertThat(result.getErrors().get(1).toString(), containsString("type=exception, reason=boo"));
        assertThat(result.getErrors().get(1).toString(), containsString("type=illegal_state_exception, reason=far"));
    }
}
