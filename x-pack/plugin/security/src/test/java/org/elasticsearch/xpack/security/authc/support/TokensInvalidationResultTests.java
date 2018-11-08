/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;

import static org.hamcrest.Matchers.equalTo;

public class TokensInvalidationResultTests extends ESTestCase {

    public void testToXcontent() throws Exception{
        TokensInvalidationResult result =
            new TokensInvalidationResult(
                new String[]{"tokens1", "tokens2"}, new String[]{"tokens3", "tokens4"}, new String[]{"error1", "error2"}, 0);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder),
                equalTo(
                    "{\"invalidated_tokens\":2," +
                             "\"prev_invalidated_tokens\":2," +
                             "\"errors\":" +
                               "{\"size\":2," +
                                 "\"error_messages\":" +
                                 "[\"error1\"," +
                                   "\"error2\"]" +
                               "}" +
                            "}"));
        }
    }

    public void testToXcontentWithNoErrors() throws Exception{
        TokensInvalidationResult result =
            new TokensInvalidationResult(
                new String[]{"tokens1", "tokens2"}, new String[]{"tokens3", "tokens4"}, new String[0], 0);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder),
                equalTo(
                    "{\"invalidated_tokens\":2," +
                             "\"prev_invalidated_tokens\":2," +
                             "\"errors\":" +
                               "{\"size\":0}" +
                             "}"));
        }
    }
}
