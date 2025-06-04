/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class TokensInvalidationResultTests extends ESTestCase {

    public void testToXcontent() throws Exception {
        TokensInvalidationResult result = new TokensInvalidationResult(
            Arrays.asList("token1", "token2"),
            Arrays.asList("token3", "token4"),
            Arrays.asList(
                new ElasticsearchException("foo", new IllegalStateException("bar")),
                new ElasticsearchException("boo", new IllegalStateException("far"))
            ),
            RestStatus.OK
        );

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder), equalTo(XContentHelper.stripWhitespace("""
                {
                  "invalidated_tokens": 2,
                  "previously_invalidated_tokens": 2,
                  "error_count": 2,
                  "error_details": [
                    {
                      "type": "exception",
                      "reason": "foo",
                      "caused_by": {
                        "type": "illegal_state_exception",
                        "reason": "bar"
                      }
                    },
                    {
                      "type": "exception",
                      "reason": "boo",
                      "caused_by": {
                        "type": "illegal_state_exception",
                        "reason": "far"
                      }
                    }
                  ]
                }""")));
        }
    }

    public void testToXcontentWithNoErrors() throws Exception {
        TokensInvalidationResult result = new TokensInvalidationResult(
            Arrays.asList("token1", "token2"),
            Collections.emptyList(),
            Collections.emptyList(),
            RestStatus.OK
        );
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder), equalTo("""
                {"invalidated_tokens":2,"previously_invalidated_tokens":0,"error_count":0}"""));
        }
    }
}
