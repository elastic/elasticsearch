/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.ApiKeysInvalidationResult;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class ApiKeysInvalidationResultTests extends ESTestCase {

    public void testToXcontent() throws Exception {
        ApiKeysInvalidationResult result = new ApiKeysInvalidationResult(Arrays.asList("api-key-id-1"),
                Arrays.asList("api-key-id-2", "api-key-id-3"),
                Arrays.asList(new ElasticsearchException("foo", new IllegalStateException("bar")),
                        new ElasticsearchException("boo", new IllegalStateException("far"))),
                randomIntBetween(0, 5));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder),
                    equalTo("{\"invalidated_api_keys\":[\"api-key-id-1\"],"
                            + "\"previously_invalidated_api_keys\":[\"api-key-id-2\",\"api-key-id-3\"],"
                            + "\"error_count\":2,"
                            + "\"error_details\":["
                            + "{\"type\":\"exception\","
                            + "\"reason\":\"foo\","
                            + "\"caused_by\":{"
                            + "\"type\":\"illegal_state_exception\","
                            + "\"reason\":\"bar\""
                            + "}"
                            + "},"
                            + "{\"type\":\"exception\","
                            + "\"reason\":\"boo\","
                            + "\"caused_by\":{"
                            + "\"type\":\"illegal_state_exception\","
                            + "\"reason\":\"far\""
                            + "}"
                            + "}"
                            + "]"
                            + "}"));
        }
    }

    public void testToXcontentWithNoErrors() throws Exception{
        ApiKeysInvalidationResult result = new ApiKeysInvalidationResult(Arrays.asList("api-key-1", "api-key-2"),
            Collections.emptyList(),
            Collections.emptyList(), randomIntBetween(0, 5));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder),
                equalTo(
                    "{\"invalidated_api_keys\":[\"api-key-1\",\"api-key-2\"]," +
                        "\"previously_invalidated_api_keys\":[]," +
                        "\"error_count\":0" +
                        "}"));
        }
    }

}
