/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import static org.hamcrest.CoreMatchers.equalTo;

public class InferenceExceptionTests extends ESTestCase {
    public void testWrapException() throws Exception {
        ElasticsearchStatusException cause = new ElasticsearchStatusException("test", RestStatus.BAD_REQUEST);
        InferenceException testException = new InferenceException("test wrapper", cause);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        testException.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertThat(
            Strings.toString(builder),
            equalTo(
                "{\"type\":\"inference_exception\",\"reason\":\"test wrapper\","
                    + "\"caused_by\":{\"type\":\"status_exception\",\"reason\":\"test\"}}"
            )
        );
        assertThat(testException.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testNullCause() throws Exception {
        InferenceException testException = new InferenceException("test exception", null);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        testException.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertThat(Strings.toString(builder), equalTo("{\"type\":\"inference_exception\",\"reason\":\"test exception\"}"));
        assertThat(testException.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }
}
