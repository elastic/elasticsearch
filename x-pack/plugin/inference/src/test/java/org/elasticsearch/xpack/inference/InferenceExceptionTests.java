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

    /**
     * An internal model-deployment failure (e.g., model stopped/process crashed during load)
     * is represented as SERVICE_UNAVAILABLE and must NOT collapse to BAD_REQUEST.
     * This regression-guards the fix for internal failures that were previously wrapped
     * as IllegalArgumentException and inadvertently surfaced as 400.
     */
    public void testServiceUnavailableCausePropagatesCorrectly() {
        ElasticsearchStatusException cause = new ElasticsearchStatusException(
            "model stopped before it is started",
            RestStatus.SERVICE_UNAVAILABLE
        );
        InferenceException testException = new InferenceException(
            "Exception when running inference id [.elser-2-elasticsearch] on field [sparse_field]",
            cause
        );

        assertThat(testException.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    /**
     * A plain IllegalArgumentException cause (e.g., wrong model type / invalid user input)
     * should still produce BAD_REQUEST so that legitimate client errors are not hidden.
     */
    public void testIllegalArgumentCauseReturnsBadRequest() {
        IllegalArgumentException cause = new IllegalArgumentException("must be a pytorch model");
        InferenceException testException = new InferenceException(
            "Exception when running inference id [my-model] on field [my_field]",
            cause
        );

        assertThat(testException.status(), equalTo(RestStatus.BAD_REQUEST));
    }
}
