/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UnifiedChatCompletionExceptionTests extends ESTestCase {

    public void testFromThrowable_NullMessage_DoesNotThrow() {
        var cause = new IOException((String) null);
        assertThat(cause.getMessage(), nullValue());

        var exception = UnifiedChatCompletionException.fromThrowable(cause);

        assertThat(exception.getMessage(), notNullValue());
        assertThat(exception.getMessage(), containsString("i_o_exception"));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testFromThrowable_PreservesMessage() {
        var exception = UnifiedChatCompletionException.fromThrowable(new IOException("boom"));

        assertThat(exception.getMessage(), is("boom"));
    }
}
