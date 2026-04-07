/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class QueryUserRequestTests extends ESTestCase {
    public void testValidate() {
        final QueryUserRequest request1 = new QueryUserRequest(
            null,
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            null,
            null,
            false
        );
        assertThat(request1.validate(), nullValue());

        final QueryUserRequest request2 = new QueryUserRequest(
            null,
            randomIntBetween(Integer.MIN_VALUE, -1),
            randomIntBetween(0, Integer.MAX_VALUE),
            null,
            null,
            false
        );
        assertThat(request2.validate().getMessage(), containsString("[from] parameter cannot be negative"));

        final QueryUserRequest request3 = new QueryUserRequest(
            null,
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(Integer.MIN_VALUE, -1),
            null,
            null,
            false
        );
        assertThat(request3.validate().getMessage(), containsString("[size] parameter cannot be negative"));
    }
}
