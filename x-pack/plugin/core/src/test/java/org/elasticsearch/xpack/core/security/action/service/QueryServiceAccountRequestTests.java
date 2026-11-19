/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class QueryServiceAccountRequestTests extends ESTestCase {

    public void testValidate() {
        final QueryServiceAccountRequest valid = new QueryServiceAccountRequest(
            null,
            randomFrom(randomIntBetween(0, Integer.MAX_VALUE), null),
            randomFrom(randomIntBetween(0, Integer.MAX_VALUE), null),
            null,
            null
        );
        assertThat(valid.validate(), nullValue());

        final QueryServiceAccountRequest negativeFrom = new QueryServiceAccountRequest(
            null,
            randomIntBetween(Integer.MIN_VALUE, -1),
            randomIntBetween(0, Integer.MAX_VALUE),
            null,
            null
        );
        assertThat(negativeFrom.validate().getMessage(), containsString("[from] parameter cannot be negative"));

        final QueryServiceAccountRequest negativeSize = new QueryServiceAccountRequest(
            null,
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(Integer.MIN_VALUE, -1),
            null,
            null
        );
        assertThat(negativeSize.validate().getMessage(), containsString("[size] parameter cannot be negative"));

        final ActionRequestValidationException both = new QueryServiceAccountRequest(null, -1, -1, null, null).validate();
        assertThat(both.getMessage(), containsString("[from] parameter cannot be negative"));
        assertThat(both.getMessage(), containsString("[size] parameter cannot be negative"));
    }
}
