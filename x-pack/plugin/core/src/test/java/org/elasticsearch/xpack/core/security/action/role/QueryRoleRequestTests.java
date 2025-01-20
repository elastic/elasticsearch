/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class QueryRoleRequestTests extends ESTestCase {
    public void testValidate() {
        final QueryRoleRequest request1 = new QueryRoleRequest(
            null,
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            null,
            null
        );
        assertThat(request1.validate(), nullValue());

        final QueryRoleRequest request2 = new QueryRoleRequest(
            null,
            randomIntBetween(Integer.MIN_VALUE, -1),
            randomIntBetween(0, Integer.MAX_VALUE),
            null,
            null
        );
        assertThat(request2.validate().getMessage(), containsString("[from] parameter cannot be negative"));

        final QueryRoleRequest request3 = new QueryRoleRequest(
            null,
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(Integer.MIN_VALUE, -1),
            null,
            null
        );
        assertThat(request3.validate().getMessage(), containsString("[size] parameter cannot be negative"));
    }
}
