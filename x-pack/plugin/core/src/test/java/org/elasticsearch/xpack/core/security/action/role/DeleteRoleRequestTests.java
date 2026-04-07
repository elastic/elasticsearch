/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DeleteRoleRequestTests extends ESTestCase {

    public void testSetRefreshPolicy() {
        final DeleteRoleRequest request = new DeleteRoleRequest();
        final String refreshPolicy = randomFrom(
            WriteRequest.RefreshPolicy.IMMEDIATE.getValue(),
            WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue()
        );
        request.setRefreshPolicy(refreshPolicy);
        assertThat(request.getRefreshPolicy().getValue(), equalTo(refreshPolicy));

        request.setRefreshPolicy((String) null);
        assertThat(request.getRefreshPolicy().getValue(), equalTo(refreshPolicy));
    }
}
