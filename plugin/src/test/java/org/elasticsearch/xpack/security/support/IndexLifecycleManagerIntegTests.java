/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.security.action.user.PutUserResponse;

import java.util.ArrayList;
import java.util.List;

public class IndexLifecycleManagerIntegTests extends SecurityIntegTestCase {

    public void testConcurrentOperationsTryingToCreateSecurityIndexAndAlias() throws Exception {
        assertSecurityIndexWriteable();
        final int numRequests = scaledRandomIntBetween(4, 16);
        List<ActionFuture<PutUserResponse>> futures = new ArrayList<>(numRequests);
        List<PutUserRequest> requests = new ArrayList<>(numRequests);
        for (int i = 0; i < numRequests; i++) {
            requests.add(securityClient()
                    .preparePutUser("user" + i, "password".toCharArray(), randomAlphaOfLengthBetween(1, 16))
                    .request());
        }

        for (PutUserRequest request : requests) {
            PlainActionFuture<PutUserResponse> responsePlainActionFuture = new PlainActionFuture<>();
            securityClient().putUser(request, responsePlainActionFuture);
            futures.add(responsePlainActionFuture);
        }

        for (ActionFuture<PutUserResponse> future : futures) {
            assertTrue(future.actionGet().created());
        }
    }
}
