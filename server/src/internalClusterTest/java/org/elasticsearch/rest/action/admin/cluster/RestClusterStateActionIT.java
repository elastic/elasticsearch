/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_PARAM;

public class RestClusterStateActionIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testInfiniteTimeOut() throws IOException {
        final var request = new Request("GET", "/_cluster/state/none");
        request.addParameter(REST_MASTER_TIMEOUT_PARAM, "-1");
        getRestClient().performRequest(request);
    }
}
