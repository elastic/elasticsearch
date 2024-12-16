/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.synonyms.RestPutSynonymsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Map;

public class PutSynonymsActionTests extends ESTestCase {

    public void testEmptyRequestBody() throws Exception {
        RestPutSynonymsAction action = new RestPutSynonymsAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withParams(Map.of("synonymsSet", "test"))
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, randomBoolean(), 0);
        try (var threadPool = createThreadPool()) {
            final var nodeClient = new NoOpNodeClient(threadPool);
            expectThrows(IllegalArgumentException.class, () -> action.handleRequest(request, channel, nodeClient));
        }
    }
}
