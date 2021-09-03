/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class VerifyRepositoryResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            VerifyRepositoryResponseTests::createTestInstance,
            VerifyRepositoryResponseTests::toXContent,
            VerifyRepositoryResponse::fromXContent)
            .supportsUnknownFields(true)
            .shuffleFieldsExceptions(new String[] {"nodes"}) // do not mix up the order of nodes, it will cause the tests to fail
            .randomFieldsExcludeFilter((f) -> f.equals("nodes")) // everything in nodes needs to be a particular parseable object
            .assertToXContentEquivalence(false)
            .test();
    }

    private static VerifyRepositoryResponse createTestInstance() {
        List<VerifyRepositoryResponse.NodeView> nodes = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            nodes.add(new VerifyRepositoryResponse.NodeView(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        }

        return new VerifyRepositoryResponse(nodes);
    }

    private static XContentBuilder toXContent(VerifyRepositoryResponse response, XContentBuilder builder) throws IOException {
        return response.toXContent(builder, ToXContent.EMPTY_PARAMS);
    }
}
