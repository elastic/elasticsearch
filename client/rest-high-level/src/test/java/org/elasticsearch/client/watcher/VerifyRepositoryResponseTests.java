/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
