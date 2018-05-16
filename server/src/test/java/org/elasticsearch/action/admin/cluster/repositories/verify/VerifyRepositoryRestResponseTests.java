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
package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VerifyRepositoryRestResponseTests extends AbstractXContentTestCase<VerifyRepositoryRestResponse> {

    @Override
    protected VerifyRepositoryRestResponse doParseInstance(XContentParser parser) throws IOException {
        return VerifyRepositoryRestResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected VerifyRepositoryRestResponse createTestInstance() {
        VerifyRepositoryRestResponse response = new VerifyRepositoryRestResponse();
        List<VerifyRepositoryRestResponse.NodeView> nodes = new ArrayList<>();
        nodes.add(new VerifyRepositoryRestResponse.NodeView("node-id", "node-name"));
        response.nodes(nodes);
        return response;
    }
}
