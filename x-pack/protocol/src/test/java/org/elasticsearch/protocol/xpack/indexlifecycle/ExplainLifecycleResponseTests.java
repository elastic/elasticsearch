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
package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ExplainLifecycleResponseTests extends AbstractStreamableXContentTestCase<ExplainLifecycleResponse> {

    @Override
    protected ExplainLifecycleResponse createTestInstance() {
        Set<IndexLifecycleExplainResponse> indexResponses = new HashSet<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            indexResponses.add(IndexExplainResponseTests.randomIndexExplainResponse());
        }
        return new ExplainLifecycleResponse(indexResponses);
    }

    @Override
    protected ExplainLifecycleResponse createBlankInstance() {
        return new ExplainLifecycleResponse();
    }

    @Override
    protected ExplainLifecycleResponse mutateInstance(ExplainLifecycleResponse response) {
        Set<IndexLifecycleExplainResponse> indexResponses = new HashSet<>(response.getIndexResponses());
        if (indexResponses.size() > 0) {
            indexResponses.add(IndexExplainResponseTests.randomIndexExplainResponse());
        } else {
            indexResponses.add(IndexExplainResponseTests.randomIndexExplainResponse());
        }
        return new ExplainLifecycleResponse(indexResponses);
    }

    @Override
    protected ExplainLifecycleResponse doParseInstance(XContentParser parser) throws IOException {
        return ExplainLifecycleResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
