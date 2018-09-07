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
package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExplainLifecycleResponseTests extends AbstractStreamableXContentTestCase<ExplainLifecycleResponse> {

    @Override
    protected ExplainLifecycleResponse createTestInstance() {
        Map<String, IndexLifecycleExplainResponse> indexResponses = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            IndexLifecycleExplainResponse indexResponse = IndexExplainResponseTests.randomIndexExplainResponse();
            indexResponses.put(indexResponse.getIndex(), indexResponse);
        }
        return new ExplainLifecycleResponse(indexResponses);
    }

    @Override
    protected ExplainLifecycleResponse createBlankInstance() {
        return new ExplainLifecycleResponse();
    }

    @Override
    protected ExplainLifecycleResponse mutateInstance(ExplainLifecycleResponse response) {
        Map<String, IndexLifecycleExplainResponse> indexResponses = new HashMap<>(response.getIndexResponses());
        IndexLifecycleExplainResponse indexResponse = IndexExplainResponseTests.randomIndexExplainResponse();
        indexResponses.put(indexResponse.getIndex(), indexResponse);
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
