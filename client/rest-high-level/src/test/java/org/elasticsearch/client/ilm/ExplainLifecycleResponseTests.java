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
package org.elasticsearch.client.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExplainLifecycleResponseTests extends AbstractXContentTestCase<ExplainLifecycleResponse> {

    @Override
    protected ExplainLifecycleResponse createTestInstance() {
        Map<String, IndexLifecycleExplainResponse> indexResponses = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            IndexLifecycleExplainResponse indexResponse = IndexLifecycleExplainResponseTests.randomIndexExplainResponse();
            indexResponses.put(indexResponse.getIndex(), indexResponse);
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

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        return new NamedXContentRegistry(entries);
    }
}
