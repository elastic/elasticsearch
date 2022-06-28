/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
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
        return new NamedXContentRegistry(
            CollectionUtils.appendToCopy(
                ClusterModule.getNamedXWriteables(),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse)
            )
        );
    }
}
