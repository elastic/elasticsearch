/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

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
