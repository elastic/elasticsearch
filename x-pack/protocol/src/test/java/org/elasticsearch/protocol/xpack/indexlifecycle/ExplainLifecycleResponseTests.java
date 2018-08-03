/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
