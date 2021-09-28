/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.List;

public class VerifyRepositoryResponseTests extends AbstractXContentTestCase<VerifyRepositoryResponse> {

    @Override
    protected VerifyRepositoryResponse doParseInstance(XContentParser parser) {
        return VerifyRepositoryResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected VerifyRepositoryResponse createTestInstance() {
        VerifyRepositoryResponse response = new VerifyRepositoryResponse();
        List<VerifyRepositoryResponse.NodeView> nodes = new ArrayList<>();
        nodes.add(new VerifyRepositoryResponse.NodeView("node-id", "node-name"));
        response.setNodes(nodes);
        return response;
    }
}
