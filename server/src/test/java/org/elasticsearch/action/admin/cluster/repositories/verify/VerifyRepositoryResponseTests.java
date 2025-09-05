/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.List;

public class VerifyRepositoryResponseTests extends AbstractXContentTestCase<VerifyRepositoryResponse> {

    private static final ObjectParser<VerifyRepositoryResponse, Void> PARSER = new ObjectParser<>(
        VerifyRepositoryResponse.class.getName(),
        true,
        VerifyRepositoryResponse::new
    );
    static {
        ObjectParser<VerifyRepositoryResponse.NodeView, Void> internalParser = new ObjectParser<>(
            VerifyRepositoryResponse.NODES,
            true,
            null
        );
        internalParser.declareString(VerifyRepositoryResponse.NodeView::setName, new ParseField(VerifyRepositoryResponse.NAME));
        PARSER.declareNamedObjects(
            VerifyRepositoryResponse::setNodes,
            (p, v, name) -> internalParser.parse(p, new VerifyRepositoryResponse.NodeView(name), null),
            new ParseField("nodes")
        );
    }

    @Override
    protected VerifyRepositoryResponse doParseInstance(XContentParser parser) {
        return PARSER.apply(parser, null);
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
