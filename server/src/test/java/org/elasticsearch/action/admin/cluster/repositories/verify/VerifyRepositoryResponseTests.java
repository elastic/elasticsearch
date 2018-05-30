package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VerifyRepositoryResponseTests  extends AbstractXContentTestCase<VerifyRepositoryResponse> {

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
