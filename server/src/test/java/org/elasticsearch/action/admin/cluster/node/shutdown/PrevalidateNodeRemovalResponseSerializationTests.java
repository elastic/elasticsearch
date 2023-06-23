/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PrevalidateNodeRemovalResponseSerializationTests extends AbstractXContentSerializingTestCase<PrevalidateNodeRemovalResponse> {

    @Override
    protected Writeable.Reader<PrevalidateNodeRemovalResponse> instanceReader() {
        return PrevalidateNodeRemovalResponse::new;
    }

    @Override
    protected PrevalidateNodeRemovalResponse createTestInstance() {
        return new PrevalidateNodeRemovalResponse(NodesRemovalPrevalidationSerializationTests.randomNodesRemovalPrevalidation());
    }

    @Override
    protected PrevalidateNodeRemovalResponse mutateInstance(PrevalidateNodeRemovalResponse instance) {
        return new PrevalidateNodeRemovalResponse(NodesRemovalPrevalidationSerializationTests.mutateNodes(instance.getPrevalidation()));
    }

    @Override
    protected PrevalidateNodeRemovalResponse doParseInstance(XContentParser parser) throws IOException {
        return PrevalidateNodeRemovalResponse.fromXContent(parser);
    }
}
