/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.ReindexDataStreamRequest;

import java.io.IOException;

public class ReindexDataStreamRequestTests extends AbstractXContentSerializingTestCase<ReindexDataStreamRequest> {

    @Override
    protected ReindexDataStreamRequest createTestInstance() {
        return new ReindexDataStreamRequest(ReindexDataStreamAction.Mode.UPGRADE, randomAlphaOfLength(40));
    }

    @Override
    protected ReindexDataStreamRequest mutateInstance(ReindexDataStreamRequest instance) {
        // There is currently only one possible value for mode, so we can't change it
        return new ReindexDataStreamRequest(instance.getMode(), randomAlphaOfLength(50));
    }

    @Override
    protected ReindexDataStreamRequest doParseInstance(XContentParser parser) throws IOException {
        return ReindexDataStreamRequest.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ReindexDataStreamRequest> instanceReader() {
        return ReindexDataStreamRequest::new;
    }
}
