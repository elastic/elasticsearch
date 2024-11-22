/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class ReindexDataStreamRequestTests extends AbstractWireSerializingTestCase<ReindexDataStreamRequest> {
    @Override
    protected Writeable.Reader<ReindexDataStreamRequest> instanceReader() {
        return ReindexDataStreamRequest::new;
    }

    @Override
    protected ReindexDataStreamRequest createTestInstance() {
        return new ReindexDataStreamRequest(randomAlphaOfLength(50));
    }

    @Override
    protected ReindexDataStreamRequest mutateInstance(ReindexDataStreamRequest instance) throws IOException {
        return createTestInstance();
    }
}
