/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.action.admin.indices.sampling.GetSampleStatsAction.NodeRequest;

public class GetSampleStatsActionNodeRequestTests extends AbstractWireSerializingTestCase<NodeRequest> {
    @Override
    protected Writeable.Reader<NodeRequest> instanceReader() {
        return NodeRequest::new;
    }

    @Override
    protected NodeRequest createTestInstance() {
        return new NodeRequest(randomIdentifier());
    }

    @Override
    protected NodeRequest mutateInstance(NodeRequest instance) throws IOException {
        String index = instance.indices()[0];
        index = randomValueOtherThan(index, ESTestCase::randomIdentifier);
        return new NodeRequest(index);
    }
}
