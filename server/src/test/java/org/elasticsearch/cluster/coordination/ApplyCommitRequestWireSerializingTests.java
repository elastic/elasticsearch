/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.cluster.node.DiscoveryNodeUtils.randomDiscoveryNode;

/**
 * Wire serialization tests for {@link ApplyCommitRequest}.
 */
public class ApplyCommitRequestWireSerializingTests extends AbstractWireSerializingTestCase<ApplyCommitRequest> {

    @Override
    protected Writeable.Reader<ApplyCommitRequest> instanceReader() {
        return ApplyCommitRequest::new;
    }

    @Override
    protected ApplyCommitRequest createTestInstance() {
        return new ApplyCommitRequest(randomDiscoveryNode(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected ApplyCommitRequest mutateInstance(ApplyCommitRequest instance) throws IOException {
        DiscoveryNode sourceNode = instance.getSourceNode();
        long term = instance.getTerm();
        long version = instance.getVersion();

        switch (between(0, 2)) {
            case 0 -> sourceNode = randomValueOtherThan(sourceNode, DiscoveryNodeUtils::randomDiscoveryNode);
            case 1 -> term = randomValueOtherThan(term, ESTestCase::randomNonNegativeLong);
            case 2 -> version = randomValueOtherThan(version, ESTestCase::randomNonNegativeLong);
            default -> throw new AssertionError();
        }

        return new ApplyCommitRequest(sourceNode, term, version);
    }
}
