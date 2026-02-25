/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

/**
 * Wire serialization tests for {@link VotingConfigExclusion}.
 */
public class VotingConfigExclusionWireSerializingTests extends AbstractWireSerializingTestCase<VotingConfigExclusion> {

    @Override
    protected Writeable.Reader<VotingConfigExclusion> instanceReader() {
        return VotingConfigExclusion::new;
    }

    @Override
    protected VotingConfigExclusion createTestInstance() {
        return randomVotingConfigExclusion();
    }

    @Override
    protected VotingConfigExclusion mutateInstance(VotingConfigExclusion instance) throws IOException {
        String nodeId = instance.getNodeId();
        String nodeName = instance.getNodeName();
        int field = between(0, 1);
        if (field == 0) {
            nodeId = randomValueOtherThan(nodeId, () -> randomAlphaOfLength(10));
        } else {
            nodeName = randomValueOtherThan(nodeName, () -> randomAlphaOfLength(10));
        }
        return new VotingConfigExclusion(nodeId, nodeName);
    }

    private VotingConfigExclusion randomVotingConfigExclusion() {
        return new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }
}
