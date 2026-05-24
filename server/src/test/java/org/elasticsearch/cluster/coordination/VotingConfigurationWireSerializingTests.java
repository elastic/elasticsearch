/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Wire serialization tests for {@link VotingConfiguration}.
 */
public class VotingConfigurationWireSerializingTests extends AbstractWireSerializingTestCase<VotingConfiguration> {

    @Override
    protected Writeable.Reader<VotingConfiguration> instanceReader() {
        return VotingConfiguration::new;
    }

    @Override
    protected VotingConfiguration createTestInstance() {
        return randomVotingConfiguration();
    }

    @Override
    protected VotingConfiguration mutateInstance(VotingConfiguration instance) throws IOException {
        Set<String> nodeIds = new HashSet<>(instance.getNodeIds());
        String newNodeId;
        do {
            newNodeId = randomAlphaOfLength(20);
        } while (nodeIds.contains(newNodeId));
        nodeIds.add(newNodeId);
        return new VotingConfiguration(nodeIds);
    }

    private VotingConfiguration randomVotingConfiguration() {
        return new VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
    }
}
