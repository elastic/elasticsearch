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
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Wire serialization tests for {@link CoordinationMetadata}.
 */
public class CoordinationMetadataWireSerializingTests extends AbstractWireSerializingTestCase<CoordinationMetadata> {

    @Override
    protected Writeable.Reader<CoordinationMetadata> instanceReader() {
        return CoordinationMetadata::new;
    }

    @Override
    protected CoordinationMetadata createTestInstance() {
        long term = randomNonNegativeLong();
        VotingConfiguration lastCommittedConfiguration = randomVotingConfiguration();
        VotingConfiguration lastAcceptedConfiguration = randomVotingConfiguration();
        Set<VotingConfigExclusion> votingConfigExclusions = randomVotingConfigExclusions();
        return new CoordinationMetadata(term, lastCommittedConfiguration, lastAcceptedConfiguration, votingConfigExclusions);
    }

    @Override
    protected CoordinationMetadata mutateInstance(CoordinationMetadata instance) throws IOException {
        long term = instance.term();
        VotingConfiguration lastCommittedConfiguration = instance.getLastCommittedConfiguration();
        VotingConfiguration lastAcceptedConfiguration = instance.getLastAcceptedConfiguration();
        Set<VotingConfigExclusion> votingConfigExclusions = instance.getVotingConfigExclusions();

        int field = between(0, 3);
        switch (field) {
            case 0 -> term = randomValueOtherThan(term, () -> randomNonNegativeLong());
            case 1 -> lastCommittedConfiguration = randomValueOtherThan(lastCommittedConfiguration, this::randomVotingConfiguration);
            case 2 -> lastAcceptedConfiguration = randomValueOtherThan(lastAcceptedConfiguration, this::randomVotingConfiguration);
            default -> {
                Set<VotingConfigExclusion> mutableExclusions = new HashSet<>(votingConfigExclusions);
                mutableExclusions.add(randomVotingConfigExclusion());
                votingConfigExclusions = mutableExclusions;
            }
        }

        return new CoordinationMetadata(term, lastCommittedConfiguration, lastAcceptedConfiguration, votingConfigExclusions);
    }

    private VotingConfiguration randomVotingConfiguration() {
        return new VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
    }

    /**
     * Returns a set of voting config exclusions (tombstones), possibly empty, so that random metadata can include tombstones.
     */
    private Set<VotingConfigExclusion> randomVotingConfigExclusions() {
        int size = randomIntBetween(0, 10);
        Set<VotingConfigExclusion> votingConfigExclusions = Sets.newHashSetWithExpectedSize(size);
        while (votingConfigExclusions.size() < size) {
            assertTrue(votingConfigExclusions.add(randomVotingConfigExclusion()));
        }
        return votingConfigExclusions;
    }

    private VotingConfigExclusion randomVotingConfigExclusion() {
        return new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }
}
