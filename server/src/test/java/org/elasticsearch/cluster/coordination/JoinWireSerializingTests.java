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
 * Wire serialization tests for {@link Join}.
 */
public class JoinWireSerializingTests extends AbstractWireSerializingTestCase<Join> {

    @Override
    protected Writeable.Reader<Join> instanceReader() {
        return Join::new;
    }

    @Override
    protected Join createTestInstance() {
        return randomJoin();
    }

    @Override
    protected Join mutateInstance(Join instance) throws IOException {
        DiscoveryNode votingNode = instance.votingNode();
        DiscoveryNode masterCandidateNode = instance.masterCandidateNode();
        long term = instance.term();
        long lastAcceptedTerm = instance.lastAcceptedTerm();
        long lastAcceptedVersion = instance.lastAcceptedVersion();

        switch (between(0, 4)) {
            case 0 -> votingNode = randomValueOtherThan(votingNode, DiscoveryNodeUtils::randomDiscoveryNode);
            case 1 -> masterCandidateNode = randomValueOtherThan(masterCandidateNode, DiscoveryNodeUtils::randomDiscoveryNode);
            case 2 -> term = randomValueOtherThan(term, ESTestCase::randomNonNegativeLong);
            case 3 -> lastAcceptedTerm = randomValueOtherThan(lastAcceptedTerm, ESTestCase::randomNonNegativeLong);
            case 4 -> lastAcceptedVersion = randomValueOtherThan(lastAcceptedVersion, ESTestCase::randomNonNegativeLong);
            default -> throw new AssertionError();
        }

        return new Join(votingNode, masterCandidateNode, term, lastAcceptedTerm, lastAcceptedVersion);
    }

    public static Join randomJoin() {
        return new Join(
            randomDiscoveryNode(),
            randomDiscoveryNode(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }
}
