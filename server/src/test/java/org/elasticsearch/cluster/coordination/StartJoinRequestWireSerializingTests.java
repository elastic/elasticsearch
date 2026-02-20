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

import static org.elasticsearch.cluster.node.DiscoveryNodeUtils.create;

/**
 * Wire serialization tests for {@link StartJoinRequest}.
 */
public class StartJoinRequestWireSerializingTests extends AbstractWireSerializingTestCase<StartJoinRequest> {

    @Override
    protected Writeable.Reader<StartJoinRequest> instanceReader() {
        return StartJoinRequest::new;
    }

    @Override
    protected StartJoinRequest createTestInstance() {
        DiscoveryNode masterCandidateNode = create();
        long term = randomNonNegativeLong();
        return new StartJoinRequest(masterCandidateNode, term);
    }

    @Override
    protected StartJoinRequest mutateInstance(StartJoinRequest instance) throws IOException {
        DiscoveryNode masterCandidateNode = instance.getMasterCandidateNode();
        long term = instance.getTerm();

        int field = between(0, 1);
        if (field == 0) {
            masterCandidateNode = randomValueOtherThan(masterCandidateNode, DiscoveryNodeUtils::create);
        } else {
            term = randomValueOtherThan(term, ESTestCase::randomNonNegativeLong);
        }

        return new StartJoinRequest(masterCandidateNode, term);
    }
}
