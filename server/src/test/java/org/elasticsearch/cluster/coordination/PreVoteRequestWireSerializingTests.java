/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Wire serialization tests for {@link PreVoteRequest}.
 */
public class PreVoteRequestWireSerializingTests extends AbstractWireSerializingTestCase<PreVoteRequest> {

    @Override
    protected Writeable.Reader<PreVoteRequest> instanceReader() {
        return PreVoteRequest::new;
    }

    @Override
    protected PreVoteRequest createTestInstance() {
        return new PreVoteRequest(DiscoveryNodeUtils.randomDiscoveryNode(), randomNonNegativeLong());
    }

    @Override
    protected PreVoteRequest mutateInstance(PreVoteRequest instance) throws IOException {
        if (randomBoolean()) {
            return new PreVoteRequest(
                randomValueOtherThan(instance.getSourceNode(), DiscoveryNodeUtils::randomDiscoveryNode),
                instance.getCurrentTerm()
            );
        }
        return new PreVoteRequest(
            instance.getSourceNode(),
            randomValueOtherThan(instance.getCurrentTerm(), ESTestCase::randomNonNegativeLong)
        );
    }
}
