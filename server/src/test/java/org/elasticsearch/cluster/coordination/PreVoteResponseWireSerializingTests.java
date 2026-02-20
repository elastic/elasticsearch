/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Wire serialization tests for {@link PreVoteResponse}.
 */
public class PreVoteResponseWireSerializingTests extends AbstractWireSerializingTestCase<PreVoteResponse> {

    @Override
    protected Writeable.Reader<PreVoteResponse> instanceReader() {
        return PreVoteResponse::new;
    }

    @Override
    protected PreVoteResponse createTestInstance() {
        long currentTerm = randomNonNegativeLong();
        long lastAcceptedTerm = randomLongBetween(1, currentTerm);
        long lastAcceptedVersion = randomNonNegativeLong();
        return new PreVoteResponse(currentTerm, lastAcceptedTerm, lastAcceptedVersion);
    }

    @Override
    protected PreVoteResponse mutateInstance(PreVoteResponse preVoteResponse) throws IOException {
        return switch (randomInt(2)) {
            case 0 -> {
                assumeTrue("last-accepted term is Long.MAX_VALUE", preVoteResponse.getLastAcceptedTerm() < Long.MAX_VALUE);
                yield new PreVoteResponse(
                    randomValueOtherThan(
                        preVoteResponse.getCurrentTerm(),
                        () -> randomLongBetween(preVoteResponse.getLastAcceptedTerm(), Long.MAX_VALUE)
                    ),
                    preVoteResponse.getLastAcceptedTerm(),
                    preVoteResponse.getLastAcceptedVersion()
                );
            }
            case 1 -> {
                assumeTrue("current term is 1", 1 < preVoteResponse.getCurrentTerm());
                yield new PreVoteResponse(
                    preVoteResponse.getCurrentTerm(),
                    randomValueOtherThan(
                        preVoteResponse.getLastAcceptedTerm(),
                        () -> randomLongBetween(1, preVoteResponse.getCurrentTerm())
                    ),
                    preVoteResponse.getLastAcceptedVersion()
                );
            }
            case 2 -> new PreVoteResponse(
                preVoteResponse.getCurrentTerm(),
                preVoteResponse.getLastAcceptedTerm(),
                randomValueOtherThan(preVoteResponse.getLastAcceptedVersion(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError();
        };
    }
}
