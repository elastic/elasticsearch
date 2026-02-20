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

import java.io.IOException;
import java.util.Optional;

import static org.elasticsearch.cluster.coordination.JoinWireSerializingTests.randomJoin;

/**
 * Wire serialization tests for {@link PublishWithJoinResponse}.
 */
public class PublishWithJoinResponseWireSerializingTests extends AbstractWireSerializingTestCase<PublishWithJoinResponse> {

    @Override
    protected Writeable.Reader<PublishWithJoinResponse> instanceReader() {
        return PublishWithJoinResponse::new;
    }

    @Override
    protected PublishWithJoinResponse createTestInstance() {
        PublishResponse publishResponse = new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong());
        Optional<Join> optionalJoin = randomBoolean() ? Optional.empty() : Optional.of(randomJoin());
        return new PublishWithJoinResponse(publishResponse, optionalJoin);
    }

    @Override
    protected PublishWithJoinResponse mutateInstance(PublishWithJoinResponse instance) throws IOException {
        PublishResponse publishResponse = instance.getPublishResponse();
        Optional<Join> optionalJoin = instance.getJoin();

        int field = between(0, 1);
        switch (field) {
            case 0 -> publishResponse = randomValueOtherThan(
                publishResponse,
                () -> new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong())
            );
            default -> optionalJoin = optionalJoin.isPresent() && randomBoolean()
                ? Optional.empty()
                : Optional.of(randomValueOtherThan(optionalJoin.orElse(null), JoinWireSerializingTests::randomJoin));
        }

        return new PublishWithJoinResponse(publishResponse, optionalJoin);
    }
}
