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
 * Wire serialization tests for {@link PublishResponse}.
 */
public class PublishResponseWireSerializingTests extends AbstractWireSerializingTestCase<PublishResponse> {

    @Override
    protected Writeable.Reader<PublishResponse> instanceReader() {
        return PublishResponse::new;
    }

    @Override
    protected PublishResponse createTestInstance() {
        return new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected PublishResponse mutateInstance(PublishResponse publishResponse) throws IOException {
        if (randomBoolean()) {
            return new PublishResponse(
                randomValueOtherThan(publishResponse.getTerm(), ESTestCase::randomNonNegativeLong),
                publishResponse.getVersion()
            );
        }
        return new PublishResponse(
            publishResponse.getTerm(),
            randomValueOtherThan(publishResponse.getVersion(), ESTestCase::randomNonNegativeLong)
        );
    }
}
