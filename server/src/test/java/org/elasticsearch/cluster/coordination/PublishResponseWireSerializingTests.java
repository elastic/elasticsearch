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
        long term = randomNonNegativeLong();
        long version = randomNonNegativeLong();
        return new PublishResponse(term, version);
    }

    @Override
    protected PublishResponse mutateInstance(PublishResponse instance) throws IOException {
        long term = instance.getTerm();
        long version = instance.getVersion();
        int field = between(0, 1);
        if (field == 0) {
            term = randomValueOtherThan(term, () -> randomNonNegativeLong());
        } else {
            version = randomValueOtherThan(version, () -> randomNonNegativeLong());
        }
        return new PublishResponse(term, version);
    }
}
