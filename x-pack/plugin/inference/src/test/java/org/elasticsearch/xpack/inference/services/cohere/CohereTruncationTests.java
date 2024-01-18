/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class CohereTruncationTests extends AbstractWireSerializingTestCase<CohereTruncation> {
    public static CohereTruncation createRandom() {
        return randomFrom(CohereTruncation.values());
    }

    @Override
    protected Writeable.Reader<CohereTruncation> instanceReader() {
        return CohereTruncation::fromStream;
    }

    @Override
    protected CohereTruncation createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereTruncation mutateInstance(CohereTruncation instance) throws IOException {
        return null;
    }
}
