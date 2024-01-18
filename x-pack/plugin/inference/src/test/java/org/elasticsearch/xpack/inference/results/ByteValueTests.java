/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.ByteValue;

import java.io.IOException;

public class ByteValueTests extends AbstractWireSerializingTestCase<ByteValue> {
    public static ByteValue createRandom() {
        return new ByteValue(randomByte());
    }

    @Override
    protected Writeable.Reader<ByteValue> instanceReader() {
        return ByteValue::new;
    }

    @Override
    protected ByteValue createTestInstance() {
        return createRandom();
    }

    @Override
    protected ByteValue mutateInstance(ByteValue instance) throws IOException {
        return null;
    }
}
