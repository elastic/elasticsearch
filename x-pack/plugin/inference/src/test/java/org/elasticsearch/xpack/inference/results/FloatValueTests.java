/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.FloatValue;

import java.io.IOException;

public class FloatValueTests extends AbstractWireSerializingTestCase<FloatValue> {
    public static FloatValue createRandom() {
        return new FloatValue(randomFloat());
    }

    @Override
    protected Writeable.Reader<FloatValue> instanceReader() {
        return FloatValue::new;
    }

    @Override
    protected FloatValue createTestInstance() {
        return createRandom();
    }

    @Override
    protected FloatValue mutateInstance(FloatValue instance) throws IOException {
        return null;
    }
}
