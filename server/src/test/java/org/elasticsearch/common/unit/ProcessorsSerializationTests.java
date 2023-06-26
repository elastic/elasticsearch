/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ProcessorsSerializationTests extends AbstractWireSerializingTestCase<Processors> {
    @Override
    protected Writeable.Reader<Processors> instanceReader() {
        return Processors::readFrom;
    }

    @Override
    protected Processors createTestInstance() {
        return Processors.of(randomDoubleBetween(Double.MIN_VALUE, 512.99999999, true));
    }

    @Override
    protected Processors mutateInstance(Processors instance) {
        return Processors.of(instance.count() + randomDoubleBetween(0.01, 1, true));
    }
}
