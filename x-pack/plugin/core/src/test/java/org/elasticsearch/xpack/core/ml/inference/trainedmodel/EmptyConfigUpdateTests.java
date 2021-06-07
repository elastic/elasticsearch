/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class EmptyConfigUpdateTests extends AbstractWireSerializingTestCase<EmptyConfigUpdate> {

    public static EmptyConfigUpdate testInstance() {
        return new EmptyConfigUpdate();
    }

    @Override
    protected Writeable.Reader<EmptyConfigUpdate> instanceReader() {
        return EmptyConfigUpdate::new;
    }

    @Override
    protected EmptyConfigUpdate createTestInstance() {
        return new EmptyConfigUpdate();
    }

    public void testToConfig() {
        expectThrows(UnsupportedOperationException.class, () -> new EmptyConfigUpdate().toConfig());
    }
}
