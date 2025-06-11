/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class NoneChunkingSettingsTests extends AbstractWireSerializingTestCase<NoneChunkingSettings> {
    @Override
    protected Writeable.Reader<NoneChunkingSettings> instanceReader() {
        return in -> NoneChunkingSettings.INSTANCE;
    }

    @Override
    protected NoneChunkingSettings createTestInstance() {
        return NoneChunkingSettings.INSTANCE;
    }

    @Override
    protected NoneChunkingSettings mutateInstance(NoneChunkingSettings instance) throws IOException {
        return null;
    }
}
