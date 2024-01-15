/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class ChunkedInferenceActionRequestTests extends AbstractWireSerializingTestCase<ChunkedInferenceAction.Request> {
    @Override
    protected Writeable.Reader<ChunkedInferenceAction.Request> instanceReader() {
        return ChunkedInferenceAction.Request::new;
    }

    @Override
    protected ChunkedInferenceAction.Request createTestInstance() {
        return new ChunkedInferenceAction.Request(
            randomAlphaOfLength(5),
            randomList(1, 5, () -> randomAlphaOfLength(6)),
            randomBoolean() ? null : randomIntBetween(10, 20),
            randomBoolean() ? null : randomIntBetween(1, 10),
            TimeValue.timeValueSeconds(randomIntBetween(0, 60))
        );
    }

    @Override
    protected ChunkedInferenceAction.Request mutateInstance(ChunkedInferenceAction.Request instance) throws IOException {
        return null;
    }
}
