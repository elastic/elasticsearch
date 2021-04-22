/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class TrainedModelLocationTests extends AbstractSerializingTestCase<TrainedModelLocation> {

    private final boolean lenient = randomBoolean();

    public static TrainedModelLocation randomInstance() {
        return new TrainedModelLocation(randomAlphaOfLength(7), randomAlphaOfLength(7));
    }

    @Override
    protected TrainedModelLocation doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelLocation.fromXContent(parser, lenient);
    }

    @Override
    protected Writeable.Reader<TrainedModelLocation> instanceReader() {
        return TrainedModelLocation::new;
    }

    @Override
    protected TrainedModelLocation createTestInstance() {
        return randomInstance();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }
}
