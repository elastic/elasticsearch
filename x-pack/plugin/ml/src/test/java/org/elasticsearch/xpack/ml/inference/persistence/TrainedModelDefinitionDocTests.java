/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class TrainedModelDefinitionDocTests extends AbstractXContentTestCase<TrainedModelDefinitionDoc> {

    private final boolean isLenient = randomBoolean();

    @Override
    protected TrainedModelDefinitionDoc doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelDefinitionDoc.fromXContent(parser, isLenient).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return isLenient;
    }

    @Override
    protected TrainedModelDefinitionDoc createTestInstance() {
        int length = randomIntBetween(4, 10);

        return new TrainedModelDefinitionDoc.Builder()
            .setModelId(randomAlphaOfLength(6))
            .setDefinitionLength(length)
            .setTotalDefinitionLength(randomIntBetween(length, length *2))
            .setBinaryData(new BytesArray(randomByteArrayOfLength(length)))
            .setDocNum(randomIntBetween(0, 10))
            .setCompressionVersion(randomIntBetween(1, 5))
            .setEos(randomBoolean())
            .build();
    }
}
