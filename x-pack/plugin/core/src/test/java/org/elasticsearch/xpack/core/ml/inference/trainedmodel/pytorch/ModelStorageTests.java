/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.pytorch;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class ModelStorageTests extends AbstractSerializingTestCase<ModelStorage> {

    private final boolean lenient = randomBoolean();

    @Override
    protected ModelStorage doParseInstance(XContentParser parser) throws IOException {
        return ModelStorage.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Writeable.Reader<ModelStorage> instanceReader() {
        return ModelStorage::new;
    }

    @Override
    protected ModelStorage createTestInstance() {
        return new ModelStorage(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInstant(),
            randomAlphaOfLength(5),  randomBoolean() ? null : randomAlphaOfLength(5), randomInt(), randomNonNegativeLong());
    }
}
