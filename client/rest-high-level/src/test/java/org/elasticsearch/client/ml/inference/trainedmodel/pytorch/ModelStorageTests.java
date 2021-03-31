/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference.trainedmodel.pytorch;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;

public class ModelStorageTests extends AbstractXContentTestCase<ModelStorage> {
    @Override
    protected ModelStorage doParseInstance(XContentParser parser) throws IOException {
        return ModelStorage.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected ModelStorage createTestInstance() {
        return new ModelStorage(randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            Instant.ofEpochMilli(randomNonNegativeLong()),
            randomBoolean() ? null : randomAlphaOfLength(5),
            randomInt(),
            randomLong());
    }
}
