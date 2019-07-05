/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class ModelMetaTests extends AbstractSerializingTestCase<ModelMeta> {
    @Override
    protected ModelMeta doParseInstance(XContentParser parser) throws IOException {
        return ModelMeta.fromXContent(parser);
    }

    @Override
    protected ModelMeta createTestInstance() {
        return new ModelMeta(randomAlphaOfLength(6), randomAlphaOfLength(6));
    }

    @Override
    protected Writeable.Reader<ModelMeta> instanceReader() {
        return ModelMeta::new;
    }
}
