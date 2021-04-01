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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;

public class PyTorchModelTests extends AbstractSerializingTestCase<PyTorchModel> {

    private boolean lenientParsing = randomBoolean();

    @Override
    protected boolean supportsUnknownFields() {
        return lenientParsing;
    }

    @Override
    protected PyTorchModel doParseInstance(XContentParser parser) throws IOException {
        return PyTorchModel.fromXContent(parser, lenientParsing);
    }

    @Override
    protected Writeable.Reader<PyTorchModel> instanceReader() {
        return PyTorchModel::new;
    }

    @Override
    protected PyTorchModel createTestInstance() {
        return new PyTorchModel(randomAlphaOfLength(5), randomFrom(TargetType.values()));
    }
}
