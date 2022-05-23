/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PyTorchResultTests extends AbstractXContentTestCase<PyTorchResult> {

    @Override
    protected PyTorchResult createTestInstance() {
        int type = randomIntBetween(0, 2);
        return switch (type) {
            case 0 -> new PyTorchResult(PyTorchInferenceResultTests.createRandom(), null, null);
            case 1 -> new PyTorchResult(null, ThreadSettingsTests.createRandom(), null);
            default -> new PyTorchResult(null, null, ErrorResultTests.createRandom());
        };
    }

    @Override
    protected PyTorchResult doParseInstance(XContentParser parser) throws IOException {
        return PyTorchResult.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
