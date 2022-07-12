/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PyTorchResultTests extends AbstractXContentTestCase<PyTorchResult> {

    @Override
    protected PyTorchResult createTestInstance() {
        int type = randomIntBetween(0, 2);
        return switch (type) {
            case 0 -> new PyTorchResult(randomAlphaOfLength(10), PyTorchInferenceResultTests.createRandom(), null, null);
            case 1 -> new PyTorchResult(randomAlphaOfLength(10), null, ThreadSettingsTests.createRandom(), null);
            default -> new PyTorchResult(randomAlphaOfLength(10), null, null, ErrorResultTests.createRandom());
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

    public void testRequestIdSetFromSubObject() throws IOException {
        String testId = randomAlphaOfLength(10);
        CheckedConsumer<PyTorchResult, IOException> tester = r -> {
            BytesReference xcontent = XContentHelper.toXContent(r, XContentType.JSON, false);
            try (XContentParser parser = createParser(XContentFactory.xContent(XContentType.JSON), xcontent)) {
                assertThat(PyTorchResult.PARSER.parse(parser, null).requestId(), equalTo(testId));
            }
        };
        tester.accept(new PyTorchResult(null, new PyTorchInferenceResult(testId, null, null, null), null, null));
        tester.accept(new PyTorchResult(null, null, new ThreadSettings(1, 1, testId), null));
        tester.accept(new PyTorchResult(null, null, null, new ErrorResult(testId, "some error")));
    }
}
