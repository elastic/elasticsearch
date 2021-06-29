/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.client.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class PutTrainedModelActionResponseTests extends AbstractXContentTestCase<PutTrainedModelResponse> {

    @Override
    protected PutTrainedModelResponse createTestInstance() {
        return new PutTrainedModelResponse(TrainedModelConfigTests.createTestTrainedModelConfig());
    }

    @Override
    protected PutTrainedModelResponse doParseInstance(XContentParser parser) throws IOException {
        return new PutTrainedModelResponse(TrainedModelConfig.PARSER.apply(parser, null).build());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

}
