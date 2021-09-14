/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InferTrainedModelDeploymentRequestsTests extends AbstractSerializingTestCase<InferTrainedModelDeploymentAction.Request> {
    @Override
    protected InferTrainedModelDeploymentAction.Request doParseInstance(XContentParser parser) throws IOException {
        return InferTrainedModelDeploymentAction.Request.parseRequest(null, parser);
    }

    @Override
    protected Writeable.Reader<InferTrainedModelDeploymentAction.Request> instanceReader() {
        return InferTrainedModelDeploymentAction.Request::new;
    }

    @Override
    protected InferTrainedModelDeploymentAction.Request createTestInstance() {
        List<Map<String, Object>> docs = randomList(5, () -> randomMap(1, 3,
            () -> Tuple.tuple(randomAlphaOfLength(7), randomAlphaOfLength(7))));

        InferTrainedModelDeploymentAction.Request request =
            new InferTrainedModelDeploymentAction.Request(randomAlphaOfLength(4), docs);
        if (randomBoolean()) {
            request.setTimeout(randomTimeValue());
        }
        return request;
    }

    public void testTimeoutNotNull() {
        assertNotNull(createTestInstance().getTimeout());
    }
}
