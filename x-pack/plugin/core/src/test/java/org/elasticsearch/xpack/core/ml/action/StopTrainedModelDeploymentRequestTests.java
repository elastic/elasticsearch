/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelDeploymentAction.Request;

import java.io.IOException;

public class StopTrainedModelDeploymentRequestTests extends AbstractXContentSerializingTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.parseRequest(null, parser);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLength(10));
        if (randomBoolean()) {
            request.setAllowNoMatch(randomBoolean());
        }
        if (randomBoolean()) {
            request.setForce(randomBoolean());
        }
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
