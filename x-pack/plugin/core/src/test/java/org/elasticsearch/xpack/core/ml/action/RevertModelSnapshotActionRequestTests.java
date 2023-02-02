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
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction.Request;

public class RevertModelSnapshotActionRequestTests extends AbstractXContentSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        RevertModelSnapshotAction.Request request = new RevertModelSnapshotAction.Request(
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 20)
        );
        if (randomBoolean()) {
            request.setDeleteInterveningResults(randomBoolean());
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

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return RevertModelSnapshotAction.Request.parseRequest(null, null, parser);
    }
}
