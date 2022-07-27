/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction.Request;

public class UpgradeJobModelSnapshotRequestTests extends AbstractSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomTimeValue(),
            randomBoolean() ? null : randomBoolean()
        );
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
        return Request.parseRequest(parser);
    }
}
