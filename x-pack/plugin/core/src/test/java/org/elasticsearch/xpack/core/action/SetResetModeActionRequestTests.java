/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class SetResetModeActionRequestTests extends AbstractSerializingTestCase<SetResetModeActionRequest> {

    @Override
    protected SetResetModeActionRequest createTestInstance() {
        boolean enabled = randomBoolean();
        return new SetResetModeActionRequest(enabled, enabled == false && randomBoolean());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Writeable.Reader<SetResetModeActionRequest> instanceReader() {
        return SetResetModeActionRequest::new;
    }

    @Override
    protected SetResetModeActionRequest doParseInstance(XContentParser parser) {
        return SetResetModeActionRequest.PARSER.apply(parser, null);
    }
}
