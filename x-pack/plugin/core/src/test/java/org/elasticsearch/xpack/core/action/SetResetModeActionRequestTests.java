/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

public class SetResetModeActionRequestTests extends AbstractXContentSerializingTestCase<SetResetModeActionRequest> {

    @Override
    protected SetResetModeActionRequest createTestInstance() {
        boolean enabled = randomBoolean();
        return new SetResetModeActionRequest(enabled, enabled == false && randomBoolean());
    }

    @Override
    protected SetResetModeActionRequest mutateInstance(SetResetModeActionRequest instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
