/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DeleteQueryRuleActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<DeleteQueryRuleAction.Request> {

    @Override
    protected Writeable.Reader<DeleteQueryRuleAction.Request> instanceReader() {
        return DeleteQueryRuleAction.Request::new;
    }

    @Override
    protected DeleteQueryRuleAction.Request createTestInstance() {
        return new DeleteQueryRuleAction.Request(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected DeleteQueryRuleAction.Request mutateInstance(DeleteQueryRuleAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected DeleteQueryRuleAction.Request doParseInstance(XContentParser parser) throws IOException {
        return DeleteQueryRuleAction.Request.parse(parser);
    }

    @Override
    protected DeleteQueryRuleAction.Request mutateInstanceForVersion(DeleteQueryRuleAction.Request instance, TransportVersion version) {
        return instance;
    }
}
