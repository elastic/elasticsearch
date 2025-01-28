/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class GetSearchApplicationActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    GetSearchApplicationAction.Request> {

    @Override
    protected Writeable.Reader<GetSearchApplicationAction.Request> instanceReader() {
        return GetSearchApplicationAction.Request::new;
    }

    @Override
    protected GetSearchApplicationAction.Request createTestInstance() {
        return new GetSearchApplicationAction.Request(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected GetSearchApplicationAction.Request mutateInstance(GetSearchApplicationAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetSearchApplicationAction.Request doParseInstance(XContentParser parser) throws IOException {
        return GetSearchApplicationAction.Request.parse(parser);
    }

    @Override
    protected GetSearchApplicationAction.Request mutateInstanceForVersion(
        GetSearchApplicationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
