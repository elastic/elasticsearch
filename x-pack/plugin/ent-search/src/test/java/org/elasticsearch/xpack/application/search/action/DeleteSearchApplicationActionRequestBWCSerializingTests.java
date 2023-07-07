/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;

public class DeleteSearchApplicationActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    DeleteSearchApplicationAction.Request> {

    @Override
    protected Writeable.Reader<DeleteSearchApplicationAction.Request> instanceReader() {
        return DeleteSearchApplicationAction.Request::new;
    }

    @Override
    protected DeleteSearchApplicationAction.Request createTestInstance() {
        return new DeleteSearchApplicationAction.Request(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected DeleteSearchApplicationAction.Request mutateInstance(DeleteSearchApplicationAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected DeleteSearchApplicationAction.Request doParseInstance(XContentParser parser) throws IOException {
        return DeleteSearchApplicationAction.Request.parse(parser);
    }

    @Override
    protected DeleteSearchApplicationAction.Request mutateInstanceForVersion(
        DeleteSearchApplicationAction.Request instance,
        TransportVersion version
    ) {
        return new DeleteSearchApplicationAction.Request(instance.getName());
    }
}
