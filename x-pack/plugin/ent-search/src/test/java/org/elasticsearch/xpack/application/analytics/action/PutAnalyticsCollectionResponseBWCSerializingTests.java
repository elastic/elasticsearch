/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.test.rest.TestResponseParsers;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PutAnalyticsCollectionResponseBWCSerializingTests extends AbstractBWCSerializationTestCase<
    PutAnalyticsCollectionAction.Response> {

    private boolean acknowledgeResponse;

    private String name;

    @Override
    protected Writeable.Reader<PutAnalyticsCollectionAction.Response> instanceReader() {
        return PutAnalyticsCollectionAction.Response::new;
    }

    @Override
    protected PutAnalyticsCollectionAction.Response createTestInstance() {
        this.acknowledgeResponse = randomBoolean();
        this.name = randomIdentifier();
        return new PutAnalyticsCollectionAction.Response(this.acknowledgeResponse, this.name);
    }

    @Override
    protected PutAnalyticsCollectionAction.Response mutateInstance(PutAnalyticsCollectionAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutAnalyticsCollectionAction.Response doParseInstance(XContentParser parser) throws IOException {
        return new PutAnalyticsCollectionAction.Response(TestResponseParsers.parseAcknowledgedResponse(parser).isAcknowledged(), this.name);
    }

    @Override
    protected PutAnalyticsCollectionAction.Response mutateInstanceForVersion(
        PutAnalyticsCollectionAction.Response instance,
        TransportVersion version
    ) {
        return new PutAnalyticsCollectionAction.Response(this.acknowledgeResponse, instance.getName());
    }
}
