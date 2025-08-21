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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PostAnalyticsEventResponseBWCSerializingTests extends AbstractBWCSerializationTestCase<PostAnalyticsEventAction.Response> {
    @Override
    protected Writeable.Reader<PostAnalyticsEventAction.Response> instanceReader() {
        return PostAnalyticsEventAction.Response::readFromStreamInput;
    }

    @Override
    protected PostAnalyticsEventAction.Response createTestInstance() {
        return new PostAnalyticsEventAction.Response(randomBoolean());
    }

    @Override
    protected PostAnalyticsEventAction.Response mutateInstance(PostAnalyticsEventAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PostAnalyticsEventAction.Response doParseInstance(XContentParser parser) throws IOException {
        return PostAnalyticsEventAction.Response.parse(parser);
    }

    @Override
    protected PostAnalyticsEventAction.Response mutateInstanceForVersion(
        PostAnalyticsEventAction.Response instance,
        TransportVersion version
    ) {
        return new PostAnalyticsEventAction.Response(instance.isAccepted());
    }
}
