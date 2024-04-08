/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;

public class GetAnalyticsCollectionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    GetAnalyticsCollectionAction.Request> {

    @Override
    protected Writeable.Reader<GetAnalyticsCollectionAction.Request> instanceReader() {
        return GetAnalyticsCollectionAction.Request::new;
    }

    @Override
    protected GetAnalyticsCollectionAction.Request createTestInstance() {
        return new GetAnalyticsCollectionAction.Request(new String[] { randomIdentifier() });
    }

    @Override
    protected GetAnalyticsCollectionAction.Request mutateInstance(GetAnalyticsCollectionAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetAnalyticsCollectionAction.Request doParseInstance(XContentParser parser) throws IOException {
        return GetAnalyticsCollectionAction.Request.parse(parser);
    }

    @Override
    protected GetAnalyticsCollectionAction.Request mutateInstanceForVersion(
        GetAnalyticsCollectionAction.Request instance,
        TransportVersion version
    ) {
        return new GetAnalyticsCollectionAction.Request(instance.getNames());
    }
}
