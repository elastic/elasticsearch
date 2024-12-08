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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction.Request.NAMES_FIELD;

public class GetAnalyticsCollectionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    GetAnalyticsCollectionAction.Request> {

    @Override
    protected Writeable.Reader<GetAnalyticsCollectionAction.Request> instanceReader() {
        return GetAnalyticsCollectionAction.Request::new;
    }

    @Override
    protected GetAnalyticsCollectionAction.Request createTestInstance() {
        return new GetAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, new String[] { randomIdentifier() });
    }

    @Override
    protected GetAnalyticsCollectionAction.Request mutateInstance(GetAnalyticsCollectionAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetAnalyticsCollectionAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected GetAnalyticsCollectionAction.Request mutateInstanceForVersion(
        GetAnalyticsCollectionAction.Request instance,
        TransportVersion version
    ) {
        return new GetAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, instance.getNames());
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetAnalyticsCollectionAction.Request, Void> PARSER = new ConstructingObjectParser<>(
        "get_analytics_collection_request",
        p -> new GetAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, ((List<String>) p[0]).toArray(String[]::new))
    );
    static {
        PARSER.declareStringArray(constructorArg(), NAMES_FIELD);
    }
}
