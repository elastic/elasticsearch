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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.application.analytics.action.PutAnalyticsCollectionAction.Request.NAME_FIELD;

public class PutAnalyticsCollectionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    PutAnalyticsCollectionAction.Request> {

    @Override
    protected Writeable.Reader<PutAnalyticsCollectionAction.Request> instanceReader() {
        return PutAnalyticsCollectionAction.Request::new;
    }

    @Override
    protected PutAnalyticsCollectionAction.Request createTestInstance() {
        return new PutAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, randomIdentifier());
    }

    @Override
    protected PutAnalyticsCollectionAction.Request mutateInstance(PutAnalyticsCollectionAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutAnalyticsCollectionAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected PutAnalyticsCollectionAction.Request mutateInstanceForVersion(
        PutAnalyticsCollectionAction.Request instance,
        TransportVersion version
    ) {
        return new PutAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, instance.getName());
    }

    private static final ConstructingObjectParser<PutAnalyticsCollectionAction.Request, String> PARSER = new ConstructingObjectParser<>(
        "put_analytics_collection_request",
        false,
        (p) -> new PutAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, (String) p[0])
    );
    static {
        PARSER.declareString(constructorArg(), NAME_FIELD);
    }
}
