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
import static org.elasticsearch.xpack.application.analytics.action.DeleteAnalyticsCollectionAction.Request.COLLECTION_NAME_FIELD;

public class DeleteAnalyticsCollectionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    DeleteAnalyticsCollectionAction.Request> {

    @Override
    protected Writeable.Reader<DeleteAnalyticsCollectionAction.Request> instanceReader() {
        return DeleteAnalyticsCollectionAction.Request::new;
    }

    @Override
    protected DeleteAnalyticsCollectionAction.Request createTestInstance() {
        return new DeleteAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, randomIdentifier());
    }

    @Override
    protected DeleteAnalyticsCollectionAction.Request mutateInstance(DeleteAnalyticsCollectionAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected DeleteAnalyticsCollectionAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected DeleteAnalyticsCollectionAction.Request mutateInstanceForVersion(
        DeleteAnalyticsCollectionAction.Request instance,
        TransportVersion version
    ) {
        return new DeleteAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, instance.getCollectionName());
    }

    private static final ConstructingObjectParser<DeleteAnalyticsCollectionAction.Request, Void> PARSER = new ConstructingObjectParser<>(
        "delete_analytics_collection_request",
        p -> new DeleteAnalyticsCollectionAction.Request(TEST_REQUEST_TIMEOUT, (String) p[0])
    );

    static {
        PARSER.declareString(constructorArg(), COLLECTION_NAME_FIELD);
    }

}
