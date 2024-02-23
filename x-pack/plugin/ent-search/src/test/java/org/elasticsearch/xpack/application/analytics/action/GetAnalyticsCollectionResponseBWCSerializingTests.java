/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.List;

public class GetAnalyticsCollectionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    GetAnalyticsCollectionAction.Response> {

    @Override
    protected Writeable.Reader<GetAnalyticsCollectionAction.Response> instanceReader() {
        return GetAnalyticsCollectionAction.Response::new;
    }

    @Override
    protected GetAnalyticsCollectionAction.Response createTestInstance() {
        List<AnalyticsCollection> collections = randomList(randomIntBetween(1, 10), () -> new AnalyticsCollection(randomIdentifier()));
        return new GetAnalyticsCollectionAction.Response(collections);
    }

    @Override
    protected GetAnalyticsCollectionAction.Response mutateInstance(GetAnalyticsCollectionAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetAnalyticsCollectionAction.Response mutateInstanceForVersion(
        GetAnalyticsCollectionAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
