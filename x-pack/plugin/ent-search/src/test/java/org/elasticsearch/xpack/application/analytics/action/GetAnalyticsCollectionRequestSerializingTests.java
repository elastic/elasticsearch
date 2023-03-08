/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class GetAnalyticsCollectionRequestSerializingTests extends AbstractWireSerializingTestCase<GetAnalyticsCollectionAction.Request> {

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
}
