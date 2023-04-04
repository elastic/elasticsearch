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

public class PutAnalyticsCollectionRequestSerializingTests extends AbstractWireSerializingTestCase<PutAnalyticsCollectionAction.Request> {

    @Override
    protected Writeable.Reader<PutAnalyticsCollectionAction.Request> instanceReader() {
        return PutAnalyticsCollectionAction.Request::new;
    }

    @Override
    protected PutAnalyticsCollectionAction.Request createTestInstance() {
        return new PutAnalyticsCollectionAction.Request(randomIdentifier());
    }

    @Override
    protected PutAnalyticsCollectionAction.Request mutateInstance(PutAnalyticsCollectionAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
