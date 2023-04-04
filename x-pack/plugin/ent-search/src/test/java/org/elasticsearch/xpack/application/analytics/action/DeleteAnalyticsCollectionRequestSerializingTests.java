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

public class DeleteAnalyticsCollectionRequestSerializingTests extends AbstractWireSerializingTestCase<
    DeleteAnalyticsCollectionAction.Request> {

    @Override
    protected Writeable.Reader<DeleteAnalyticsCollectionAction.Request> instanceReader() {
        return DeleteAnalyticsCollectionAction.Request::new;
    }

    @Override
    protected DeleteAnalyticsCollectionAction.Request createTestInstance() {
        return new DeleteAnalyticsCollectionAction.Request(randomIdentifier());
    }

    @Override
    protected DeleteAnalyticsCollectionAction.Request mutateInstance(DeleteAnalyticsCollectionAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
