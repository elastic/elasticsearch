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

public class PutAnalyticsCollectionResponseSerializingTests extends AbstractWireSerializingTestCase<PutAnalyticsCollectionAction.Response> {

    @Override
    protected Writeable.Reader<PutAnalyticsCollectionAction.Response> instanceReader() {
        return PutAnalyticsCollectionAction.Response::new;
    }

    @Override
    protected PutAnalyticsCollectionAction.Response createTestInstance() {
        return new PutAnalyticsCollectionAction.Response(randomBoolean(), randomIdentifier());
    }

    @Override
    protected PutAnalyticsCollectionAction.Response mutateInstance(PutAnalyticsCollectionAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
