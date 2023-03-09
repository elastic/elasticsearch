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

public class PostAnalyticsEventRequestSerializingTests extends AbstractWireSerializingTestCase<PostAnalyticsEventAction.Request> {

    @Override
    protected Writeable.Reader<PostAnalyticsEventAction.Request> instanceReader() {
        return PostAnalyticsEventAction.Request::new;
    }

    @Override
    protected PostAnalyticsEventAction.Request createTestInstance() {
        return new PostAnalyticsEventAction.Request(randomIdentifier());
    }

    @Override
    protected PostAnalyticsEventAction.Request mutateInstance(PostAnalyticsEventAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
