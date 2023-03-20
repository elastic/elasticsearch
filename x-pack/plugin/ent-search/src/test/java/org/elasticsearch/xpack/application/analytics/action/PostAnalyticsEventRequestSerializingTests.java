/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEventType;

import java.io.IOException;
import java.util.Locale;

public class PostAnalyticsEventRequestSerializingTests extends AbstractWireSerializingTestCase<PostAnalyticsEventAction.Request> {

    @Override
    protected Writeable.Reader<PostAnalyticsEventAction.Request> instanceReader() {
        return PostAnalyticsEventAction.Request::new;
    }

    @Override
    protected PostAnalyticsEventAction.Request createTestInstance() {
        return new PostAnalyticsEventAction.Request(
            randomIdentifier(),
            randomEventType(),
            randomFrom(XContentType.values()),
            new BytesArray(randomByteArrayOfLength(20))
        );
    }

    @Override
    protected PostAnalyticsEventAction.Request mutateInstance(PostAnalyticsEventAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private String randomEventType() {
        return randomFrom(AnalyticsEventType.values()).toString().toLowerCase(Locale.ROOT);
    }
}
