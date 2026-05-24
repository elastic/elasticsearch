/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomAnalyticsEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomPayload;

public class AnalyticsEventTests extends AbstractWireSerializingTestCase<AnalyticsEvent> {

    @Override
    protected Writeable.Reader<AnalyticsEvent> instanceReader() {
        return AnalyticsEvent::new;
    }

    @Override
    protected AnalyticsEvent createTestInstance() {
        return randomAnalyticsEvent();
    }

    @Override
    protected AnalyticsEvent mutateInstance(AnalyticsEvent instance) throws IOException {
        String eventCollectionName = instance.eventCollectionName();
        long eventTime = instance.eventTime();
        AnalyticsEvent.Type eventType = instance.eventType();
        XContentType xContentType = instance.xContentType();
        BytesReference payload = instance.payload();
        switch (between(0, 4)) {
            case 0 -> eventCollectionName = randomValueOtherThan(eventCollectionName, () -> randomIdentifier());
            case 1 -> eventTime = randomValueOtherThan(eventTime, () -> randomLong());
            case 2 -> eventType = randomValueOtherThan(eventType, () -> randomFrom(AnalyticsEvent.Type.values()));
            case 3 -> xContentType = randomValueOtherThan(xContentType, () -> randomFrom(XContentType.values()));
            case 4 -> payload = randomValueOtherThan(payload, () -> randomPayload());
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AnalyticsEvent(eventCollectionName, eventTime, eventType, xContentType, payload);
    }
}
