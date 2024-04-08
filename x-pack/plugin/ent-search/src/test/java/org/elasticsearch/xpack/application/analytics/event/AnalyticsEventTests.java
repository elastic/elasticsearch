/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomAnalyticsEvent;

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
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
