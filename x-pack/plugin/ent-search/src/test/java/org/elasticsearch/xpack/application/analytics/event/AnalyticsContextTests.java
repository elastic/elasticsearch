/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;

public class AnalyticsContextTests extends AbstractWireSerializingTestCase<AnalyticsContext> {
    @Override
    protected Writeable.Reader<AnalyticsContext> instanceReader() {
        return org.elasticsearch.xpack.application.analytics.event.AnalyticsContext::new;
    }

    @Override
    protected AnalyticsContext createTestInstance() {
        return new AnalyticsContext(new AnalyticsCollection(randomIdentifier()), randomFrom(AnalyticsEvent.Type.values()), randomLong());
    }

    @Override
    protected AnalyticsContext mutateInstance(AnalyticsContext instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
