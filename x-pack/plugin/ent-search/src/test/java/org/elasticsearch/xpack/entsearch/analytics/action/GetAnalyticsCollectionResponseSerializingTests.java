/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.entsearch.analytics.AnalyticsTestUtils;

import java.io.IOException;

public class GetAnalyticsCollectionResponseSerializingTests extends AbstractWireSerializingTestCase<GetAnalyticsCollectionAction.Response> {

    @Override
    protected Writeable.Reader<GetAnalyticsCollectionAction.Response> instanceReader() {
        return GetAnalyticsCollectionAction.Response::new;
    }

    @Override
    protected GetAnalyticsCollectionAction.Response createTestInstance() {
        return new GetAnalyticsCollectionAction.Response(AnalyticsTestUtils.randomAnalyticsCollection());
    }

    @Override
    protected GetAnalyticsCollectionAction.Response mutateInstance(GetAnalyticsCollectionAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
