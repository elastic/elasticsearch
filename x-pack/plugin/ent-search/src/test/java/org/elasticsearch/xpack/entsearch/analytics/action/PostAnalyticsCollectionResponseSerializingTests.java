/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics.action;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.entsearch.analytics.AnalyticsTestUtils;

import java.io.IOException;

public class PostAnalyticsCollectionResponseSerializingTests extends AbstractWireSerializingTestCase<PostAnalyticsCollectionAction.Response> {

    @Override
    protected Writeable.Reader<PostAnalyticsCollectionAction.Response> instanceReader() {
        return PostAnalyticsCollectionAction.Response::new;
    }

    @Override
    protected PostAnalyticsCollectionAction.Response createTestInstance() {
        return new PostAnalyticsCollectionAction.Response(AnalyticsTestUtils.randomAnalyticsCollection());
    }

    @Override
    protected PostAnalyticsCollectionAction.Response mutateInstance(PostAnalyticsCollectionAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
