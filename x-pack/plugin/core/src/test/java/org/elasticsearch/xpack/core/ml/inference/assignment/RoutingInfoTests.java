/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class RoutingInfoTests extends AbstractXContentSerializingTestCase<RoutingInfo> {

    @Override
    protected RoutingInfo doParseInstance(XContentParser parser) throws IOException {
        return RoutingInfo.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RoutingInfo> instanceReader() {
        return RoutingInfo::new;
    }

    @Override
    protected RoutingInfo createTestInstance() {
        return randomInstance();
    }

    @Override
    protected RoutingInfo mutateInstance(RoutingInfo instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static RoutingInfo randomInstance() {
        return new RoutingInfo(
            randomIntBetween(1, 10),
            randomIntBetween(1, 10),
            randomFrom(RoutingState.values()),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    public static RoutingInfo randomInstance(RoutingState state) {
        return new RoutingInfo(randomIntBetween(1, 10), randomIntBetween(1, 10), state, randomBoolean() ? null : randomAlphaOfLength(10));
    }

    public void testIsRoutable_GivenNonStarted() {
        RoutingInfo routingInfo = new RoutingInfo(
            1,
            1,
            randomFrom(RoutingState.STARTING, RoutingState.FAILED, RoutingState.STOPPING, RoutingState.STOPPED),
            ""
        );
        assertThat(routingInfo.isRoutable(), is(false));
    }

    public void testIsRoutable_GivenStartedWithZeroAllocations() {
        RoutingInfo routingInfo = new RoutingInfo(0, 1, RoutingState.STARTED, "");
        assertThat(routingInfo.isRoutable(), is(false));
    }

    public void testIsRoutable_GivenStartedWithNonZeroAllocations() {
        RoutingInfo routingInfo = new RoutingInfo(randomIntBetween(1, 10), 1, RoutingState.STARTED, "");
        assertThat(routingInfo.isRoutable(), is(true));
    }
}
