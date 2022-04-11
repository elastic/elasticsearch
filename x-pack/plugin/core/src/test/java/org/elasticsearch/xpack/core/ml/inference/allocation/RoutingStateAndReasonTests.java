/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class RoutingStateAndReasonTests extends AbstractSerializingTestCase<RoutingStateAndReason> {

    public static RoutingStateAndReason randomInstance() {
        return new RoutingStateAndReason(randomFrom(RoutingState.values()), randomBoolean() ? null : randomAlphaOfLength(10));
    }

    @Override
    protected RoutingStateAndReason doParseInstance(XContentParser parser) throws IOException {
        return RoutingStateAndReason.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RoutingStateAndReason> instanceReader() {
        return RoutingStateAndReason::new;
    }

    @Override
    protected RoutingStateAndReason createTestInstance() {
        return randomInstance();
    }
}
