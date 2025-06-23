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

public class RoutingStateAndReasonTests extends AbstractXContentSerializingTestCase<RoutingStateAndReason> {

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

    @Override
    protected RoutingStateAndReason mutateInstance(RoutingStateAndReason instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
