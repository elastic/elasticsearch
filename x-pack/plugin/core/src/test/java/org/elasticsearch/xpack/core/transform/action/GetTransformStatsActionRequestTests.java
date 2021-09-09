/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction.Request;

public class GetTransformStatsActionRequestTests extends AbstractWireSerializingTestCase<Request> {
    @Override
    protected Request createTestInstance() {
        return new Request(
            randomBoolean()
                ? randomAlphaOfLengthBetween(1, 20)
                : randomBoolean() ? Metadata.ALL : null
        );
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }
}
