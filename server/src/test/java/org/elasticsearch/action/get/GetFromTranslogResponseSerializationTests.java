/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.get.GetResultTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class GetFromTranslogResponseSerializationTests extends AbstractWireSerializingTestCase<TransportGetFromTranslogAction.Response> {
    @Override
    protected Writeable.Reader<TransportGetFromTranslogAction.Response> instanceReader() {
        return TransportGetFromTranslogAction.Response::new;
    }

    @Override
    protected TransportGetFromTranslogAction.Response createTestInstance() {
        return new TransportGetFromTranslogAction.Response(randomGetResult(), randomSegmentGeneration());
    }

    @Override
    protected TransportGetFromTranslogAction.Response mutateInstance(TransportGetFromTranslogAction.Response instance) throws IOException {
        return randomBoolean()
            ? new TransportGetFromTranslogAction.Response(
                instance.getResult(),
                randomValueOtherThan(instance.segmentGeneration(), this::randomSegmentGeneration)
            )
            : new TransportGetFromTranslogAction.Response(
                randomValueOtherThan(instance.getResult(), this::randomGetResult),
                instance.segmentGeneration()
            );
    }

    private long randomSegmentGeneration() {
        return randomBoolean() ? -1L : randomNonNegativeLong();
    }

    private GetResult randomGetResult() {
        return randomBoolean() ? null : GetResultTests.randomGetResult(randomFrom(XContentType.values())).v1();
    }
}
