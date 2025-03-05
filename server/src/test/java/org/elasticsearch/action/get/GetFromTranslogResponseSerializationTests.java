/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
        return new TransportGetFromTranslogAction.Response(randomGetResult(), randomPrimaryTerm(), randomSegmentGeneration());
    }

    @Override
    protected TransportGetFromTranslogAction.Response mutateInstance(TransportGetFromTranslogAction.Response instance) throws IOException {
        return switch (randomInt(2)) {
            case 0 -> new TransportGetFromTranslogAction.Response(
                randomValueOtherThan(instance.getResult(), this::randomGetResult),
                instance.primaryTerm(),
                instance.segmentGeneration()
            );
            case 1 -> new TransportGetFromTranslogAction.Response(
                instance.getResult(),
                randomValueOtherThan(instance.primaryTerm(), this::randomPrimaryTerm),
                instance.segmentGeneration()
            );
            case 2 -> new TransportGetFromTranslogAction.Response(
                instance.getResult(),
                instance.primaryTerm(),
                randomValueOtherThan(instance.segmentGeneration(), this::randomSegmentGeneration)
            );
            default -> randomValueOtherThan(instance, this::createTestInstance);
        };
    }

    private long randomSegmentGeneration() {
        return randomBoolean() ? -1L : randomNonNegativeLong();
    }

    private long randomPrimaryTerm() {
        return randomNonNegativeLong();
    }

    private GetResult randomGetResult() {
        return randomBoolean() ? null : GetResultTests.randomGetResult(randomFrom(XContentType.values())).v1();
    }
}
