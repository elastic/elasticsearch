/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.get.TransportShardMultiGetFomTranslogAction.Response;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResultTests;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.UUID;

public class ShardMultiGetFromTranslogResponseSerializationTests extends AbstractWireSerializingTestCase<Response> {
    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return new Response(randomMultiGetShardResponse(), randomPrimaryTerm(), randomSegmentGeneration());
    }

    @Override
    protected Response mutateInstance(Response instance) throws IOException {
        return switch (randomInt(2)) {
            case 0 -> new Response(
                randomValueOtherThan(instance.multiGetShardResponse(), this::randomMultiGetShardResponse),
                instance.primaryTerm(),
                instance.segmentGeneration()
            );
            case 1 -> new Response(
                instance.multiGetShardResponse(),
                randomValueOtherThan(instance.primaryTerm(), this::randomPrimaryTerm),
                instance.segmentGeneration()
            );
            case 2 -> new Response(
                instance.multiGetShardResponse(),
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

    private GetResponse randomGetResponse() {
        return randomBoolean() ? null : new GetResponse(GetResultTests.randomGetResult(randomFrom(XContentType.values())).v1());
    }

    private MultiGetShardResponse randomMultiGetShardResponse() {
        var response = new MultiGetShardResponse();
        int size = randomIntBetween(0, 100);
        for (int i = 0; i < size; i++) {
            int l = randomNonNegativeInt();
            if (randomBoolean()) {
                response.add(l, randomGetResponse());
            } else {
                response.add(l, randomFailure());
            }
        }
        return response;
    }

    private MultiGetResponse.Failure randomFailure() {
        return new MultiGetResponse.Failure(randomIdentifier(), randomIdentifier(), randomException());
    }

    private Exception randomException() {
        return randomFrom(
            new ShardNotFoundException(randomShardId()),
            new IOException(randomUnicodeOfLengthBetween(10, 100)),
            new IndexNotFoundException(randomIdentifier())
        );
    }

    private ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(10), UUID.randomUUID().toString(), randomIntBetween(0, 5));
    }
}
