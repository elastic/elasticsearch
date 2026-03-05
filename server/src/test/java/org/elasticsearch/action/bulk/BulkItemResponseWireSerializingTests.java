/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.bulk.BulkItemResponseFailureWireSerializingTests.randomException;
import static org.elasticsearch.action.bulk.BulkItemResponseFailureWireSerializingTests.randomFailure;

public class BulkItemResponseWireSerializingTests extends AbstractWireSerializingTestCase<
    BulkItemResponseWireSerializingTests.BulkItemResponseWrapper> {

    @Override
    protected BulkItemResponseWrapper createTestInstance() {
        int itemId = randomIntBetween(0, 1000);
        OpType opType = randomFrom(OpType.values());
        BulkItemResponse response = randomBoolean()
            ? BulkItemResponse.success(itemId, opType, randomResponse())
            : BulkItemResponse.failure(itemId, opType, randomFailure());
        return new BulkItemResponseWrapper(response);
    }

    @Override
    protected Writeable.Reader<BulkItemResponseWrapper> instanceReader() {
        return BulkItemResponseWrapper::new;
    }

    @Override
    protected BulkItemResponseWrapper mutateInstance(BulkItemResponseWrapper instance) {
        BulkItemResponse original = instance.response();
        int fieldToMutate = randomIntBetween(0, 3);
        BulkItemResponse mutated = switch (fieldToMutate) {
            case 0 -> recreate(
                randomValueOtherThan(original.getItemId(), () -> randomIntBetween(0, 1000)),
                original.getOpType(),
                original.getResponse(),
                original.getFailure()
            );
            case 1 -> recreate(
                original.getItemId(),
                randomValueOtherThan(original.getOpType(), () -> randomFrom(OpType.values())),
                original.getResponse(),
                original.getFailure()
            );
            case 2 -> recreate(
                original.getItemId(),
                original.getOpType(),
                randomValueOtherThan(original.getResponse(), BulkItemResponseWireSerializingTests::randomResponse),
                null
            );
            case 3 -> {
                if (original.isFailed()) {
                    Failure newFailure = randomValueOtherThan(
                        original.getFailure(),
                        BulkItemResponseFailureWireSerializingTests::randomFailure
                    );
                    yield recreate(original.getItemId(), original.getOpType(), null, newFailure);
                } else {
                    yield recreate(original.getItemId(), original.getOpType(), null, randomFailure());
                }
            }
            default -> throw new AssertionError();
        };
        return new BulkItemResponseWrapper(mutated);
    }

    /**
     * Wrapper around {@link BulkItemResponse} used exclusively for wire-serialization tests.
     * <p>
     * {@link AbstractWireSerializingTestCase} requires test instances to be
     * {@link Writeable} and comparable via {@code equals}/{@code hashCode()} in
     * order to support mutation testing and instance comparison. {@link BulkItemResponse}
     * itself does not provide suitable equality semantics due to its polymorphic
     * structure and embedded {@link Exception} instances.
     * <p>
     * This wrapper supplies a stable, semantic equality definition that compares
     * only the meaningful identifying fields of a {@link BulkItemResponse},
     * delegating deep comparison of responses and failures to test-specific logic.
     * This keeps production code free of test-driven equality requirements while
     * enabling thorough wire-serialization coverage.
     */
    static final class BulkItemResponseWrapper implements Writeable {
        private final BulkItemResponse response;

        BulkItemResponseWrapper(BulkItemResponse response) {
            this.response = response;
        }

        BulkItemResponseWrapper(StreamInput in) throws IOException {
            this.response = new BulkItemResponse(in);
        }

        BulkItemResponse response() {
            return response;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            response.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BulkItemResponseWrapper that = (BulkItemResponseWrapper) o;
            return bulkItemResponsesEqual(response, that.response);
        }

        @Override
        public int hashCode() {
            return Objects.hash(response.getItemId(), response.getOpType(), response.isFailed());
        }
    }

    private static boolean bulkItemResponsesEqual(BulkItemResponse e, BulkItemResponse a) {
        if (e.getItemId() != a.getItemId()) return false;
        if (e.getOpType() != a.getOpType()) return false;
        if (e.isFailed() != a.isFailed()) return false;
        if (e.isFailed()) {
            return failuresEqual(e.getFailure(), a.getFailure());
        } else {
            return responsesEqual(e.getResponse(), a.getResponse());
        }
    }

    private static boolean failuresEqual(Failure e, Failure a) {
        if (e == null || a == null) return e == a;
        return BulkItemResponseFailureWireSerializingTests.FailureWrapper.failuresEqual(e, a);
    }

    private static boolean responsesEqual(DocWriteResponse e, DocWriteResponse a) {
        return e.getClass().equals(a.getClass())
            && Objects.equals(e.getIndex(), a.getIndex())
            && Objects.equals(e.getId(), a.getId())
            && e.getSeqNo() == a.getSeqNo()
            && e.getPrimaryTerm() == a.getPrimaryTerm()
            && e.getVersion() == a.getVersion()
            && e.getResult() == a.getResult()
            && Objects.equals(e.getShardId(), a.getShardId());
    }

    private static BulkItemResponse recreate(int itemId, OpType opType, DocWriteResponse response, Failure failure) {
        return failure != null ? BulkItemResponse.failure(itemId, opType, failure) : BulkItemResponse.success(itemId, opType, response);
    }

    private static DocWriteResponse randomResponse() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(5, 10), randomIntBetween(0, 10));
        return switch (randomIntBetween(0, 3)) {
            case 0 -> {
                IndexResponse r = new IndexResponse(
                    shardId,
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomBoolean()
                );
                setSafeShardInfo(r);
                yield r;
            }
            case 1 -> {
                DeleteResponse r = new DeleteResponse(
                    shardId,
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomBoolean()
                );
                setSafeShardInfo(r);
                yield r;
            }
            case 2 -> {
                UpdateResponse r = new UpdateResponse(
                    shardId,
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomFrom(DocWriteResponse.Result.values())
                );
                setSafeShardInfo(r);
                yield r;
            }
            case 3 -> {
                boolean hasSource = randomBoolean();
                BytesArray source = hasSource ? new BytesArray(randomAlphaOfLengthBetween(10, 100)) : null;
                XContentType xContentType = randomFrom(XContentType.values());
                SimulateIndexResponse response = new SimulateIndexResponse(
                    randomAlphaOfLengthBetween(3, 10),
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    source,
                    xContentType,
                    randomBoolean() ? randomList(0, 5, () -> randomAlphaOfLengthBetween(3, 10)) : List.of(),
                    randomBoolean() ? randomList(0, 5, () -> randomAlphaOfLengthBetween(3, 10)) : List.of(),
                    randomBoolean() ? randomException() : null,
                    null
                );
                setSafeShardInfo(response);
                yield response;
            }
            default -> throw new AssertionError();
        };
    }

    private static void setSafeShardInfo(DocWriteResponse response) {
        response.setShardInfo(ReplicationResponse.ShardInfo.of(1, 1));
    }
}
