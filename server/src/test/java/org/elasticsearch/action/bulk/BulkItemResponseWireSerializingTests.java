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
            case 2 -> {
                if (original.isFailed()) {
                    yield recreate(original.getItemId(), original.getOpType(), randomResponse(), null);
                } else {
                    DocWriteResponse newResponse = randomValueOtherThan(
                        original.getResponse(),
                        BulkItemResponseWireSerializingTests::randomResponse
                    );
                    yield recreate(original.getItemId(), original.getOpType(), newResponse, null);
                }
            }
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
     * Custom semantic equality assertion for wire-serialization testing.
     * <p>
     * {@link AbstractWireSerializingTestCase} verifies that an object survives a
     * serialize/deserialize round-trip without losing meaningful state. For
     * {@link BulkItemResponse}, relying on default equality is insufficient
     * because:
     * <ul>
     *     <li>{@link BulkItemResponse} does not implement {@code equals}/{@code hashCode()}.</li>
     *     <li>The response is polymorphic ({@link DocWriteResponse} subclasses) or a
     *     {@link Failure}, each with different equality requirements.</li>
     *     <li>Failures contain {@link Exception} instances whose equality is based on
     *     identity rather than semantic content.</li>
     * </ul>
     * <p>
     * This method performs a field-by-field comparison of the observable state,
     * asserting semantic equivalence (including response type and exception class
     * and message) while explicitly avoiding identity-based comparisons. This
     * allows reliable verification of wire serialization behavior without
     * imposing production equality semantics on {@link BulkItemResponse}.
     */
    @Override
    protected void assertEqualInstances(BulkItemResponseWrapper expected, BulkItemResponseWrapper actual) {
        BulkItemResponse e = expected.response();
        BulkItemResponse a = actual.response();
        assertNotSame(e, a);
        assertEquals(e.getItemId(), a.getItemId());
        assertEquals(e.getOpType(), a.getOpType());
        assertEquals(e.isFailed(), a.isFailed());
        if (e.isFailed()) {
            assertNull(a.getResponse());
            assertFailuresEqual(e.getFailure(), a.getFailure());
        } else {
            assertNull(a.getFailure());
            assertResponsesEqual(e.getResponse(), a.getResponse());
        }
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
        return Objects.equals(e.getIndex(), a.getIndex())
            && Objects.equals(e.getId(), a.getId())
            && e.getStatus() == a.getStatus()
            && e.isAborted() == a.isAborted()
            && e.getSeqNo() == a.getSeqNo()
            && e.getFailureStoreStatus() == a.getFailureStoreStatus()
            && e.getCause().getClass().equals(a.getCause().getClass())
            && Objects.equals(e.getCause().getMessage(), a.getCause().getMessage());
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

    private static void assertFailuresEqual(Failure expected, Failure actual) {
        assertEquals("index mismatch", expected.getIndex(), actual.getIndex());
        assertEquals("id mismatch", expected.getId(), actual.getId());
        assertEquals("status mismatch", expected.getStatus(), actual.getStatus());
        assertEquals("aborted mismatch", expected.isAborted(), actual.isAborted());
        assertEquals("seqNo mismatch", expected.getSeqNo(), actual.getSeqNo());
        assertEquals("failureStoreStatus mismatch", expected.getFailureStoreStatus(), actual.getFailureStoreStatus());
        // Compare exception class + message only (never instance equality)
        if (expected.getCause() == null) {
            assertNull(actual.getCause());
        } else {
            assertNotNull(actual.getCause());
            assertEquals("exception class mismatch", expected.getCause().getClass(), actual.getCause().getClass());
            assertEquals("exception message mismatch", expected.getCause().getMessage(), actual.getCause().getMessage());
        }
    }

    private static void assertResponsesEqual(DocWriteResponse expected, DocWriteResponse actual) {
        assertEquals("response class mismatch", expected.getClass(), actual.getClass());
        assertEquals("index mismatch", expected.getIndex(), actual.getIndex());
        assertEquals("id mismatch", expected.getId(), actual.getId());
        assertEquals("seqNo mismatch", expected.getSeqNo(), actual.getSeqNo());
        assertEquals("primaryTerm mismatch", expected.getPrimaryTerm(), actual.getPrimaryTerm());
        assertEquals("version mismatch", expected.getVersion(), actual.getVersion());
        assertEquals("result mismatch", expected.getResult(), actual.getResult());
        assertEquals("shardId mismatch", expected.getShardId(), actual.getShardId());
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
