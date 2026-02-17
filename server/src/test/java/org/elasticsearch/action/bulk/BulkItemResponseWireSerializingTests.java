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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

import static org.elasticsearch.action.bulk.BulkItemResponseFailureWireSerializingTests.randomException;
import static org.elasticsearch.action.bulk.BulkItemResponseFailureWireSerializingTests.randomFailure;

public class BulkItemResponseWireSerializingTests extends AbstractWireSerializingTestCase<BulkItemResponse> {
    @Override
    protected BulkItemResponse createTestInstance() {
        int itemId = randomIntBetween(0, 1000);
        OpType opType = randomFrom(OpType.values());
        if (randomBoolean()) {
            return BulkItemResponse.success(itemId, opType, randomResponse());
        } else {
            return BulkItemResponse.failure(itemId, opType, randomFailure());
        }
    }

    @Override
    protected Writeable.Reader<BulkItemResponse> instanceReader() {
        return BulkItemResponse::new;
    }

    @Override
    protected BulkItemResponse mutateInstance(BulkItemResponse instance) {
        int fieldToMutate = randomIntBetween(0, 3);
        switch (fieldToMutate) {
            case 0 -> {
                int newId = randomValueOtherThan(instance.getItemId(), () -> randomIntBetween(0, 1000));
                return recreate(instance, newId, instance.getOpType(), instance.getResponse(), instance.getFailure());
            }
            case 1 -> {
                OpType newOpType = randomValueOtherThan(instance.getOpType(), () -> randomFrom(OpType.values()));
                return recreate(instance, instance.getItemId(), newOpType, instance.getResponse(), instance.getFailure());
            }
            case 2 -> {
                if (instance.isFailed()) {
                    return recreate(instance, instance.getItemId(), instance.getOpType(), randomResponse(), null);
                } else {
                    DocWriteResponse newResponse = randomValueOtherThan(
                        instance.getResponse(),
                        BulkItemResponseWireSerializingTests::randomResponse
                    );
                    return recreate(instance, instance.getItemId(), instance.getOpType(), newResponse, null);
                }
            }
            case 3 -> {
                if (instance.isFailed()) {
                    Failure newFailure = randomValueOtherThan(
                        instance.getFailure(),
                        BulkItemResponseFailureWireSerializingTests::randomFailure
                    );
                    return recreate(instance, instance.getItemId(), instance.getOpType(), null, newFailure);
                } else {
                    return recreate(instance, instance.getItemId(), instance.getOpType(), null, randomFailure());
                }
            }
            default -> throw new AssertionError("Unknown field index [" + fieldToMutate + "]");
        }
    }

    @Override
    protected void assertEqualInstances(BulkItemResponse expected, BulkItemResponse actual) {
        assertNotSame(expected, actual);
        assertEquals("itemId mismatch", expected.getItemId(), actual.getItemId());
        assertEquals("opType mismatch", expected.getOpType(), actual.getOpType());
        assertEquals("failed flag mismatch", expected.isFailed(), actual.isFailed());
        if (expected.isFailed()) {
            assertNull("actual response should be null", actual.getResponse());
            assertNotNull("actual failure should not be null", actual.getFailure());
            assertFailuresEqual(expected.getFailure(), actual.getFailure());
        } else {
            assertNull("actual failure should be null", actual.getFailure());
            assertNotNull("actual response should not be null", actual.getResponse());
            assertResponsesEqual(expected.getResponse(), actual.getResponse());
        }
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

    private static BulkItemResponse recreate(
        BulkItemResponse original,
        int itemId,
        OpType opType,
        DocWriteResponse response,
        Failure failure
    ) {
        if (failure != null) {
            return BulkItemResponse.failure(itemId, opType, failure);
        }
        return BulkItemResponse.success(itemId, opType, response);
    }

    private static DocWriteResponse randomResponse() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(5, 10), randomIntBetween(0, 10));
        return switch (randomIntBetween(0, 3)) {
            case 0 -> {
                IndexResponse response = new IndexResponse(
                    shardId,
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomBoolean()
                );
                // response.setShardInfo(randomShardInfo(random()).v1());
                setSafeShardInfo(response);
                yield response;
            }
            case 1 -> {
                DeleteResponse response = new DeleteResponse(
                    shardId,
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomBoolean()
                );
                // response.setShardInfo(randomShardInfo(random()).v1());
                setSafeShardInfo(response);
                yield response;
            }
            case 2 -> {
                UpdateResponse response = new UpdateResponse(
                    shardId,
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomFrom(DocWriteResponse.Result.values())
                );
                // response.setShardInfo(randomShardInfo(random()).v1());
                setSafeShardInfo(response);
                yield response;
            }
            case 3 -> {
                SimulateIndexResponse response = new SimulateIndexResponse(
                    randomAlphaOfLengthBetween(3, 10),
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomBoolean() ? new BytesArray(randomAlphaOfLengthBetween(10, 100)) : null,
                    randomBoolean() ? randomFrom(XContentType.values()) : null,
                    randomBoolean() ? randomList(0, 5, () -> randomAlphaOfLengthBetween(3, 10)) : List.of(),
                    randomBoolean() ? randomList(0, 5, () -> randomAlphaOfLengthBetween(3, 10)) : List.of(),
                    randomBoolean() ? randomException() : null,
                    null
                );
                // response.setShardInfo(randomShardInfo(random()).v1());
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
