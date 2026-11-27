/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.commits.MergeBlobTests;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.stateless.commits.MergeBlobTests.randomMergeBlob;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomShardId;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomTimestampFieldValueRangeOrNull;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NewMergeBlobNotificationRequestTests extends AbstractWireSerializingTestCase<NewMergeBlobNotificationRequestTests.Wrapper> {

    @Override
    protected Writeable.Reader<Wrapper> instanceReader() {
        return Wrapper::new;
    }

    @Override
    protected Wrapper createTestInstance() {
        final var request = new NewMergeBlobNotificationRequest(
            indexShardRoutingTable(randomShardId()),
            randomMergeBlob(),
            randomMergeBlobLengthBytes(),
            randomTimestampFieldValueRangeOrNull()
        );
        assertThat(request.validate(), nullValue());
        return new Wrapper(request);
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) {
        final var request = instance.request;
        final var shardRoutingTable = indexShardRoutingTable(request.shardId());
        return new Wrapper(switch (between(0, 3)) {
            case 0 -> new NewMergeBlobNotificationRequest(
                indexShardRoutingTable(randomValueOtherThan(request.shardId(), StatelessCompoundCommitTestUtils::randomShardId)),
                request.getMergeBlob(),
                request.getMergeBlobLengthBytes(),
                request.getTimestampFieldValueRange()
            );
            case 1 -> new NewMergeBlobNotificationRequest(
                shardRoutingTable,
                randomValueOtherThan(request.getMergeBlob(), MergeBlobTests::randomMergeBlob),
                request.getMergeBlobLengthBytes(),
                request.getTimestampFieldValueRange()
            );
            case 2 -> new NewMergeBlobNotificationRequest(
                shardRoutingTable,
                request.getMergeBlob(),
                randomValueOtherThan(request.getMergeBlobLengthBytes(), NewMergeBlobNotificationRequestTests::randomMergeBlobLengthBytes),
                request.getTimestampFieldValueRange()
            );
            case 3 -> new NewMergeBlobNotificationRequest(
                shardRoutingTable,
                request.getMergeBlob(),
                request.getMergeBlobLengthBytes(),
                randomValueOtherThan(
                    request.getTimestampFieldValueRange(),
                    StatelessCompoundCommitTestUtils::randomTimestampFieldValueRangeOrNull
                )
            );
            default -> throw new AssertionError("unexpected mutation");
        });
    }

    public void testValidationFailsOnNonPositiveLength() {
        final long mergeBlobLengthBytes = randomLongBetween(Long.MIN_VALUE, 0L);
        final var request = new NewMergeBlobNotificationRequest(
            indexShardRoutingTable(randomShardId()),
            randomMergeBlob(),
            mergeBlobLengthBytes,
            randomTimestampFieldValueRangeOrNull()
        );
        final var validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(
            validationException.validationErrors(),
            equalTo(List.of("merge blob length [" + mergeBlobLengthBytes + "] must be positive"))
        );
    }

    public void testDoesNotFailShardOnError() {
        assertThat(createTestInstance().request.failShardOnError(), is(false));
    }

    /// Wraps a [NewMergeBlobNotificationRequest] to add the equality checks the request does not have.
    static final class Wrapper implements Writeable {
        private final NewMergeBlobNotificationRequest request;

        Wrapper(NewMergeBlobNotificationRequest request) {
            this.request = request;
        }

        Wrapper(StreamInput in) throws IOException {
            this.request = new NewMergeBlobNotificationRequest(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            request.writeTo(out);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            final var otherRequest = ((Wrapper) other).request;
            return Objects.equals(request.shardId(), otherRequest.shardId())
                && request.failShardOnError() == otherRequest.failShardOnError()
                && Objects.equals(request.getMergeBlob(), otherRequest.getMergeBlob())
                && request.getMergeBlobLengthBytes() == otherRequest.getMergeBlobLengthBytes()
                && Objects.equals(request.getTimestampFieldValueRange(), otherRequest.getTimestampFieldValueRange());
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                request.shardId(),
                request.failShardOnError(),
                request.getMergeBlob(),
                request.getMergeBlobLengthBytes(),
                request.getTimestampFieldValueRange()
            );
        }

        @Override
        public String toString() {
            return request.toString();
        }
    }

    private static IndexShardRoutingTable indexShardRoutingTable(ShardId shardId) {
        return new IndexShardRoutingTable.Builder(shardId).addShard(
            TestShardRouting.newShardRouting(shardId, null, true, ShardRoutingState.UNASSIGNED)
        ).build();
    }

    private static long randomMergeBlobLengthBytes() {
        return randomLongBetween(1L, Long.MAX_VALUE);
    }
}
