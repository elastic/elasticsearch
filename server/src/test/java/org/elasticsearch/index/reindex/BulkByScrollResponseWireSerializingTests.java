/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.IndexDocFailureStoreStatus;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BulkByScrollResponseWireSerializingTests extends AbstractWireSerializingTestCase<
    BulkByScrollResponseWireSerializingTests.BulkByScrollResponseWrapper> {
    @Override
    protected Writeable.Reader<BulkByScrollResponseWrapper> instanceReader() {
        return BulkByScrollResponseWrapper::new;
    }

    @Override
    protected BulkByScrollResponseWrapper createTestInstance() {
        return new BulkByScrollResponseWrapper(
            new BulkByScrollResponse(
                randomTimeValue(),
                BulkByScrollTaskStatusTests.randomStatus(),
                randomBulkFailures(),
                randomSearchFailures(),
                randomBoolean()
            )
        );
    }

    @Override
    protected BulkByScrollResponseWrapper mutateInstance(BulkByScrollResponseWrapper instance) {
        BulkByScrollResponse r = instance.response();
        return new BulkByScrollResponseWrapper(switch (between(0, 4)) {
            case 0 -> new BulkByScrollResponse(
                randomValueOtherThan(r.getTook(), ESTestCase::randomTimeValue),
                r.getStatus(),
                r.getBulkFailures(),
                r.getSearchFailures(),
                r.isTimedOut()
            );
            case 1 -> new BulkByScrollResponse(
                r.getTook(),
                mutateRandomStatus(r.getStatus()),
                r.getBulkFailures(),
                r.getSearchFailures(),
                r.isTimedOut()
            );
            case 2 -> new BulkByScrollResponse(
                r.getTook(),
                r.getStatus(),
                mutateBulkFailures(r.getBulkFailures()),
                r.getSearchFailures(),
                r.isTimedOut()
            );
            case 3 -> new BulkByScrollResponse(
                r.getTook(),
                r.getStatus(),
                r.getBulkFailures(),
                mutateSearchFailures(r.getSearchFailures()),
                r.isTimedOut()
            );
            case 4 -> new BulkByScrollResponse(
                r.getTook(),
                r.getStatus(),
                r.getBulkFailures(),
                r.getSearchFailures(),
                r.isTimedOut() == false
            );
            default -> throw new AssertionError();
        });
    }

    private BulkByScrollTask.Status mutateRandomStatus(BulkByScrollTask.Status currentStatus) {
        while (true) {
            BulkByScrollTask.Status candidate = BulkByScrollTaskStatusTests.randomStatus();
            try {
                BulkByScrollTaskStatusTests.assertTaskStatusEquals(currentStatus, candidate);
                // Equal → try again
            } catch (AssertionError e) {
                // Not equal → success
                return candidate;
            }
        }
    }

    private List<Failure> mutateBulkFailures(List<Failure> currentFailures) {
        List<Failure> newFailures = new ArrayList<>(currentFailures);
        newFailures.add(randomFailure());
        return newFailures;
    }

    private List<Failure> randomBulkFailures() {
        return randomList(0, 5, BulkByScrollResponseWireSerializingTests::randomFailure);
    }

    static Failure randomFailure() {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null;
        Exception cause = randomException();
        return randomFailure(
            index,
            id,
            cause,
            randomBoolean() ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_SEQ_NO,
            randomBoolean() ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            randomBoolean(),
            randomFrom(IndexDocFailureStoreStatus.values())
        );
    }

    static Failure randomFailure(
        String index,
        String id,
        Exception cause,
        long seqNo,
        long term,
        boolean aborted,
        IndexDocFailureStoreStatus failureStoreStatus
    ) {
        Failure failure;
        if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO || term != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            failure = new Failure(index, id, cause, seqNo, term);
        } else if (aborted) {
            failure = new Failure(index, id, cause, true);
        } else if (failureStoreStatus != IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN) {
            failure = new Failure(index, id, cause, failureStoreStatus);
        } else {
            failure = new Failure(index, id, cause);
        }
        failure.setFailureStoreStatus(failureStoreStatus);
        return failure;
    }

    static Exception randomException() {
        return randomFrom(
            new IllegalArgumentException(randomAlphaOfLengthBetween(5, 20)),
            new IllegalStateException(randomAlphaOfLengthBetween(5, 20)),
            new ElasticsearchException(randomAlphaOfLengthBetween(5, 20))
        );
    }

    private List<PaginatedHitSource.SearchFailure> randomSearchFailures() {
        return randomList(0, 5, this::randomSearchFailure);
    }

    private List<PaginatedHitSource.SearchFailure> mutateSearchFailures(List<PaginatedHitSource.SearchFailure> searchFailures) {
        List<PaginatedHitSource.SearchFailure> newFailures = new ArrayList<>(searchFailures);
        newFailures.add(randomSearchFailure());
        return newFailures;
    }

    private PaginatedHitSource.SearchFailure randomSearchFailure() {
        Throwable reason = randomException();
        String index = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;
        Integer shardId = randomBoolean() ? randomIntBetween(0, 100) : null;
        String nodeId = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;
        return new PaginatedHitSource.SearchFailure(reason, index, shardId, nodeId);
    }

    /**
     * {@code BulkByScrollResponse} does not implement {@code equals}/{@code hashCode},
     * and its {@link BulkByScrollTask.Status} contains slice-level and implementation
     * details that are not stable for direct equality checks.
     * <p>
     * Equality is defined in terms of wire-relevant state only: top-level fields,
     * aggregated task status counters (via
     * {@link BulkByScrollTaskStatusTests#assertTaskStatusEquals}), and the stable
     * attributes of bulk and search failures. Care must be taken for exceptions, since
     * two messages with the same cause and message would be different instances after
     * serialization / deserialization, and fail the default equality check. For this
     * reason, we define custom equality below.
     */
    static class BulkByScrollResponseWrapper implements Writeable {
        private final BulkByScrollResponse response;

        BulkByScrollResponseWrapper(BulkByScrollResponse response) {
            this.response = response;
        }

        BulkByScrollResponseWrapper(StreamInput in) throws IOException {
            this.response = new BulkByScrollResponse(in);
        }

        BulkByScrollResponse response() {
            return response;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            response.writeTo(out);
        }

        @Override
        public String toString() {
            return response.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BulkByScrollResponseWrapper that = (BulkByScrollResponseWrapper) o;
            return responsesEqual(response, that.response);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                response.getTook(),
                response.isTimedOut(),
                response.getCreated(),
                response.getTotal(),
                response.getDeleted(),
                response.getUpdated(),
                response.getBatches(),
                response.getVersionConflicts(),
                response.getNoops(),
                response.getBulkRetries(),
                response.getSearchRetries(),
                response.getReasonCancelled(),
                response.getBulkFailures()
                    .stream()
                    .map(f -> Objects.hash(f.getIndex(), f.getId(), f.getStatus(), f.getCause().getClass()))
                    .toList(),
                response.getSearchFailures()
                    .stream()
                    .map(f -> Objects.hash(f.getIndex(), f.getShardId(), f.getNodeId(), f.getReason().getClass(), f.getStatus()))
                    .toList()
            );
        }

    }

    private static boolean responsesEqual(BulkByScrollResponse a, BulkByScrollResponse b) {
        if (a.getTook().equals(b.getTook()) == false) return false;

        try {
            BulkByScrollTaskStatusTests.assertTaskStatusEquals(a.getStatus(), b.getStatus());
            // Equal → skip to next check
        } catch (AssertionError e) {
            // Assertion error → not equal
            return false;
        }

        if (a.getBulkFailures().size() != b.getBulkFailures().size()) return false;
        for (int i = 0; i < a.getBulkFailures().size(); i++) {
            Failure fa = a.getBulkFailures().get(i);
            Failure fb = b.getBulkFailures().get(i);
            if (Objects.equals(fa.getIndex(), fb.getIndex()) == false) return false;
            if (Objects.equals(fa.getId(), fb.getId()) == false) return false;
            if (Objects.equals(fa.getStatus(), fb.getStatus()) == false) return false;
            if (Objects.equals(fa.getCause().getClass(), fb.getCause().getClass()) == false) return false;
        }

        if (a.getSearchFailures().size() != b.getSearchFailures().size()) return false;
        for (int i = 0; i < a.getSearchFailures().size(); i++) {
            PaginatedHitSource.SearchFailure fa = a.getSearchFailures().get(i);
            PaginatedHitSource.SearchFailure fb = b.getSearchFailures().get(i);
            if (Objects.equals(fa.getIndex(), fb.getIndex()) == false) return false;
            if (Objects.equals(fa.getShardId(), fb.getShardId()) == false) return false;
            if (Objects.equals(fa.getNodeId(), fb.getNodeId()) == false) return false;
            if (Objects.equals(fa.getReason().getClass(), fb.getReason().getClass()) == false) return false;
            if (Objects.equals(fa.getStatus(), fb.getStatus()) == false) return false;
        }

        return a.isTimedOut() == b.isTimedOut();
    }
}
