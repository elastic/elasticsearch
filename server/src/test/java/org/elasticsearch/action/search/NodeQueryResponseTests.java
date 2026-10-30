/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction.NodeQueryResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.nullValue;

public class NodeQueryResponseTests extends ESTestCase {

    public void testLastDecRefReleasesAggregationsOfUntakenMergeResult() throws IOException {
        final var buffer = new TrackedBuffer(writeSuccessfulResponse());
        final var response = buffer.read();
        buffer.releaseInitialReference();
        buffer.assertNotReleased();

        response.incRef();
        assertFalse(response.decRef());
        buffer.assertNotReleased();

        assertTrue(response.decRef());
        buffer.assertReleased();
    }

    public void testTakenMergeResultIsOwnedByTheCaller() throws IOException {
        final var buffer = new TrackedBuffer(writeSuccessfulResponse());
        final var response = buffer.read();
        buffer.releaseInitialReference();

        final var mergeResult = response.takeMergeResult();
        assertNotNull(mergeResult);
        assertNotNull(mergeResult.reducedAggs());
        assertThat(response.takeMergeResult(), nullValue());

        assertTrue(response.decRef());
        buffer.assertNotReleased();

        mergeResult.reducedAggs().close();
        buffer.assertReleased();
    }

    public void testResponseWithReductionFailureHoldsNoMergeResult() throws IOException {
        final var buffer = new TrackedBuffer(writeReductionFailureResponse());
        final var response = buffer.read();
        buffer.releaseInitialReference();

        assertNotNull(response.getReductionFailure());
        assertThat(response.takeMergeResult(), nullValue());
        assertTrue(response.decRef());
        buffer.assertReleased();
    }

    private static BytesReference writeSuccessfulResponse() throws IOException {
        final var aggs = InternalAggregations.from(
            List.of(
                new Min("min_ts", randomDouble(), DocValueFormat.RAW, Map.of()),
                new Max("max_ts", randomDouble(), DocValueFormat.RAW, Map.of())
            )
        );
        final var mergeResult = new QueryPhaseResultConsumer.MergeResult(
            List.of(),
            new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
            DelayableWriteable.referencing(aggs),
            randomNonNegativeLong()
        );
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            out.writeVInt(0);
            out.writeBoolean(false);
            mergeResult.writeTo(out);
            new SearchPhaseController.TopDocsStats(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO).writeTo(out);
            return out.copyBytes();
        }
    }

    private static BytesReference writeReductionFailureResponse() throws IOException {
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            out.writeVInt(0);
            out.writeBoolean(true);
            out.writeException(new IllegalStateException("reduction failed"));
            return out.copyBytes();
        }
    }

    /**
     * Stands in for the pooled network buffer a response is read from: it starts with the reference that the transport layer holds
     * while it reads, and records whether it has been released.
     */
    private static final class TrackedBuffer {
        private final AtomicBoolean released = new AtomicBoolean();
        private final ReleasableBytesReference bytes;

        TrackedBuffer(BytesReference content) {
            this.bytes = new ReleasableBytesReference(
                content,
                AbstractRefCounted.of(() -> assertTrue(released.compareAndSet(false, true)))
            );
        }

        NodeQueryResponse read() throws IOException {
            final StreamInput in = bytes.streamInput();
            in.setTransportVersion(TransportVersion.current());
            return new NodeQueryResponse(in);
        }

        void releaseInitialReference() {
            bytes.decRef();
        }

        void assertReleased() {
            assertTrue(released.get());
        }

        void assertNotReleased() {
            assertFalse(released.get());
        }
    }
}
