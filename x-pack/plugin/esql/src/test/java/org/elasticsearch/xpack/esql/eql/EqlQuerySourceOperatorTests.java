/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link EqlQuerySourceOperator}: they drive the operator against a stub {@link EqlQueryService} that
 * returns a canned {@link EqlSearchResponse} (or a failure) synchronously, and assert that the response is flattened
 * into the fixed {@code _sequence, _index, _id, _source} columns. A real {@link EqlSearchResponse} is used (it is
 * ref-counted with a leak tracker); the stub service releases it after delivery, mirroring the transport layer, and
 * the operator must not release it itself.
 */
public class EqlQuerySourceOperatorTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testEventQuery() {
        EqlSearchResponse response = new EqlSearchResponse(
            new EqlSearchResponse.Hits(List.of(event("index-1", "id-1", "{\"a\":1}"), event("index-2", "id-2", "{\"a\":2}")), null, null),
            5,
            false,
            ShardSearchFailure.EMPTY_ARRAY
        );

        Page page = runToPage(response);
        assertThat(page.getPositionCount(), equalTo(2));
        LongBlock sequences = page.getBlock(0);
        // Plain event queries have no sequence ordinal.
        assertTrue(sequences.isNull(0));
        assertTrue(sequences.isNull(1));
        assertThat(index(page, 0), equalTo("index-1"));
        assertThat(id(page, 0), equalTo("id-1"));
        assertThat(source(page, 0), equalTo("{\"a\":1}"));
        assertThat(index(page, 1), equalTo("index-2"));
        assertThat(id(page, 1), equalTo("id-2"));
        assertThat(source(page, 1), equalTo("{\"a\":2}"));
        page.releaseBlocks();
    }

    public void testSequenceQuery() {
        EqlSearchResponse response = new EqlSearchResponse(
            new EqlSearchResponse.Hits(
                null,
                List.of(
                    new EqlSearchResponse.Sequence(List.of("k1"), List.of(event("i0", "a", "{}"), event("i1", "b", "{}"))),
                    new EqlSearchResponse.Sequence(List.of("k2"), List.of(event("i2", "c", "{}")))
                ),
                null
            ),
            5,
            false,
            ShardSearchFailure.EMPTY_ARRAY
        );

        Page page = runToPage(response);
        assertThat(page.getPositionCount(), equalTo(3));
        LongBlock sequences = page.getBlock(0);
        // Rows carry the 0-based ordinal of the sequence they belong to.
        assertThat(sequences.getLong(sequences.getFirstValueIndex(0)), equalTo(0L));
        assertThat(sequences.getLong(sequences.getFirstValueIndex(1)), equalTo(0L));
        assertThat(sequences.getLong(sequences.getFirstValueIndex(2)), equalTo(1L));
        assertThat(id(page, 0), equalTo("a"));
        assertThat(id(page, 2), equalTo("c"));
        page.releaseBlocks();
    }

    public void testEmptyResult() {
        EqlSearchResponse response = new EqlSearchResponse(EqlSearchResponse.Hits.EMPTY, 0, false, ShardSearchFailure.EMPTY_ARRAY);
        Page page = runToPage(response);
        assertThat(page.getPositionCount(), equalTo(0));
        page.releaseBlocks();
    }

    public void testFailureIsPropagated() {
        EqlQueryService service = new EqlQueryService(null, null) {
            @Override
            public void query(
                String index,
                String query,
                @Nullable Integer size,
                CancellableTask parentTask,
                ActionListener<EqlSearchResponse> listener
            ) {
                listener.onFailure(new IllegalStateException("boom"));
            }
        };
        EqlQuerySourceOperator operator = new EqlQuerySourceOperator(blockFactory, service, "idx", "any where true", null, null);
        assertThat(operator.isBlocked().listener().isDone(), is(true));
        // A pending failure keeps the operator "not finished" so the driver polls getOutput() and sees the exception.
        assertThat(operator.isFinished(), is(false));
        IllegalStateException e = expectThrows(IllegalStateException.class, operator::getOutput);
        assertThat(e.getMessage(), equalTo("boom"));
        operator.close();
    }

    public void testLimitIsForwardedAsSize() {
        AtomicReference<Integer> receivedSize = new AtomicReference<>();
        EqlSearchResponse response = new EqlSearchResponse(EqlSearchResponse.Hits.EMPTY, 0, false, ShardSearchFailure.EMPTY_ARRAY);
        EqlQueryService service = new EqlQueryService(null, null) {
            @Override
            public void query(
                String index,
                String query,
                @Nullable Integer size,
                CancellableTask parentTask,
                ActionListener<EqlSearchResponse> listener
            ) {
                receivedSize.set(size);
                try {
                    listener.onResponse(response);
                } finally {
                    response.decRef();
                }
            }
        };
        EqlQuerySourceOperator operator = new EqlQuerySourceOperator(blockFactory, service, "idx", "any where true", 7, null);
        assertThat(operator.isBlocked().listener().isDone(), is(true));
        operator.getOutput().releaseBlocks();
        operator.close();
        assertThat(receivedSize.get(), equalTo(7));
    }

    public void testNoLimitForwardsNullSize() {
        AtomicReference<Integer> receivedSize = new AtomicReference<>(-1);
        EqlSearchResponse response = new EqlSearchResponse(EqlSearchResponse.Hits.EMPTY, 0, false, ShardSearchFailure.EMPTY_ARRAY);
        EqlQueryService service = new EqlQueryService(null, null) {
            @Override
            public void query(
                String index,
                String query,
                @Nullable Integer size,
                CancellableTask parentTask,
                ActionListener<EqlSearchResponse> listener
            ) {
                receivedSize.set(size);
                try {
                    listener.onResponse(response);
                } finally {
                    response.decRef();
                }
            }
        };
        EqlQuerySourceOperator operator = new EqlQuerySourceOperator(blockFactory, service, "idx", "any where true", null, null);
        assertThat(operator.isBlocked().listener().isDone(), is(true));
        operator.getOutput().releaseBlocks();
        operator.close();
        assertThat(receivedSize.get(), nullValue());
    }

    private Page runToPage(EqlSearchResponse response) {
        EqlQueryService service = new EqlQueryService(null, null) {
            @Override
            public void query(
                String index,
                String query,
                @Nullable Integer size,
                CancellableTask parentTask,
                ActionListener<EqlSearchResponse> listener
            ) {
                // Mirror the transport contract: deliver the response, then release it (the operator must not).
                try {
                    listener.onResponse(response);
                } finally {
                    response.decRef();
                }
            }
        };
        EqlQuerySourceOperator operator = new EqlQuerySourceOperator(blockFactory, service, "idx", "any where true", null, null);
        // Firing the (synchronous) request happens on the first isBlocked() poll; the listener completes immediately.
        assertThat(operator.isBlocked().listener().isDone(), is(true));
        Page page = operator.getOutput();
        assertThat(operator.isFinished(), is(true));
        operator.close();
        return page;
    }

    private static EqlSearchResponse.Event event(String index, String id, String source) {
        return new EqlSearchResponse.Event(index, id, new BytesArray(source), null, false);
    }

    private static String index(Page page, int position) {
        return bytesRef(page.getBlock(1), position);
    }

    private static String id(Page page, int position) {
        return bytesRef(page.getBlock(2), position);
    }

    private static String source(Page page, int position) {
        return bytesRef(page.getBlock(3), position);
    }

    private static String bytesRef(BytesRefBlock block, int position) {
        return block.getBytesRef(block.getFirstValueIndex(position), new BytesRef()).utf8ToString();
    }
}
