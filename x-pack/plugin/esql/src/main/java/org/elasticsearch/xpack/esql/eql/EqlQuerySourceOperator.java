/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Source operator that materializes the result of an EQL search into a single {@link Page}.
 * <p>
 * On the first poll it fires the async EQL request through {@link EqlQueryService} and reports itself blocked; when the
 * response arrives the events (or sequences) are flattened into rows on the transport thread and the operator unblocks.
 * The driver then builds the columnar page. Because EQL results are bounded and assembled on the coordinator, a single
 * page is sufficient. Columns, in output order: {@code _sequence} (long, null for event queries), {@code _index},
 * {@code _id}, {@code _source} (all keyword).
 */
public class EqlQuerySourceOperator extends SourceOperator {

    /** Flattened event row extracted off the transport thread so the {@link EqlSearchResponse} can be released early. */
    private record Row(Long sequence, BytesRef index, BytesRef id, BytesRef source) {}

    public record Factory(EqlQueryService service, String index, String query, @Nullable Integer size, CancellableTask parentTask)
        implements
            SourceOperatorFactory {
        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new EqlQuerySourceOperator(driverContext.blockFactory(), service, index, query, size, parentTask);
        }

        @Override
        public String describe() {
            return "EqlQuerySourceOperator[index=" + index + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final EqlQueryService service;
    private final String index;
    private final String query;
    @Nullable
    private final Integer size;
    private final CancellableTask parentTask;

    private boolean requestSent;
    private boolean finished;
    private volatile boolean closed;
    private IsBlockedResult blocked = NOT_BLOCKED;
    private volatile List<Row> rows;
    private volatile Exception failure;

    public EqlQuerySourceOperator(
        BlockFactory blockFactory,
        EqlQueryService service,
        String index,
        String query,
        @Nullable Integer size,
        CancellableTask parentTask
    ) {
        this.blockFactory = blockFactory;
        this.service = service;
        this.index = index;
        this.query = query;
        this.size = size;
        this.parentTask = parentTask;
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (requestSent == false) {
            requestSent = true;
            SubscribableListener<Void> listener = new SubscribableListener<>();
            blocked = new IsBlockedResult(listener, "waiting for EQL response");
            service.query(index, query, size, parentTask, ActionListener.wrap(response -> {
                // The row data is copied into owned BytesRefs (see toRow), so we must finish reading the response
                // synchronously here: the transport layer owns the ref-counted response and releases it once this
                // listener returns, so the operator must NOT decRef it itself.
                if (closed == false) {
                    rows = flatten(response);
                }
                listener.onResponse(null);
            }, e -> {
                failure = e;
                listener.onResponse(null);
            }));
        }
        return blocked;
    }

    @Override
    public Page getOutput() {
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RuntimeException(failure);
        }
        List<Row> current = rows;
        if (current == null) {
            // The request is still in flight; the driver will re-poll after isBlocked() unblocks.
            return null;
        }
        finished = true;
        return buildPage(current);
    }

    private Page buildPage(List<Row> current) {
        int positions = current.size();
        try (
            LongBlock.Builder sequences = blockFactory.newLongBlockBuilder(positions);
            BytesRefBlock.Builder indices = blockFactory.newBytesRefBlockBuilder(positions);
            BytesRefBlock.Builder ids = blockFactory.newBytesRefBlockBuilder(positions);
            BytesRefBlock.Builder sources = blockFactory.newBytesRefBlockBuilder(positions)
        ) {
            for (Row row : current) {
                if (row.sequence() == null) {
                    sequences.appendNull();
                } else {
                    sequences.appendLong(row.sequence());
                }
                appendOrNull(indices, row.index());
                appendOrNull(ids, row.id());
                appendOrNull(sources, row.source());
            }
            Block[] blocks = new Block[4];
            boolean success = false;
            try {
                blocks[0] = sequences.build();
                blocks[1] = indices.build();
                blocks[2] = ids.build();
                blocks[3] = sources.build();
                Page page = new Page(blocks);
                success = true;
                return page;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
        }
    }

    private static void appendOrNull(BytesRefBlock.Builder builder, BytesRef value) {
        if (value == null) {
            builder.appendNull();
        } else {
            builder.appendBytesRef(value);
        }
    }

    private static List<Row> flatten(EqlSearchResponse response) {
        List<Row> rows = new ArrayList<>();
        EqlSearchResponse.Hits hits = response.hits();
        if (hits == null) {
            return rows;
        }
        List<EqlSearchResponse.Sequence> sequences = hits.sequences();
        if (sequences != null) {
            long sequenceOrdinal = 0;
            for (EqlSearchResponse.Sequence sequence : sequences) {
                for (EqlSearchResponse.Event event : sequence.events()) {
                    rows.add(toRow(sequenceOrdinal, event));
                }
                sequenceOrdinal++;
            }
            return rows;
        }
        List<EqlSearchResponse.Event> events = hits.events();
        if (events != null) {
            for (EqlSearchResponse.Event event : events) {
                rows.add(toRow(null, event));
            }
        }
        return rows;
    }

    private static Row toRow(Long sequenceOrdinal, EqlSearchResponse.Event event) {
        BytesRef index = event.index() == null ? null : new BytesRef(event.index());
        BytesRef id = event.id() == null ? null : new BytesRef(event.id());
        BytesReference source = event.source();
        BytesRef sourceRef = source == null ? null : new BytesRef(source.utf8ToString());
        return new Row(sequenceOrdinal, index, id, sourceRef);
    }

    @Override
    public boolean isFinished() {
        // Keep reporting "not finished" while a failure is pending so the driver calls getOutput() and sees the
        // exception, even if finish() was invoked during teardown.
        return finished && failure == null;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public String toString() {
        return "EqlQuerySourceOperator[index=" + index + "]";
    }
}
