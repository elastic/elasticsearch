/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/**
 * Decodes remote fetch handles into a doc-location page suitable for {@link org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator}.
 * <p>
 * Input pages contain exactly one bytes block with one serialized {@link RemoteFetchHandle} per row.
 * Output pages contain:
 * <ul>
 *     <li>channel 0: doc block (shard, segment, doc)</li>
 *     <li>channel 1 (optional): position mapping used by pushdown filtering</li>
 * </ul>
 */
final class RemoteFetchHandleDecodeOperator implements Operator {
    private final BlockFactory blockFactory;
    private final boolean includePositionMapping;
    private final Deque<Page> outputQueue = new ArrayDeque<>();
    private boolean finished;
    private Exception failure;
    private int pagesReceived;
    private int pagesEmitted;
    private long rowsReceived;
    private long rowsEmitted;

    RemoteFetchHandleDecodeOperator(BlockFactory blockFactory, boolean includePositionMapping) {
        this.blockFactory = blockFactory;
        this.includePositionMapping = includePositionMapping;
    }

    @Override
    public boolean needsInput() {
        return finished == false && failure == null;
    }

    @Override
    public void addInput(Page page) {
        try {
            if (failure != null) {
                return;
            }
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            Page decoded = decodeHandles(page);
            outputQueue.add(decoded);
        } catch (Exception e) {
            failure = e;
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return (finished || failure != null) && outputQueue.isEmpty();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return outputQueue.isEmpty() == false || failure != null;
    }

    @Override
    public Page getOutput() {
        if (failure == null) {
            Page page = outputQueue.pollFirst();
            if (page != null) {
                pagesEmitted++;
                rowsEmitted += page.getPositionCount();
            }
            return page;
        }
        Exception e = failure;
        failure = null;
        if (e instanceof RuntimeException re) {
            throw re;
        }
        throw new IllegalStateException("remote fetch handle decode failed", e);
    }

    @Override
    public void close() {
        for (Page page : outputQueue) {
            Releasables.closeExpectNoException(page::releaseBlocks);
        }
        outputQueue.clear();
    }

    @Override
    public Operator.Status status() {
        return new Status(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
    }

    private Page decodeHandles(Page page) {
        if (page.getBlockCount() != 1) {
            throw new IllegalStateException("expected a single handle block but got [" + page.getBlockCount() + "]");
        }
        BytesRefBlock handlesBlock = page.getBlock(0);
        BytesRef scratch = new BytesRef();
        String expectedNodeId = null;
        String expectedSessionId = null;
        try (
            DocVector.FixedBuilder docBuilder = DocVector.newFixedBuilder(blockFactory, page.getPositionCount());
            IntBlock.Builder positionBuilder = includePositionMapping ? blockFactory.newIntBlockBuilder(page.getPositionCount()) : null
        ) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (handlesBlock.isNull(position)) {
                    throw new IllegalStateException("remote fetch handle block cannot contain nulls");
                }
                if (handlesBlock.getValueCount(position) != 1) {
                    throw new IllegalStateException("remote fetch handle block must have exactly one value per row");
                }
                RemoteFetchHandle handle = RemoteFetchHandle.fromBytesRef(
                    handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch)
                );
                if (expectedNodeId == null) {
                    expectedNodeId = handle.nodeId();
                    expectedSessionId = handle.retainedSessionId();
                } else if (expectedNodeId.equals(handle.nodeId()) == false
                    || expectedSessionId.equals(handle.retainedSessionId()) == false) {
                        throw new IllegalStateException(
                            "remote fetch batch must contain handles from a single target session but saw ["
                                + expectedNodeId
                                + "/"
                                + expectedSessionId
                                + "] and ["
                                + handle.nodeId()
                                + "/"
                                + handle.retainedSessionId()
                                + "]"
                        );
                    }
                docBuilder.append(handle.shard(), handle.segment(), handle.doc());
                if (positionBuilder != null) {
                    positionBuilder.appendInt(position);
                }
            }
            Block docBlock = docBuilder.build(DocVector.config()).asBlock();
            if (positionBuilder == null) {
                return new Page(docBlock);
            }
            return new Page(docBlock, positionBuilder.build());
        }
    }

    public record Status(int pagesReceived, int pagesEmitted, long rowsReceived, long rowsEmitted) implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "remote_fetch_handle_decode",
            Status::new
        );
        private static final TransportVersion REMOTE_FETCH_HANDLE_DECODE_STATUS = TransportVersion.fromName(
            "remote_fetch_handle_decode_status"
        );

        Status(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVInt(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesReceived);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return REMOTE_FETCH_HANDLE_DECODE_STATUS;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_received", pagesReceived);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Status status = (Status) o;
            return pagesReceived == status.pagesReceived
                && pagesEmitted == status.pagesEmitted
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
        }
    }
}
