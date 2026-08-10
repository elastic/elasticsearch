/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Decodes remote fetch handles into a doc-location page suitable for
 * {@link org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator}.
 * <p>
 * This is a strict one-to-one page mapping: each input page produces exactly one output page, so the operator never
 * accumulates pages. Input pages contain exactly one bytes block with one serialized {@link RemoteFetchHandle} per row.
 * Output pages contain:
 * <ul>
 *     <li>channel 0: doc block (shard, segment, doc)</li>
 *     <li>channel 1 (optional): position mapping used by pushdown filtering</li>
 * </ul>
 */
final class RemoteFetchHandleDecodeOperator extends AbstractPageMappingOperator {
    record Factory(boolean includePositionMapping) implements Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new RemoteFetchHandleDecodeOperator(driverContext.blockFactory(), includePositionMapping);
        }

        @Override
        public String describe() {
            return "RemoteFetchHandleDecodeOperator[include_position_mapping=" + includePositionMapping + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final boolean includePositionMapping;

    RemoteFetchHandleDecodeOperator(BlockFactory blockFactory, boolean includePositionMapping) {
        this.blockFactory = blockFactory;
        this.includePositionMapping = includePositionMapping;
    }

    @Override
    protected Page process(Page page) {
        Page decoded = decodeHandles(page);
        page.releaseBlocks();
        return decoded;
    }

    @Override
    public String toString() {
        return "RemoteFetchHandleDecodeOperator[include_position_mapping=" + includePositionMapping + "]";
    }

    private Page decodeHandles(Page page) {
        if (page.getBlockCount() != 1) {
            throw new IllegalStateException("expected a single handle block but got [" + page.getBlockCount() + "]");
        }
        BytesRefBlock handlesBlock = page.getBlock(0);
        BytesRef scratch = new BytesRef();
        // All handles in one batch must target the same node and retained session. Instead of materializing the two
        // strings for every row, decode them once from the first handle and compare the serialized prefix bytes of
        // every subsequent handle.
        BytesRef expectedTargetSessionPrefix = null;
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
                BytesRef handleBytes = handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch);
                if (expectedTargetSessionPrefix == null) {
                    RemoteFetchHandle first = RemoteFetchHandle.fromBytesRef(handleBytes);
                    expectedTargetSessionPrefix = RemoteFetchHandle.encodeTargetSessionPrefix(first.nodeId(), first.retainedSessionId());
                    docBuilder.append(first.shard(), first.segment(), first.doc());
                } else if (RemoteFetchHandle.startsWithTargetSessionPrefix(handleBytes, expectedTargetSessionPrefix)) {
                    appendDocAfterPrefix(docBuilder, handleBytes, expectedTargetSessionPrefix.length);
                } else {
                    RemoteFetchHandle expected = RemoteFetchHandle.fromBytesRef(
                        handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(0), scratch)
                    );
                    RemoteFetchHandle actual = RemoteFetchHandle.fromBytesRef(
                        handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch)
                    );
                    throw new IllegalStateException(
                        "remote fetch batch must contain handles from a single target session but saw ["
                            + expected.nodeId()
                            + "/"
                            + expected.retainedSessionId()
                            + "] and ["
                            + actual.nodeId()
                            + "/"
                            + actual.retainedSessionId()
                            + "]"
                    );
                }
                if (positionBuilder != null) {
                    positionBuilder.appendInt(position);
                }
            }
            Block docBlock = docBuilder.build(DocVector.config()).asBlock();
            if (positionBuilder == null) {
                return new Page(docBlock);
            }
            Block positionBlock;
            try {
                positionBlock = positionBuilder.build();
            } catch (Exception e) {
                Releasables.closeExpectNoException(docBlock);
                throw e;
            }
            return new Page(docBlock, positionBlock);
        }
    }

    private static void appendDocAfterPrefix(DocVector.FixedBuilder docBuilder, BytesRef handleBytes, int prefixLength) {
        try (StreamInput in = StreamInput.wrap(handleBytes.bytes, handleBytes.offset + prefixLength, handleBytes.length - prefixLength)) {
            docBuilder.append(in.readVInt(), in.readVInt(), in.readVInt());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode remote fetch handle", e);
        }
    }
}
