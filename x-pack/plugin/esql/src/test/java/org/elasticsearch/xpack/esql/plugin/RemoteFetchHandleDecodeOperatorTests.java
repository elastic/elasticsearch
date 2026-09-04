/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.operator.blocksource.BytesRefBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RemoteFetchHandleDecodeOperatorTests extends OperatorTestCase {
    private final boolean includePositionMapping = randomBoolean();

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new RemoteFetchHandleDecodeOperator.Factory(includePositionMapping);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("RemoteFetchHandleDecodeOperator[include_position_mapping=" + includePositionMapping + "]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<BytesRef> handles = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // Doc positions are unique per handle, matching production where each handle points at a distinct doc.
            handles.add(new RemoteFetchHandle("node-1", "session-1", between(0, 10), between(0, 100), i).toBytesRef());
        }
        return new BytesRefBlockSourceOperator(blockFactory, handles);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results.size(), equalTo(input.size()));
        BytesRef scratch = new BytesRef();
        for (int pageIndex = 0; pageIndex < input.size(); pageIndex++) {
            Page inputPage = input.get(pageIndex);
            Page outputPage = results.get(pageIndex);
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(includePositionMapping ? 2 : 1));
            BytesRefBlock handlesBlock = inputPage.getBlock(0);
            DocBlock docBlock = outputPage.getBlock(0);
            for (int position = 0; position < inputPage.getPositionCount(); position++) {
                RemoteFetchHandle handle = RemoteFetchHandle.fromBytesRef(
                    handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch)
                );
                assertThat(docBlock.asVector().shards().getInt(position), equalTo(handle.shard()));
                assertThat(docBlock.asVector().segments().getInt(position), equalTo(handle.segment()));
                assertThat(docBlock.asVector().docs().getInt(position), equalTo(handle.doc()));
            }
            if (includePositionMapping) {
                IntBlock positionBlock = outputPage.getBlock(1);
                for (int position = 0; position < inputPage.getPositionCount(); position++) {
                    assertThat(positionBlock.getInt(positionBlock.getFirstValueIndex(position)), equalTo(position));
                }
            }
        }
    }

    public void testDecodeHandlesBuildsDocBlock() {
        Page input = null;
        Page output = null;
        try (
            RemoteFetchHandleDecodeOperator operator = new RemoteFetchHandleDecodeOperator(TestBlockFactory.getNonBreakingInstance(), false)
        ) {
            input = handlesPage(
                List.of(new RemoteFetchHandle("node-1", "session-1", 3, 7, 11), new RemoteFetchHandle("node-1", "session-1", 5, 13, 17))
            );
            operator.addInput(input);
            input = null;

            output = operator.getOutput();
            assertThat(output.getBlockCount(), equalTo(1));
            assertThat(output.getPositionCount(), equalTo(2));

            DocBlock docBlock = output.getBlock(0);
            assertThat(docBlock.asVector().shards().getInt(0), equalTo(3));
            assertThat(docBlock.asVector().segments().getInt(0), equalTo(7));
            assertThat(docBlock.asVector().docs().getInt(0), equalTo(11));
            assertThat(docBlock.asVector().shards().getInt(1), equalTo(5));
            assertThat(docBlock.asVector().segments().getInt(1), equalTo(13));
            assertThat(docBlock.asVector().docs().getInt(1), equalTo(17));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
            if (output != null) {
                output.releaseBlocks();
            }
        }
    }

    public void testDecodeHandlesWithPositionMappingAddsTrailingPositionColumn() {
        Page input = null;
        Page output = null;
        try (
            RemoteFetchHandleDecodeOperator operator = new RemoteFetchHandleDecodeOperator(TestBlockFactory.getNonBreakingInstance(), true)
        ) {
            input = handlesPage(
                List.of(new RemoteFetchHandle("node-1", "session-1", 3, 7, 11), new RemoteFetchHandle("node-1", "session-1", 5, 13, 17))
            );
            operator.addInput(input);
            input = null;

            output = operator.getOutput();
            assertThat(output.getBlockCount(), equalTo(2));
            assertThat(output.getPositionCount(), equalTo(2));

            IntBlock positionBlock = output.getBlock(1);
            assertThat(positionBlock.getInt(positionBlock.getFirstValueIndex(0)), equalTo(0));
            assertThat(positionBlock.getInt(positionBlock.getFirstValueIndex(1)), equalTo(1));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
            if (output != null) {
                output.releaseBlocks();
            }
        }
    }

    public void testRejectsNullHandle() {
        Page input = null;
        try (
            RemoteFetchHandleDecodeOperator operator = new RemoteFetchHandleDecodeOperator(TestBlockFactory.getNonBreakingInstance(), false)
        ) {
            try (BytesRefBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(2)) {
                builder.appendBytesRef(new RemoteFetchHandle("node-1", "session-1", 3, 7, 11).toBytesRef());
                builder.appendNull();
                input = new Page(builder.build());
            }
            operator.addInput(input);
            input = null;

            IllegalStateException e = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(e.getMessage(), equalTo("remote fetch handle block cannot contain nulls"));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
        }
    }

    public void testRejectsMultiValueHandle() {
        Page input = null;
        try (
            RemoteFetchHandleDecodeOperator operator = new RemoteFetchHandleDecodeOperator(TestBlockFactory.getNonBreakingInstance(), false)
        ) {
            BytesRef handleBytes = new RemoteFetchHandle("node-1", "session-1", 3, 7, 11).toBytesRef();
            try (BytesRefBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(1)) {
                builder.beginPositionEntry();
                builder.appendBytesRef(handleBytes);
                builder.appendBytesRef(handleBytes);
                builder.endPositionEntry();
                input = new Page(builder.build());
            }
            operator.addInput(input);
            input = null;

            IllegalStateException e = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(e.getMessage(), equalTo("remote fetch handle block must have exactly one value per row"));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
        }
    }

    public void testRejectsMixedTargetSessions() {
        Page input = null;
        try (
            RemoteFetchHandleDecodeOperator operator = new RemoteFetchHandleDecodeOperator(TestBlockFactory.getNonBreakingInstance(), false)
        ) {
            input = handlesPage(
                List.of(new RemoteFetchHandle("node-1", "session-1", 3, 7, 11), new RemoteFetchHandle("node-2", "session-2", 5, 13, 17))
            );
            operator.addInput(input);
            input = null;

            IllegalStateException e = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(
                e.getMessage(),
                equalTo(
                    "remote fetch batch must contain handles from a single target session but saw [node-1/session-1] and [node-2/session-2]"
                )
            );
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
        }
    }

    private static Page handlesPage(List<RemoteFetchHandle> handles) {
        try (BytesRefBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(handles.size())) {
            for (RemoteFetchHandle handle : handles) {
                builder.appendBytesRef(handle.toBytesRef());
            }
            return new Page(builder.build());
        }
    }
}
