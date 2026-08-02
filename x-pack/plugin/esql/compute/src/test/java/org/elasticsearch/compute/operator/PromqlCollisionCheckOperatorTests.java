/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class PromqlCollisionCheckOperatorTests extends OperatorTestCase {

    /**
     * The generic {@link OperatorTestCase} harness only exercises the pass-through path: a single long key channel fed
     * strictly increasing (hence globally distinct) values never collides, so the operator behaves as a no-op mapper.
     * The collision failure is covered by the dedicated tests below, which feed hand-built pages with repeated keys.
     */
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Long> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add((long) i);
        }
        return new SequenceLongBlockSourceOperator(blockFactory, data);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputRows = input.stream().mapToInt(Page::getPositionCount).sum();
        int outputRows = results.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(outputRows, equalTo(inputRows));

        List<Long> inputValues = new ArrayList<>(inputRows);
        for (Page page : input) {
            LongBlock block = page.getBlock(0);
            for (int i = 0; i < page.getPositionCount(); i++) {
                inputValues.add(block.getLong(i));
            }
        }
        List<Long> outputValues = new ArrayList<>(outputRows);
        for (Page page : results) {
            LongBlock block = page.getBlock(0);
            for (int i = 0; i < page.getPositionCount(); i++) {
                outputValues.add(block.getLong(i));
            }
        }
        assertThat(outputValues, equalTo(inputValues));
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new PromqlCollisionCheckOperator.Factory(List.of(0));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("PromqlCollisionCheckOperator[keyChannels=[0]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("PromqlCollisionCheckOperator[keyChannels=[0]]");
    }

    public void testFailsOnSameIdentitySameBucketWithinPage() {
        DriverContext ctx = driverContext();
        // Two rows with identical (identity, bucket): a second source series relabeled onto the first's identity.
        Page page = page(ctx.blockFactory(), List.of("a", "a"), 100L, 100L);
        try (PromqlCollisionCheckOperator op = new PromqlCollisionCheckOperator(ctx, new int[] { 0, 1 })) {
            op.addInput(page);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, op::getOutput);
            assertThat(e.getMessage(), equalTo(PromqlCollisionCheckOperator.COLLISION_ERROR));
        }
    }

    public void testFailsOnSameIdentitySameBucketAcrossPages() {
        DriverContext ctx = driverContext();
        Page first = page(ctx.blockFactory(), List.of("a"), 100L);
        Page second = page(ctx.blockFactory(), List.of("a"), 100L);
        try (PromqlCollisionCheckOperator op = new PromqlCollisionCheckOperator(ctx, new int[] { 0, 1 })) {
            op.addInput(first);
            // The first page passes through unchanged.
            Page out = op.getOutput();
            out.releaseBlocks();

            op.addInput(second);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, op::getOutput);
            assertThat(e.getMessage(), equalTo(PromqlCollisionCheckOperator.COLLISION_ERROR));
        }
    }

    public void testPassesThroughDistinctIdentitySameBucket() {
        DriverContext ctx = driverContext();
        Page page = page(ctx.blockFactory(), List.of("a", "b"), 100L, 100L);
        assertPassThrough(ctx, page, List.of("a", "b"), new long[] { 100L, 100L });
    }

    public void testPassesThroughSameIdentityDistinctBucket() {
        DriverContext ctx = driverContext();
        // Identities that coincide only at different buckets are not a collision.
        Page page = page(ctx.blockFactory(), List.of("a", "a"), 100L, 200L);
        assertPassThrough(ctx, page, List.of("a", "a"), new long[] { 100L, 200L });
    }

    private void assertPassThrough(DriverContext ctx, Page page, List<String> expectedIdentity, long[] expectedBucket) {
        try (PromqlCollisionCheckOperator op = new PromqlCollisionCheckOperator(ctx, new int[] { 0, 1 })) {
            op.addInput(page);
            Page out = op.getOutput();
            try {
                assertThat(out.getPositionCount(), equalTo(expectedIdentity.size()));
                BytesRefBlock identity = out.getBlock(0);
                LongBlock bucket = out.getBlock(1);
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < expectedIdentity.size(); i++) {
                    assertThat(identity.getBytesRef(i, scratch).utf8ToString(), equalTo(expectedIdentity.get(i)));
                    assertThat(bucket.getLong(i), equalTo(expectedBucket[i]));
                }
            } finally {
                out.releaseBlocks();
            }
        }
    }

    private static Page page(BlockFactory blockFactory, List<String> identity, long... bucket) {
        assert identity.size() == bucket.length;
        Block identityBlock;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(identity.size())) {
            for (String value : identity) {
                builder.appendBytesRef(new BytesRef(value));
            }
            identityBlock = builder.build();
        }
        Block bucketBlock;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(bucket.length)) {
            for (long value : bucket) {
                builder.appendLong(value);
            }
            bucketBlock = builder.build();
        }
        return new Page(identityBlock, bucketBlock);
    }
}
