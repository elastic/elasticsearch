/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class LookupFromIndexServiceResponseTests extends AbstractWireSerializingTestCase<LookupFromIndexService.LookupResponse> {
    private final List<CircuitBreaker> breakers = new ArrayList<>();

    LookupFromIndexService.LookupResponse createTestInstance(BlockFactory blockFactory) {
        return new LookupFromIndexService.LookupResponse(randomList(0, 10, () -> randomPage(blockFactory)), blockFactory);
    }

    /**
     * Build a random {@link Page} to test serialization.
     */
    Page randomPage(BlockFactory blockFactory) {
        Block[] blocks = new Block[between(1, 20)];
        int positionCount = between(1, 100);
        try {
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = RandomBlock.randomBlock(
                    blockFactory,
                    RandomBlock.randomElementType(),
                    positionCount,
                    randomBoolean(),
                    1,
                    1,
                    0,
                    0
                ).block();
            }
        } finally {
            if (blocks[blocks.length - 1] == null) {
                Releasables.close(blocks);
            }
        }
        return new Page(blocks);
    }

    @Override
    protected LookupFromIndexService.LookupResponse createTestInstance() {
        // Can't use a real block factory for the basic serialization tests because they don't release.
        return createTestInstance(TestBlockFactory.getNonBreakingInstance());
    }

    @Override
    protected Writeable.Reader<LookupFromIndexService.LookupResponse> instanceReader() {
        return in -> new LookupFromIndexService.LookupResponse(in, TestBlockFactory.getNonBreakingInstance());
    }

    @Override
    protected LookupFromIndexService.LookupResponse mutateInstance(LookupFromIndexService.LookupResponse instance) throws IOException {
        assertThat(instance.blockFactory, sameInstance(TestBlockFactory.getNonBreakingInstance()));
        List<Page> pages = new ArrayList<>(instance.pages().size());
        pages.addAll(instance.pages());
        pages.add(randomPage(TestBlockFactory.getNonBreakingInstance()));
        return new LookupFromIndexService.LookupResponse(pages, instance.blockFactory);
    }

    public void testWithBreaker() throws IOException {
        BlockFactory origFactory = blockFactory();
        BlockFactory copyFactory = blockFactory();
        LookupFromIndexService.LookupResponse orig = createTestInstance(origFactory);
        try {
            LookupFromIndexService.LookupResponse copy = copyInstance(
                orig,
                getNamedWriteableRegistry(),
                (out, v) -> v.writeTo(out),
                in -> new LookupFromIndexService.LookupResponse(in, copyFactory),
                TransportVersion.current()
            );
            try {
                assertThat(copy, equalTo(orig));
            } finally {
                copy.decRef();
            }
            assertThat(copyFactory.breaker().getUsed(), equalTo(0L));
        } finally {
            orig.decRef();
        }
        assertThat(origFactory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Tests that we don't reserve any memory other than that in the {@link Page}s we
     * hold, and calling {@link LookupFromIndexService.LookupResponse#takePages}
     * gives us those pages. If we then close those pages, we should have 0
     * reserved memory.
     */
    public void testTakePages() {
        BlockFactory factory = blockFactory();
        LookupFromIndexService.LookupResponse orig = createTestInstance(factory);
        try {
            if (orig.pages().isEmpty()) {
                assertThat(factory.breaker().getUsed(), equalTo(0L));
                return;
            }
            List<Page> pages = orig.takePages();
            Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(pages.iterator(), page -> page::releaseBlocks)));
            assertThat(factory.breaker().getUsed(), equalTo(0L));
            assertThat(orig.takePages(), nullValue());
        } finally {
            orig.decRef();
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    private BlockFactory blockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(4 /* more than we need*/))
            .withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new BlockFactory(breaker, bigArrays);
    }

    @After
    public void allBreakersEmpty() throws Exception {
        // first check that all big arrays are released, which can affect breakers
        MockBigArrays.ensureAllArraysAreReleased();

        for (CircuitBreaker breaker : breakers) {
            assertThat("Unexpected used in breaker: " + breaker, breaker.getUsed(), equalTo(0L));
        }
    }
}
