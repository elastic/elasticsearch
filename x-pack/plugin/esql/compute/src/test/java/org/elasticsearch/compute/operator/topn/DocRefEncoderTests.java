/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The registry {@link DocRefEncoder} gives each TopN, and the checks that keep document references out of null blocks.
 */
public class DocRefEncoderTests extends ESTestCase {
    public void testInternsEachOriginOnce() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (DocRefEncoder.Registry registry = registry(breaker)) {
            DocRefOrigin a = RandomBlock.randomDocRefOrigin();
            DocRefOrigin b = randomValueOtherThan(a, RandomBlock::randomDocRefOrigin);
            int ordinalA = registry.intern(a);
            int ordinalB = registry.intern(b);
            assertThat(registry.intern(a), equalTo(ordinalA));
            assertThat(registry.origin(ordinalA), equalTo(a));
            assertThat(registry.origin(ordinalB), equalTo(b));
        }
    }

    public void testReturnsItsBytesWhenClosed() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        DocRefEncoder.Registry registry = registry(breaker);
        DocRefOrigin known = RandomBlock.randomDocRefOrigin();
        registry.intern(known);
        for (int i = 0; i < 10; i++) {
            registry.intern(RandomBlock.randomDocRefOrigin());
        }
        assertThat(registry.ramBytesUsed(), greaterThan(0L));
        assertThat(breaker.getUsed(), equalTo(registry.ramBytesUsed()));

        registry.close();
        assertThat(breaker.getUsed(), equalTo(0L));
        // a parallel worker may still run after its merge target closed, so new origins must not reserve bytes
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.intern(RandomBlock.randomDocRefOrigin()));
        assertThat(e.getMessage(), containsString("closed"));
        assertThat(registry.intern(known), equalTo(0));
        assertThat(breaker.getUsed(), equalTo(0L));
        registry.close();
    }

    public void testBreaks() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(100));
        try (DocRefEncoder.Registry registry = registry(breaker)) {
            expectThrows(Exception.class, () -> registry.intern(RandomBlock.randomDocRefOrigin()));
            assertThat(registry.ramBytesUsed(), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testConcurrentInterning() throws Exception {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        List<DocRefOrigin> origins = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            origins.add(RandomBlock.randomDocRefOrigin());
        }
        try (DocRefEncoder.Registry registry = registry(breaker)) {
            int threadCount = between(2, 8);
            CyclicBarrier start = new CyclicBarrier(threadCount);
            List<Thread> threads = new ArrayList<>();
            List<int[]> ordinals = new ArrayList<>();
            for (int t = 0; t < threadCount; t++) {
                int[] seen = new int[origins.size()];
                ordinals.add(seen);
                List<Integer> order = new ArrayList<>();
                for (int i = 0; i < origins.size(); i++) {
                    order.add(i);
                }
                Collections.shuffle(order, random());
                Thread thread = new Thread(() -> {
                    safeAwait(start);
                    for (int i : order) {
                        seen[i] = registry.intern(origins.get(i));
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            for (int i = 0; i < origins.size(); i++) {
                int ordinal = ordinals.get(0)[i];
                for (int[] seen : ordinals) {
                    assertThat("every thread sees one ordinal per origin", seen[i], equalTo(ordinal));
                }
                assertThat(registry.origin(ordinal), equalTo(origins.get(i)));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testOnlyTheOperatorEncoderEncodes() {
        expectThrows(IllegalStateException.class, () -> DocRefEncoder.PROTOTYPE.intern(RandomBlock.randomDocRefOrigin()));
        expectThrows(IllegalStateException.class, () -> DocRefEncoder.PROTOTYPE.origin(0));
        try (DocRefEncoder.Registry registry = registry(newLimitedBreaker(ByteSizeValue.ofMb(1)))) {
            expectThrows(IllegalStateException.class, () -> registry.forOperator(newLimitedBreaker(ByteSizeValue.ofMb(1))));
        }
    }

    public void testNullBlocksCantStandInForDocuments() {
        ElementType elementType = randomFrom(ElementType.DOC_REF, ElementType.DOC);
        try (Block nulls = TestBlockFactory.getNonBreakingInstance().newConstantNullBlock(1)) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> ValueExtractor.extractorFor(elementType, TopNEncoder.DEFAULT_UNSORTABLE, false, nulls)
            );
            assertThat(e.getMessage(), equalTo("[" + elementType + "] channels can't carry null blocks"));
            e = expectThrows(
                IllegalStateException.class,
                () -> KeyExtractor.extractorFor(elementType, TopNEncoder.DEFAULT_SORTABLE, true, (byte) 1, (byte) 2, nulls)
            );
            assertThat(e.getMessage(), equalTo("[" + elementType + "] channels can't carry null blocks"));
        }
    }

    private static DocRefEncoder.Registry registry(CircuitBreaker breaker) {
        return (DocRefEncoder.Registry) DocRefEncoder.PROTOTYPE.forOperator(breaker);
    }
}
