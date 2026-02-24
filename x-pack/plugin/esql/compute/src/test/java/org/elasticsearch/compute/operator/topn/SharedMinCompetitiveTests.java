/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SharedMinCompetitiveTests extends ComputeTestCase {
    public void testEmpty() {
        try (SharedMinCompetitive minCompetitive = longMinCompetitive()) {
            assertThat(minCompetitive.get(blockFactory()), nullValue());
        }
    }

    public void testOneOffer() {
        long v = randomLong();
        try (SharedMinCompetitive minCompetitive = longMinCompetitive()) {
            offerLong(blockFactory(), minCompetitive, v);
            try (Page p = minCompetitive.get(blockFactory())) {
                assertThat(p.getPositionCount(), equalTo(1));
                assertThat(p.getBlockCount(), equalTo(1));
                LongVector vector = p.<LongBlock>getBlock(0).asVector();
                assertThat(vector.getLong(0), equalTo(v));
            }
        }
    }

    public void testManyOffers() {
        int count = 10_000;
        long[] values = randomLongs().limit(count).toArray();
        try (SharedMinCompetitive minCompetitive = longMinCompetitive()) {
            BlockFactory blockFactory = blockFactory();
            for (long v : values) {
                offerLong(blockFactory, minCompetitive, v);
            }
            try (Page p = minCompetitive.get(blockFactory())) {
                assertThat(p.getPositionCount(), equalTo(1));
                assertThat(p.getBlockCount(), equalTo(1));
                LongVector vector = p.<LongBlock>getBlock(0).asVector();
                assertThat(vector.getLong(0), equalTo(Arrays.stream(values).max().getAsLong()));
            }
        }
    }

    public void testManyConcurrentOffers() throws ExecutionException, InterruptedException, TimeoutException {
        int count = 10_000;
        int threads = 5;
        long max = Long.MIN_VALUE;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<Future<?>> wait = new ArrayList<>();
        try {
            try (SharedMinCompetitive minCompetitive = longMinCompetitive()) {
                for (int t = 0; t < threads; t++) {
                    BlockFactory blockFactory = blockFactory();
                    long[] values = randomLongs().limit(count).toArray();
                    max = Math.max(max, Arrays.stream(values).max().getAsLong());
                    wait.add(exec.submit(() -> {
                        for (long v : values) {
                            offerLong(blockFactory, minCompetitive, v);
                        }
                    }));
                }
                for (Future<?> w : wait) {
                    w.get(1, TimeUnit.MINUTES);
                }
                try (Page p = minCompetitive.get(blockFactory())) {
                    assertThat(p.getPositionCount(), equalTo(1));
                    assertThat(p.getBlockCount(), equalTo(1));
                    LongVector vector = p.<LongBlock>getBlock(0).asVector();
                    assertThat(vector.getLong(0), equalTo(max));
                }
            }
        } finally {
            exec.shutdown();
        }
    }

    private SharedMinCompetitive longMinCompetitive() {
        TopNOperator.SortOrder so = longSortOrder();
        SharedMinCompetitive.KeyConfig keyConfig = new SharedMinCompetitive.KeyConfig(
            ElementType.LONG,
            TopNEncoder.DEFAULT_UNSORTABLE,
            so.asc(),
            so.nullsFirst()
        );
        return new SharedMinCompetitive.Supplier(blockFactory().breaker(), List.of(keyConfig)).get();
    }

    private void offerLong(BlockFactory blockFactory, SharedMinCompetitive minCompetitive, long l) {
        try (
            Block block = blockFactory.newConstantLongBlockWith(l, 1);
            BreakingBytesRefBuilder b = new BreakingBytesRefBuilder(blockFactory.breaker(), "work");
        ) {
            KeyExtractor extractor = longExtractor(block);
            extractor.writeKey(b, 0);
            minCompetitive.offer(b.bytesRefView());
        }
    }

    private KeyExtractor longExtractor(Block block) {
        TopNOperator.SortOrder so = longSortOrder();
        return KeyExtractor.extractorFor(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE, so.asc(), so.nul(), so.nonNul(), block);
    }

    private TopNOperator.SortOrder longSortOrder() {
        return new TopNOperator.SortOrder(0, false, false);
    }
}
