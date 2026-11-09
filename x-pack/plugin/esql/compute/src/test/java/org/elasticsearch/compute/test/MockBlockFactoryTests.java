/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MockBlockFactoryTests extends ESTestCase {
    private final MockBlockFactory factory = new MockBlockFactory(
        BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking())
    );

    public void testReleasingBlocksStopsTrackingThem() {
        List<IntVector> vectors = allocate(between(1, 5_000));
        assertThat(factory.TRACKED_BLOCKS.size(), equalTo(vectors.size()));

        Releasables.close(vectors);

        assertThat(factory.TRACKED_BLOCKS, anEmptyMap());
    }

    public void testTrackingKeepsOtherAttachedReleasables() {
        IntVector vector = allocate(1).getFirst();
        AtomicBoolean released = new AtomicBoolean();
        vector.attachReleasable(() -> released.set(true));

        vector.close();

        assertTrue(released.get());
        assertThat(factory.TRACKED_BLOCKS, anEmptyMap());
    }

    /**
     * Closing a builder counts toward the next scan, and a scan waits for at most as many calls as there were live
     * builders, so closing every builder leaves at most {@link MockBlockFactory#MIN_PURGE_INTERVAL} of them tracked.
     */
    public void testReleasedBuildersArePurgedWithoutFinalCheck() {
        List<IntBlock.Builder> builders = new ArrayList<>();
        for (int i = between(1, 5_000); i > 0; i--) {
            builders.add(factory.newIntBlockBuilder(1));
        }
        assertThat(factory.TRACKED_BUILDERS.size(), equalTo(builders.size()));

        Releasables.close(builders);

        assertThat(factory.TRACKED_BUILDERS.size(), lessThanOrEqualTo(MockBlockFactory.MIN_PURGE_INTERVAL));
        factory.ensureAllBlocksAreReleased();
        assertThat(factory.TRACKED_BUILDERS, anEmptyMap());
    }

    public void testLeakReportedAmongManyReleasedBlocks() {
        List<IntVector> vectors = allocate(between(1, 5_000));
        IntVector leaked = vectors.remove(randomIntBetween(0, vectors.size() - 1));
        Releasables.close(vectors);

        RuntimeException e = expectThrows(RuntimeException.class, factory::ensureAllBlocksAreReleased);
        assertThat(e.getMessage(), equalTo("1 releasables have not been released"));

        leaked.close();
        factory.ensureAllBlocksAreReleased();
    }

    public void testTrackingReleasedObjectFails() {
        IntVector vector = allocate(1).getFirst();
        vector.close();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> factory.track(vector, "detail"));
        assertThat(e.getMessage(), equalTo("Tracking an already released object: " + vector));
    }

    private List<IntVector> allocate(int count) {
        List<IntVector> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            vectors.add(factory.newIntArrayVector(new int[] { i }, 1, 0L));
        }
        return vectors;
    }
}
