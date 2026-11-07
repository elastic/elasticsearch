/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IvfSegmentConfigTests extends ESTestCase {

    public void testFromCodecDefaultsUsesNaNOversampling() {
        var ci = CentroidIndexFormat.FLAT;
        var q = QuantEncoding.FOUR_BIT_SYMMETRIC;
        IvfSegmentConfig c = IvfSegmentConfig.fromCodecDefaults(ci, new IvfSegmentConfig.OsqConfig(q), true);
        assertThat(c.centroidIndexFormat(), is(ci));
        assertThat(c.osqEncoding(), is(q));
        assertTrue(c.usePrecondition());
        assertTrue(Float.isNaN(c.rescoreOversample()));
    }

    public void testEmptyFlushSource() throws Exception {
        IvfFlushConfigSource src = IvfFlushConfigSource.empty();
        assertEquals(Optional.empty(), src.load(null, null));
    }

    public void testMergeResolverReturnsCodecDefault() throws Exception {
        IvfMergeConfigResolver r = IvfMergeConfigResolver.useCodecDefault();
        IvfSegmentConfig def = IvfSegmentConfig.fromCodecDefaults(
            CentroidIndexFormat.FLAT,
            new IvfSegmentConfig.OsqConfig(QuantEncoding.SEVEN_BIT_SYMMETRIC),
            false
        );
        assertSame(def, r.resolve(null, null, def));
    }

    public void testEffectiveRescoreOversampleQueryOverrideWins() {
        assertThat(IvfSegmentConfig.effectiveRescoreOversample(2f, 5f, 3f), equalTo(5f));
    }

    public void testEffectiveRescoreOversampleUsesPersistedWhenFinite() {
        assertThat(IvfSegmentConfig.effectiveRescoreOversample(4f, null, 3f), equalTo(4f));
    }

    public void testEffectiveRescoreOversampleUsesMappingWhenPersistedNaN() {
        assertThat(IvfSegmentConfig.effectiveRescoreOversample(Float.NaN, null, 3f), equalTo(3f));
    }

    public void testWithEffectiveRescoreOversampleReplacesNaN() {
        IvfSegmentConfig raw = IvfSegmentConfig.of(
            CentroidIndexFormat.FLAT,
            new IvfSegmentConfig.OsqConfig(QuantEncoding.ONE_BIT_4BIT_QUERY),
            true,
            Float.NaN
        );
        IvfSegmentConfig effective = IvfSegmentConfig.withEffectiveRescoreOversample(raw, null, 2.5f);
        assertThat(effective.rescoreOversample(), equalTo(2.5f));
        assertThat(effective.usePrecondition(), is(true));
    }

    public void testLeafCollectorBudget() {
        assertThat(IvfSegmentConfig.leafCollectorBudget(10, 3f), equalTo(60));
    }

    public void testShardMergeBudget() {
        assertThat(IvfSegmentConfig.shardMergeBudget(10, 5f), equalTo(50));
    }

    /**
     * The IVF budget functions take the user's {@code k} and expand it themselves. These numbers are the
     * default {@code bbq_disk} shape (k=10, 1-bit quantization -> oversample 3) and exist so that handing
     * these functions an already-oversampled {@code k} - which would triple the collector and the merge cap -
     * shows up as a failure here as well as in the query wiring.
     */
    public void testBudgetsExpandFromTheFinalK() {
        assertEquals("per-leaf collector: 2 * k * oversample", 60, IvfSegmentConfig.leafCollectorBudget(10, 3f));
        assertEquals("shard merge cap: ceil(k * oversample)", 30, IvfSegmentConfig.shardMergeBudget(10, 3f));

        // Passing k*oversample instead of k is the regression this guards against.
        assertEquals(180, IvfSegmentConfig.leafCollectorBudget(30, 3f));
        assertEquals(90, IvfSegmentConfig.shardMergeBudget(30, 3f));
    }

    public void testAshForDimsDerivesBitsAndProjectedDims() {
        // dims -> {bits, projected dims}
        int[][] cases = {
            { 1, 1, 1 },
            { 64, 1, 64 },
            { 100, 1, 100 },
            { 384, 1, 384 },
            { 511, 1, 511 },
            { 512, 2, 256 },
            { 768, 2, 384 },
            { 1000, 2, 512 },
            { 1024, 2, 512 },
            { 1536, 2, 768 },
            { 2047, 2, 1024 },
            { 2048, 2, 1024 },
            { 2049, 4, 512 },
            { 3072, 4, 768 },
            { 4096, 4, 1024 },
            { 8192, 4, 1024 } };
        for (int[] c : cases) {
            int dims = c[0];
            IvfSegmentConfig.AshConfig config = IvfSegmentConfig.AshConfig.forDims(dims, null, 4);
            assertEquals("bits for dims [" + dims + "]", c[1], config.bitsPerDim());
            assertEquals("projected dims for dims [" + dims + "]", c[2], config.projectedDims(dims));
            assertEquals(4, config.queryBitsPerDim());
        }
    }

    public void testAshForDimsRespectsConfiguredBits() {
        for (int dims : new int[] { 384, 1024, 3072 }) {
            int bits = randomFrom(1, 2, 3, 4, 8);
            IvfSegmentConfig.AshConfig configured = IvfSegmentConfig.AshConfig.forDims(dims, bits, 4);
            assertEquals(bits, configured.bitsPerDim());
            // the projection depends on the dimension only, so changing the bits never changes its shape
            IvfSegmentConfig.AshConfig derived = IvfSegmentConfig.AshConfig.forDims(dims, null, 4);
            assertEquals(derived.projectedDims(dims), configured.projectedDims(dims));
        }
        expectThrows(IllegalArgumentException.class, () -> IvfSegmentConfig.AshConfig.forDims(1024, 7, 4));
    }

    public void testAshProjectedDimsAreAligned() {
        int dims = randomIntBetween(1, 4096);
        int projectedDims = IvfSegmentConfig.AshConfig.forDims(dims, null, 4).projectedDims(dims);
        assertTrue(projectedDims > 0);
        assertTrue(projectedDims <= dims);
        assertTrue(projectedDims <= IvfSegmentConfig.AshConfig.DEFAULT_MAX_PROJECTED_DIMS);
        if (dims >= IvfSegmentConfig.AshConfig.LOW_DIMS_THRESHOLD) {
            assertEquals(0, projectedDims % IvfSegmentConfig.AshConfig.PROJECTED_DIMS_ALIGNMENT);
        } else {
            // small vectors are not reduced at all
            assertEquals(dims, projectedDims);
        }
    }

    public void testAshConfigResolvers() {
        IvfSegmentConfig.AshConfig fixed = IvfSegmentConfig.AshConfig.defaults();
        AshConfigResolver fixedResolver = AshConfigResolver.fixed(fixed);
        assertSame(fixed, fixedResolver.resolve(randomIntBetween(1, 4096)));
        assertEquals(fixed.queryBitsPerDim(), fixedResolver.queryBitsPerDim());

        AshConfigResolver derived = AshConfigResolver.dimsDerived(null, 4);
        assertEquals(IvfSegmentConfig.AshConfig.forDims(3072, null, 4), derived.resolve(3072));
        assertEquals(4, derived.queryBitsPerDim());
        expectThrows(IllegalArgumentException.class, () -> AshConfigResolver.dimsDerived(5, 4));
    }

    /** An oversample below 1 (or absent) must never shrink the budget below the plain k shape. */
    public void testBudgetsFloorOversampleAtOne() {
        assertEquals(20, IvfSegmentConfig.leafCollectorBudget(10, 0.5f));
        assertEquals(10, IvfSegmentConfig.shardMergeBudget(10, 0.5f));
        assertEquals(20, IvfSegmentConfig.leafCollectorBudget(10, 1f));
        assertEquals(10, IvfSegmentConfig.shardMergeBudget(10, 1f));
    }
}
