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
        IvfSegmentConfig c = IvfSegmentConfig.fromCodecDefaults(ci, q, true);
        assertThat(c.centroidIndexFormat(), is(ci));
        assertThat(c.quantEncoding(), is(q));
        assertTrue(c.usePrecondition());
        assertTrue(Float.isNaN(c.rescoreOversample()));
    }

    public void testEmptyFlushSource() throws Exception {
        IvfFlushConfigSource src = IvfFlushConfigSource.empty();
        assertEquals(Optional.empty(), src.load(null, null));
    }

    public void testMergeResolverReturnsCodecDefault() throws Exception {
        IvfMergeConfigResolver r = IvfMergeConfigResolver.useCodecDefault();
        IvfSegmentConfig def = IvfSegmentConfig.fromCodecDefaults(CentroidIndexFormat.FLAT, QuantEncoding.SEVEN_BIT_SYMMETRIC, false);
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
        IvfSegmentConfig raw = new IvfSegmentConfig(CentroidIndexFormat.FLAT, QuantEncoding.ONE_BIT_4BIT_QUERY, true, Float.NaN);
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
}
