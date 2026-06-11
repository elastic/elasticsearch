/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;

public class DiagnosticsCacheTests extends ESTestCase {

    private static final String INFERENCE_ID = "inference-id";
    private static final String CACHED_VALUE = "cached-value";

    public void testStats_ReturnsEmptyStatsWhenCacheDisabled() {
        var testCache = buildCache(false, 10);
        var key = new InferenceIdAndProject(INFERENCE_ID, ProjectId.DEFAULT);
        testCache.put(key, CACHED_VALUE);
        testCache.get(key);

        var stats = testCache.stats();
        assertThat(stats.getHits(), is(0L));
        assertThat(stats.getMisses(), is(0L));
        assertThat(stats.getEvictions(), is(0L));
    }

    public void testStats_ReturnsSameEmptyInstanceWhenDisabled() {
        var testCache = buildCache(false, 10);

        assertSame(DiagnosticsCache.EMPTY, testCache.stats());
    }

    public void testStats_PassThroughHitsAndMissesWhenEnabled() {
        var testCache = buildCache(true, 10);
        var key = new InferenceIdAndProject(INFERENCE_ID, ProjectId.DEFAULT);
        var missingKey = new InferenceIdAndProject("missing", ProjectId.DEFAULT);

        testCache.put(key, CACHED_VALUE);
        testCache.get(key);
        testCache.get(missingKey);

        var stats = testCache.stats();
        assertThat(stats.getHits(), is(1L));
        assertThat(stats.getMisses(), is(1L));
    }

    public void testStats_PassThroughEvictionsWhenEnabled() {
        var testCache = buildCache(true, 1);
        var key1 = new InferenceIdAndProject("id-1", ProjectId.DEFAULT);
        var key2 = new InferenceIdAndProject("id-2", ProjectId.DEFAULT);

        testCache.put(key1, CACHED_VALUE);
        testCache.put(key2, CACHED_VALUE);

        assertThat(testCache.stats().getEvictions(), is(1L));
    }

    public void testCacheCount_ReturnsZeroWhenDisabled() {
        var testCache = buildCache(false, 10);
        testCache.put(new InferenceIdAndProject(INFERENCE_ID, ProjectId.DEFAULT), CACHED_VALUE);

        assertThat(testCache.cacheCount(), is(0));
    }

    public void testCacheCount_ReturnsActualCountWhenEnabled() {
        var testCache = buildCache(true, 10);
        var count = randomIntBetween(1, 5);
        for (int i = 0; i < count; i++) {
            testCache.put(new InferenceIdAndProject("id-" + i, ProjectId.DEFAULT), CACHED_VALUE);
        }

        assertThat(testCache.cacheCount(), is(count));
    }

    public void testToggleEnabled_RestoresVisibilityOfStatsAndCount() {
        var testCache = buildCache(true, 10);
        var key = new InferenceIdAndProject(INFERENCE_ID, ProjectId.DEFAULT);
        testCache.put(key, CACHED_VALUE);
        testCache.get(key);

        testCache.setEnabled(false);
        assertThat(testCache.cacheCount(), is(0));
        assertThat(testCache.stats().getHits(), is(0L));

        testCache.setEnabled(true);
        assertThat(testCache.cacheCount(), is(1));
        assertThat(testCache.stats().getHits(), is(1L));
    }

    private static TestDiagnosticsCache buildCache(boolean enabled, long maxWeight) {
        var cache = CacheBuilder.<InferenceIdAndProject, String>builder().setMaximumWeight(maxWeight).build();
        return new TestDiagnosticsCache(cache, enabled);
    }

    private static class TestDiagnosticsCache extends DiagnosticsCache<String> {
        private final AtomicBoolean enabled;

        TestDiagnosticsCache(Cache<InferenceIdAndProject, String> cache, boolean initialEnabled) {
            super(cache);
            this.enabled = new AtomicBoolean(initialEnabled);
        }

        @Override
        public boolean cacheEnabled() {
            return enabled.get();
        }

        void setEnabled(boolean value) {
            enabled.set(value);
        }

        void put(InferenceIdAndProject key, String value) {
            cache.put(key, value);
        }

        String get(InferenceIdAndProject key) {
            return cache.get(key);
        }
    }
}
