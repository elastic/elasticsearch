/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.equalTo;

public class EnrichPluginTests extends ESTestCase {

    public void testConstructWithByteSize() {
        Settings settings = Settings.builder().put(EnrichPlugin.CACHE_SIZE_SETTING_NAME, "2b").build();
        EnrichStatsAction.Response.CacheStats cacheStats = cacheEntries(settings, 2, 3L);
        assertThat(cacheStats, cacheStats(0L, 2L, 0L));
    }

    public void testConstructWithFlatNumber() {
        Settings settings = Settings.builder().put(EnrichPlugin.CACHE_SIZE_SETTING_NAME, "2").build();
        EnrichStatsAction.Response.CacheStats cacheStats = cacheEntries(settings, 2, 3L);
        assertThat(cacheStats, cacheStats(2L, 0L, 6L));
    }

    public void testConstructWithByteSizeBwc() {
        Settings settings = Settings.builder().put(EnrichPlugin.CACHE_SIZE_SETTING_BWC_NAME, "2b").build();
        EnrichStatsAction.Response.CacheStats cacheStats = cacheEntries(settings, 2, 3L);
        assertThat(cacheStats, cacheStats(0L, 2L, 0L));
    }

    public void testConstructWithFlatNumberBwc() {
        Settings settings = Settings.builder().put(EnrichPlugin.CACHE_SIZE_SETTING_BWC_NAME, "2").build();
        EnrichStatsAction.Response.CacheStats cacheStats = cacheEntries(settings, 2, 3L);
        assertThat(cacheStats, cacheStats(2L, 0L, 6L));
    }

    public void testConstructWithBothSettings() {
        Settings settings = Settings.builder()
            .put(EnrichPlugin.CACHE_SIZE_SETTING_NAME, randomNonNegativeInt())
            .put(EnrichPlugin.CACHE_SIZE_SETTING_BWC_NAME, randomNonNegativeInt())
            .build();
        assertThrows(IllegalArgumentException.class, () -> new EnrichPlugin(settings));
    }

    private static EnrichStatsAction.Response.CacheStats cacheEntries(Settings settings, int entries, long sizeInBytes) {
        EnrichCache cache = new EnrichPlugin(settings).createEnrichCache(new ThreadContext(Settings.EMPTY));
        ProjectId projectId = randomProjectIdOrDefault();
        EnrichCache.CacheValue value = new EnrichCache.CacheValue(List.of(Map.of("test", "entry")), sizeInBytes);
        for (int i = 0; i < entries; i++) {
            cache.put(new EnrichCache.CacheKey(projectId, "policy-1", String.valueOf(i), 1), value);
        }
        return cache.getStats("_id");
    }

    private static Matcher<EnrichStatsAction.Response.CacheStats> cacheStats(long count, long evictions, long cacheSizeInBytes) {
        return Matchers.allOf(
            transformedMatch("count", EnrichStatsAction.Response.CacheStats::count, equalTo(count)),
            transformedMatch("evictions", EnrichStatsAction.Response.CacheStats::evictions, equalTo(evictions)),
            transformedMatch("cacheSizeInBytes", EnrichStatsAction.Response.CacheStats::cacheSizeInBytes, equalTo(cacheSizeInBytes))
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        final var warnings = super.filteredWarnings();
        warnings.add(
            "[enrich.cache.size] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
        warnings.add(
            "The [enrich.cache.size] setting is deprecated and will be removed in a future version. Please use [enrich.cache_size] instead."
        );
        return warnings;
    }
}
