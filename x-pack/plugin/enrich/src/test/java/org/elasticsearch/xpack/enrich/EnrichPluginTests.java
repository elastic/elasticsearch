/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class EnrichPluginTests extends ESTestCase {

    public void testConstructWithByteSize() {
        final var size = randomNonNegativeInt();
        Settings settings = Settings.builder().put(EnrichPlugin.CACHE_SIZE_SETTING_NAME, size + "b").build();
        EnrichPlugin plugin = new EnrichPlugin(settings);
        assertEquals(size, plugin.getMaxCacheSize());
    }

    public void testConstructWithFlatNumber() {
        final var size = randomNonNegativeInt();
        Settings settings = Settings.builder().put(EnrichPlugin.CACHE_SIZE_SETTING_NAME, size).build();
        EnrichPlugin plugin = new EnrichPlugin(settings);
        assertEquals(size, plugin.getMaxCacheSize());
    }

    public void testConstructWithByteSizeBwc() {
        final var size = randomNonNegativeInt();
        Settings settings = Settings.builder().put(EnrichPlugin.CACHE_SIZE_SETTING_BWC_NAME, size + "b").build();
        EnrichPlugin plugin = new EnrichPlugin(settings);
        assertEquals(size, plugin.getMaxCacheSize());
    }

    public void testConstructWithFlatNumberBwc() {
        final var size = randomNonNegativeInt();
        Settings settings = Settings.builder().put(EnrichPlugin.CACHE_SIZE_SETTING_BWC_NAME, size).build();
        EnrichPlugin plugin = new EnrichPlugin(settings);
        assertEquals(size, plugin.getMaxCacheSize());
    }

    public void testConstructWithBothSettings() {
        Settings settings = Settings.builder()
            .put(EnrichPlugin.CACHE_SIZE_SETTING_NAME, randomNonNegativeInt())
            .put(EnrichPlugin.CACHE_SIZE_SETTING_BWC_NAME, randomNonNegativeInt())
            .build();
        assertThrows(IllegalArgumentException.class, () -> new EnrichPlugin(settings));
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
